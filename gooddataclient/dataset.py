import os
import logging
import inspect

from gooddataclient.exceptions import DataSetNotFoundError
from gooddataclient import text
from gooddataclient.columns import Column, Date, Attribute, ConnectionPoint, \
                                   Label, Reference, Fact
from gooddataclient.text import to_identifier, to_title
from gooddataclient.archiver import CSV_DATA_FILENAME

logger = logging.getLogger("gooddataclient")


class Dataset(object):

    DATASETS_URI = '/gdc/md/%s/data/sets'

    def __init__(self, project):
        self.project = project
        self.connection = project.connection

    class Meta:
        column_order = None
        schema_name = None
        project_name = None

    @property
    def schema_name(self):
        return self.Meta.schema_name or self.__class__.__name__

    @property
    def project_name(self):
        return self.Meta.project_name

    def get_class_members(self):
        members = inspect.getmembers(self, lambda member: isinstance(member, Column))
        if not self.Meta.column_order:
            return members
        members_ordered = []
        for col_name in self.Meta.column_order:
            for name, column in members:
                if name == col_name:
                    members_ordered.append((name, column))
        return members_ordered

    def get_columns(self):
        columns = []
        for name, column in self.get_class_members():
            column.name = to_identifier(name)
            column.schema_name = to_identifier(self.schema_name)
            columns.append(column)
        return columns

    def get_datasets_metadata(self):
        return self.connection.get(uri=self.DATASETS_URI % self.project.id)

    def get_metadata(self, name):
        datasets = self.get_datasets_metadata().json()['dataSetsInfo']['sets']

        for dataset in datasets:
            if dataset['meta']['title'] == name:
                return dataset
        err_json = {
            'sets': datasets,
            'project_name': name
        }
        raise DataSetNotFoundError('DataSet %s not found' % name, err_json)

    def delete(self, name):
        dataset = self.get_metadata(name)
        return self.connection.delete(uri=dataset['meta']['uri'])

    def data(self):
        raise NotImplementedError

    def get_date_dimension(self):
        for column in self.get_columns():
            if isinstance(column, Date):
                return column

    def create(self):
        date_dimension = self.get_date_dimension()
        if date_dimension:
            DateDimension(self.project).create(name=date_dimension.schemaReference,
                                               include_time=date_dimension.datetime)
        self.project.execute_maql(self.get_maql())

    def upload(self):
        try:
            self.get_metadata(self.schema_name)
        except DataSetNotFoundError:
            self.create()
        dir_name = self.connection.webdav.upload(self.data(), self.get_sli_manifest())
        self.project.integrate_uploaded_data(dir_name)
        self.connection.webdav.delete(dir_name)

    def get_folders(self):
        attribute_folders, fact_folders = [], []
        for column in self.get_columns():
            if column.folder:
                if isinstance(column, (Attribute, Label, ConnectionPoint, Reference)):
                    if (column.folder, column.folder_title) not in attribute_folders:
                        attribute_folders.append((column.folder, column.folder_title))
                if isinstance(column, (Fact, Date)):
                    if (column.folder, column.folder_title) not in fact_folders:
                        fact_folders.append((column.folder, column.folder_title))
        return attribute_folders, fact_folders

    def get_sli_manifest(self):
        """
        Get the SLI manifest from API entry point.
        """
        sli_manifest = self.project.get_sli_manifest(self.name)
        sli_manifest['dataSetSLIManifest']["csvParams"] = {
            "quoteChar": '"',
            "escapeChar": '"',
            "separatorChar": ",",
            "endOfLine": "\n"
        }
        return sli_manifest

    def get_maql(self):
        maql = []

        maql.append("""
# CREATE DATASET. DATASET GROUPS ALL FOLLOWING LOGICAL MODEL ELEMENTS TOGETHER.
CREATE DATASET {dataset.%s} VISUAL(TITLE "%s");
""" % (to_identifier(self.schema_name), to_title(self.schema_name)))

        # create the folders if needed
        attribute_folders, fact_folders = self.get_folders()
        if attribute_folders or fact_folders:
            maql.append('# CREATE THE FOLDERS THAT GROUP ATTRIBUTES AND FACTS')
            for folder, folder_title in attribute_folders:
                maql.append('CREATE FOLDER {folder.%s.attr} VISUAL(TITLE "%s") TYPE ATTRIBUTE;'
                            % (folder, folder_title))
            maql.append('')
            for folder, folder_title in fact_folders:
                maql.append('CREATE FOLDER {folder.%s.fact} VISUAL(TITLE "%s") TYPE FACT;'
                            % (folder, folder_title))
            maql.append('')

        column_list = self.get_columns()

        # Append the attributes, ConnectionPoint, and Date that doesn't have schemaReference
        maql.append('# CREATE ATTRIBUTES.')
        for column in column_list:
            if isinstance(column, (Attribute, ConnectionPoint))\
                or (isinstance(column, Date) and not column.schemaReference):
                maql.append(column.get_maql())

        # Append the facts and date facts
        maql.append('# CREATE FACTS AND DATE FACTS')
        for column in column_list:
            if isinstance(column, Fact):
                maql.append(column.get_maql())

        # Append the references
        maql.append('# CREATE REFERENCES')
        for column in column_list:
            if isinstance(column, Reference):
                maql.append(column.get_maql())

        # Append the labels and set a default one
        default_set = False
        for column in column_list:
            if isinstance(column, Label):
                maql.append(column.get_maql())
                if not default_set:
                    maql.append(column.get_maql_default())
                    default_set = True

        cp = False
        for column in column_list:
            if isinstance(column, ConnectionPoint):
                cp = True
                maql.append('# ADD LABEL TO CONNECTION POINT')
                maql.append(column.get_original_label_maql())

        # TODO: not sure where this came from in Department example, wild guess only!
        if not cp:
            maql.append('CREATE ATTRIBUTE {attr.%s.factsof} VISUAL(TITLE "Records of %s") AS KEYS {f_%s.id} FULLSET;'
                        % (to_identifier(self.schema_name), to_title(self.schema_name), to_identifier(self.schema_name)))
            maql.append('ALTER DATASET {dataset.%s} ADD {attr.%s.factsof};'
                        % (to_identifier(self.schema_name), to_identifier(self.schema_name)))
        maql.append("""# SYNCHRONIZE THE STORAGE AND DATA LOADING INTERFACES WITH THE NEW LOGICAL MODEL
SYNCHRONIZE {dataset.%s};
""" % to_identifier(self.schema_name))

        return '\n'.join(maql)


class DateDimension(object):

    DATE_MAQL = 'INCLUDE TEMPLATE "URN:GOODDATA:DATE"'
    DATE_MAQL_ID = 'INCLUDE TEMPLATE "URN:GOODDATA:DATE" MODIFY (IDENTIFIER "%s", TITLE "%s");\n\n'

    def __init__(self, project):
        self.project = project
        self.connection = project.connection

    def get_maql(self, name=None, include_time=False):
        '''Get MAQL for date dimension.
        
        See generateMaqlCreate in DateDimensionConnect.java
        '''
        if not name:
            return self.DATE_MAQL

        maql = self.DATE_MAQL_ID % (text.to_identifier(name), name)

        if include_time:
            file_path = os.path.join(os.path.dirname(__file__), 'resources',
                                     'connector', 'TimeDimension.maql')
            time_dimension = open(file_path).read()\
                                .replace('%id%', text.to_identifier(name))\
                                .replace('%name%', name)
            maql = ''.join((maql, time_dimension))

        return maql

    def create(self, name=None, include_time=False):
        # TODO: check if not already created, if yes, do nothing
        self.project.execute_maql(self.get_maql(name, include_time))
        if include_time:
            self.upload_time(name)
        return self

    def upload_time(self, name):
        data = open(os.path.join(os.path.dirname(__file__), 'resources',
                                  'connector', 'data.csv')).read()
        sli_manifest = open(os.path.join(os.path.dirname(__file__), 'resources',
                                         'connector', 'upload_info.json')).read()\
                         .replace('%id%', text.to_identifier(name))\
                         .replace('%name%', name)
        dir_name = self.connection.webdav.upload(data, sli_manifest)
        self.project.integrate_uploaded_data(dir_name, wait_for_finish=True)
        self.connection.webdav.delete(dir_name)

