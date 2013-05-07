import os
import logging
import inspect
import re

from requests.exceptions import HTTPError

from gooddataclient.exceptions import DataSetNotFoundError, MaqlValidationFailed
from gooddataclient import text
from gooddataclient.columns import (
    Column, Date, Attribute, ConnectionPoint, Label, Reference, Fact
)
from gooddataclient.text import to_identifier, to_title
from gooddataclient.archiver import CSV_DATA_FILENAME
from gooddataclient.schema.maql import (
    SYNCHRONIZE, SYNCHRONIZE_PRESERVE, DELETE_ROW, CP_DEFAULT_NAME, CP_DEFAULT_CREATE
)


logger = logging.getLogger("gooddataclient")


class Dataset(object):

    DATASETS_URI = '/gdc/md/%s/data/sets'

    def __init__(self, project):
        self.project = project
        self.connection = project.connection

        # column initializations
        self._columns = []
        self._connection_point = CP_DEFAULT_NAME
        self._has_cp = False
        for name, column in self.get_class_members():
            column.set_name_and_schema(to_identifier(name), to_identifier(self.schema_name))
            # need to mark the labels referencing
            # connection points, they are different
            if isinstance(column, Label) and \
               isinstance(getattr(self, column.reference), ConnectionPoint):
                column.references_cp = True
            # need to know which column is connection point
            if isinstance(column, ConnectionPoint):
                self._connection_point = name
                self._has_cp = True
            self._columns.append((name, column))

    class Meta:
        column_order = None
        schema_name = None
        project_name = None

    @classmethod
    def get_synchronize_statement(cls, schema_name, preserve=False):
        return (SYNCHRONIZE_PRESERVE if preserve else SYNCHRONIZE) % {
                'schema_name': to_identifier(schema_name)
            }

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

    def get_datasets_metadata(self):
        return self.connection.get(uri=self.DATASETS_URI % self.project.id)

    def get_metadata(self, name):
        try:
            datasets = self.get_datasets_metadata().json()['dataSetsInfo']['sets']
        except KeyError:
            datasets = []

        identifier = 'dataset.%s' % to_identifier(name)
        for dataset in datasets:
            if dataset['meta']['identifier'] == identifier:
                return dataset
        raise DataSetNotFoundError(
            'DataSet %(dataset)s not found', sets=datasets,
            project_name=name, dataset=name
        )

    def get_column_uris(self):
        """
        A function to query GD API and retieve
        the list of attributes and facts of the dataset.
        """
        dataset_json = self.get_metadata(self.schema_name)
        response = self.connection.get(uri=dataset_json['meta']['uri'])
        content_json = response.json()['dataSet']['content']

        return content_json

    def get_column_detail(self, uri, dlc=False):
        """
        A function to retrieve the details of a column,
        given its uri.
        """
        column_json = self.connection.get(uri=uri).json()

        if dlc:
            return column_json['dataLoadingColumn']

        try:
            return column_json['attribute']
        except KeyError:
            return column_json['fact']

    def has_column(self, col_name, attribute=False, date=False, reference=False):
        """
        A function to check that a dataset has a specific column
        (attribute or fact), saved on GoodData.

        :param col_name:           the name of the column.
        :param attribute:          a boolean that says if the column
                                   to look for is an attribute.
        :param date:               a boolean that says if the column
                                   to look for is a date.
        :param reference:          a boolean that says if the column
                                   to look for is a reference.
        """
        col_uris = self.get_column_uris()
        if attribute:
            col_uris = col_uris['attributes']
        elif reference:
            col_uris = col_uris['dataLoadingColumns']
        else:
            col_uris = col_uris['facts']

        suffix = ''
        if date:
            prefix = 'dt.'
        elif reference:
            prefix = 'f_'
            suffix = '_id'
        else:
            prefix = 'attr.' if attribute else 'fact.'

        col_identifier = '%(prefix)s%(dataset)s.%(col_name)s%(suffix)s' % {
            'prefix': prefix,
            'dataset': to_identifier(self.schema_name),
            'col_name': col_name,
            'suffix': suffix,
        }

        for col_uri in col_uris:
            col_json = self.get_column_detail(col_uri, reference)
            if col_json['meta']['identifier'] == col_identifier:
                return True
        return False

    def has_attribute(self, attr_name):
        return self.has_column(attr_name, attribute=True)

    def has_fact(self, fact_name):
        return self.has_column(fact_name, attribute=False)

    def has_date(self, date_name):
        return self.has_column(date_name, date=True)

    def has_label(self, label_name):
        col_uris= self.get_column_uris()
        label_identifier_re = 'label\.%(dataset)s\.[a-zA-Z_]+\.%(label_name)s' % {
            'dataset': to_identifier(self.schema_name),
            'label_name': label_name,
        }

        for col_uri in col_uris['attributes'] + col_uris['facts']:
            col_json = self.get_column_detail(col_uri)
            for display in col_json['content'].get('displayForms', []):
                if re.match(label_identifier_re, display['meta']['identifier']):
                    return True

        return False

    def has_reference(self, reference_name):
        return self.has_column(reference_name, reference=True)

    def delete(self, name):
        dataset = self.get_metadata(name)
        return self.connection.delete(uri=dataset['meta']['uri'])

    def data(self, *args, **kwargs):
        raise NotImplementedError

    def get_date_dimension(self):
        #support several date dimensions
        for _, column in self._columns:
            if isinstance(column, Date):
                yield column

    def get_datetime_column_names(self):
        """
        Get the list of date and datetime columns, and return both lists.
        """
        dates = []
        datetimes = []
        for name, column in self._columns:
            if isinstance(column, Date):
                if column.datetime:
                    datetimes.append(name)
                else:
                    dates.append(name)

        return dates, datetimes

    def create(self):
        for date_dimension in self.get_date_dimension():
            DateDimension(self.project).create(name=date_dimension.schemaReference,
                                               include_time=date_dimension.datetime)
        self.project.execute_maql(self.get_maql())

    def upload(self, keep_csv=False, csv_file=None,
               no_upload=False,  full_upload=False, *args, **kwargs):
        """
        A function to upload dataset data. It tries to
        call the data() method of the dataset to retrive
        data. If `no_upload` is set to True, no dataset is
        created nor uploaded on the project.

        If `keep_csv` is set to True, a csv dump is kept, in
        the file given by `csv_file`.
        """
        if not no_upload:
            try:
                self.get_metadata(self.schema_name)
            except DataSetNotFoundError:
                self.create()

        dates, datetimes = self.get_datetime_column_names()
        dir_name = self.connection.webdav.upload(
            self.data(*args, **kwargs), self.get_sli_manifest(full_upload),
            dates, datetimes, keep_csv, csv_file, no_upload
        )

        if not no_upload:
            self.project.integrate_uploaded_data(dir_name)
            self.connection.webdav.delete(dir_name)

    def get_folders(self):
        attribute_folders, fact_folders = [], []
        for _, column in self._columns:
            if column.folder:
                if isinstance(column, (Attribute, Label, ConnectionPoint, Reference)):
                    if (column.folder, column.folder_title) not in attribute_folders:
                        attribute_folders.append((column.folder, column.folder_title))
                if isinstance(column, (Fact, Date)):
                    if (column.folder, column.folder_title) not in fact_folders:
                        fact_folders.append((column.folder, column.folder_title))
        return attribute_folders, fact_folders

    def get_sli_manifest(self, full_upload=False):
        '''Create JSON manifest from columns in schema.
        
        See populateColumnsFromSchema in AbstractConnector.java
        '''
        parts = []
        for _, column in self._columns:
            parts.extend(column.get_sli_manifest_part(full_upload))

        return {"dataSetSLIManifest": {"parts": parts,
                                       "file": CSV_DATA_FILENAME,
                                       "dataSet": 'dataset.%s' % to_identifier(self.schema_name),
                                       "csvParams": {"quoteChar": '"',
                                                     "escapeChar": '"',
                                                     "separatorChar": ",",
                                                     "endOfLine": "\n"
                                                     }}}

    # FIXME: this is a bit repetitive, we can think of a
    #        better approach.
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

        # Append the attributes, ConnectionPoint, and Date that doesn't have schemaReference
        maql.append('# CREATE ATTRIBUTES.')
        for _, column in self._columns:
            if isinstance(column, (Attribute, ConnectionPoint))\
                or (isinstance(column, Date) and not column.schemaReference):
                maql.append(column.get_maql())

        # Append the facts and date facts
        maql.append('# CREATE FACTS AND DATE FACTS')
        for _, column in self._columns:
            if isinstance(column, Fact):
                maql.append(column.get_maql())

        # Append the references
        maql.append('# CREATE REFERENCES')
        for _, column in self._columns:
            if isinstance(column, Reference):
                maql.append(column.get_maql())

        # Append the labels and set a default one
        default_set = False
        for _, column in self._columns:
            if isinstance(column, Label):
                maql.append(column.get_maql())
                if not default_set:
                    maql.append(column.get_maql_default())
                    default_set = True

        if self._has_cp:
            maql.append('# ADD LABEL TO CONNECTION POINT')
            maql.append(getattr(self, self._connection_point).get_original_label_maql())
        else:
            maql.append(CP_DEFAULT_CREATE % {
                    'schema_name': to_identifier(self.schema_name),
                    'name': self._connection_point
            })

        maql.append(SYNCHRONIZE % {
            'schema_name': to_identifier(self.schema_name)
        })

        return '\n'.join(maql)

    def get_maql_delete(self, where_clause):
        """
        A function to retrieve the maql to delete rows
        from a given dataset.
        """
        return DELETE_ROW % {
            'where_clause': where_clause,
            'connection_point': self._connection_point,
            'schema_name': to_identifier(self.schema_name),
        }


class DateDimension(object):

    DATE_MAQL = 'INCLUDE TEMPLATE "URN:GOODDATA:DATE"'
    DATE_MAQL_ID = 'INCLUDE TEMPLATE "URN:GOODDATA:DATE" MODIFY (IDENTIFIER "%s", TITLE "%s");\n\n'
    DATASETS_URI = '/gdc/md/%s/data/sets'

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
        """
        A function to create the date dimension. Before executing the MAQL,
        it checks if the date dimension already exists.
        """
        if not name or (name and not self.date_exists(name)):
            self.project.execute_maql(self.get_maql(name, include_time))
            if include_time:
                self.upload_time(name)
        return self

    def date_exists(self, name):
        """
        A function to check the existence of a date dimension.
        It is only call if we want to create the date dimension
        using a name.
        """
        try:
            response = self.connection.get(self.DATASETS_URI % self.project.id)
            response.raise_for_status()
        except HTTPError, err:
            err_msg = 'Could not check if date exists: %(status_code)s'
            raise MaqlValidationFailed(
                err_msg, response=err.response,
                status_code=err.response.status_code
            )
        else:
            try:
                sets = response.json()['dataSetsInfo']['sets']
            except KeyError:
                sets = []
            return bool(filter(lambda x: x['meta']['identifier'] == '%s.dataset.dt' % name.lower(), sets))

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
