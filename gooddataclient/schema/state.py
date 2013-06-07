from gooddataclient.columns import Reference, ConnectionPoint
from gooddataclient.exceptions import DataSetNotFoundError
from gooddataclient.schema.utils import (
    retrieve_column_tuples, retrieve_dlc_info, get_references, attr_is_cp,
    get_user_cp_info, get_column_id
)
from gooddataclient.text import to_identifier, to_title


class State(object):

    SLI_URI = '/gdc/md/%s/ldm/singleloadinterface/dataset.%s/manifest'
    USING_URI = '/gdc/md/%s/using/%s'
    DATASETS_URI = '/gdc/md/%s/data/sets'

    def __init__(self, project):
        self.project = project
        self.connection = project.connection if project else None

    def get_datasets_metadata(self):
        """
        Retrieve the metadata for every dataset.
        """
        return self.connection.get(uri=self.DATASETS_URI % self.project.id)

    def get_metadata(self, name=None):
        """
        Retrieve the metadata for a given dataset.

        :param name:      the dataset name
        """
        try:
            datasets = self.get_datasets_metadata().json()['dataSetsInfo']['sets']
        except KeyError:
            datasets = []

        identifier = 'dataset.%s' % to_identifier(name) if name else self.identifier
        for dataset in datasets:
            if dataset['meta']['identifier'] == identifier:
                return dataset
        raise DataSetNotFoundError(
            'DataSet %(dataset)s not found', sets=datasets,
            project_name=name, dataset=name
        )

    def get_column_uris(self, schema_name=None):
        """
        A function to query GD API and retieve
        the list of attributes and facts of a dataset.
        """
        schema_name = schema_name or self.schema_name
        dataset_json = self.get_metadata(schema_name)
        response = self.connection.get(uri=dataset_json['meta']['uri'])
        content_json = response.json()['dataSet']['content']

        return content_json

    def get_column_detail(self, uri):
        """
        A function to retrieve the details of a column,
        given its uri.
        """
        column_json = self.connection.get(uri=uri).json()

        try:
            return column_json['dataLoadingColumn']
        except KeyError:
            try:
                return column_json['attribute']
            except KeyError:
                return column_json['fact']

    def get_dlc_info(self, column_uris, sli_manifest, schema_name=None):
        """
        A function to build the dlc_info dictionary.

        :param column_uris:     the column uris to query
        :param sli_manifest:    useful for to retrieve the DLC info
        :param schema_name:     the schema_name to look for (self by default)
        """
        identifier = to_identifier(schema_name) or self.identifier
        dlc_info = []
        # dataLoadingColumns
        for column_uri in column_uris:
            column_json = self.get_column_detail(column_uri)
            info = retrieve_dlc_info(self.identifier, column_json, sli_manifest)
            if info:
                dlc_info.append(info)

        unique_dlc_info = {}
        for key, value in dlc_info:
            if key not in unique_dlc_info or (not value.get('is_ref', False)):
                unique_dlc_info[key] = value

        return unique_dlc_info

    def get_column_pk_identifier(self, column_json):
        """
        A function to get the column pk identifier (GD naming...),
        required to check that an attribute is a connection point
        or not.
        """
        try:
            pk_uri = column_json['content']['pk'][0]['data']
        except KeyError:
            return None

        pk_json = self.connection.get(uri=pk_uri).json()

        return pk_json['column']['meta']['identifier']

    def get_connection_point_json(self, schema_name=None):
        """
        A function to retrieve API information on a dataset
        connection point.

        :param schema_name:       if not None, it is the dataset
                                  to query, else default to instance
        """
        schema_name = schema_name or self.identifier
        attr_uris = self.get_column_uris(schema_name)['attributes']

        for uri in attr_uris:
            col_json = self.get_column_detail(uri)
            pk_identifier = self.get_column_pk_identifier(col_json)
            if attr_is_cp(pk_identifier, schema_name):
                return col_json
        return {}

    def get_references(self, sli_manifest, dlc_info):
        """
        A function to retrieve the references of a dataset. It first reads
        the SLI manifest, looking for foreign columns, and then fetches the
        connection point of the dataset.

        :param sli_manifest:         the SLI manifest of the dataset.
        """
        references = get_references(self.identifier, sli_manifest)

        ref_tuples = []
        # dataset -> connection point
        for dataset, cp in references.iteritems():
            cp_json = self.get_connection_point_json(dataset)
            cp_id = get_column_id(cp_json)
            # retrieve using on this dataset
            users_cp = self.project.get_using(cp_id)
            # look for the self's identifier
            for user_cp in users_cp:
                dataset_user, field = get_user_cp_info(user_cp)
                if dataset_user == self.identifier:
                    column_uris = self.get_column_uris(dataset)['dataLoadingColumns']
                    data_type = self.get_dlc_info(column_uris, sli_manifest, dataset).get(field, {}).get('dataType', None)
                    ref_tuples.append((field, Reference(title=None, schemaReference=dataset, reference=cp, dataType=data_type)))
                    break

        return ref_tuples

    def get_remote_columns(self):
        """
        A function to retrieve the columns that are on
        gooddata, in case they changed.

        Returns a dictionary of `column_name`: Column
        """
        column_uris = self.get_column_uris()
        sli_manifest = self.get_remote_sli_manifest()
        categories = ['attributes', 'facts']

        dlc_info = self.get_dlc_info(column_uris['dataLoadingColumns'], sli_manifest)
        remote_columns = self.get_references(sli_manifest, dlc_info)

        # attributes, facts, dates, labels, hyperlinks
        for category in categories:
            for column_uri in column_uris.get(category, []):
                column_json = self.get_column_detail(column_uri)
                pk_identifier = self.get_column_pk_identifier(column_json)
                remote_columns.extend(
                    retrieve_column_tuples(column_json, category, pk_identifier, dlc_info)
                )

        return dict(remote_columns)

    def get_remote_diff(self):
        """
        A method to retrieve the remote state of the dataset,
        and compare it the current class state. It is based on the
        DiffState object.
        """
        remote_columns = self.get_remote_columns()
        return DiffState(remote_columns, dict(self._columns)).get_diff_state()

    def is_synchronised(self):
        """
        A method to check that the remote diff is empty.
        """
        remote_diff = self.get_remote_diff()
        return not (remote_diff['added'] or remote_diff['altered'] or remote_diff['deleted'])

    def has_column(self, col_name, attribute=False, fact=False, date=False, reference=False, title=None):
        """
        A function to check that a dataset has a specific column
        (attribute or fact), saved on GoodData.

        :param col_name:           the name of the column.
        :param attribute:          a boolean that says if the column
                                   to look for is an attribute.
        :param fact:               a boolean that says if the column
                                   to look for is an fact.
        :param date:               a boolean that says if the column
                                   to look for is a date.
        :param reference:          a boolean that says if the column
                                   to look for is a reference.
        :param title:              if needed, the title to look for
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
        elif attribute:
            prefix = 'attr.'
        elif fact:
            prefix = 'fact.'

        col_identifier = '%(prefix)s%(dataset)s.%(col_name)s%(suffix)s' % {
            'prefix': prefix,
            'dataset': to_identifier(self.schema_name),
            'col_name': col_name,
            'suffix': suffix,
        }

        for col_uri in col_uris:
            col_json = self.get_column_detail(col_uri)
            if col_json['meta']['identifier'] == col_identifier:
                if (not title or (title and col_json['meta']['title'] == title)):
                    return True

        return False

    def has_label(self, label_name, title=None, hyperlink=False):
        """
        Labels are particular because they are bound to columns.

        :param label_name:       the name of the label to get
        :param title:            if needed, the title of the label
        :param hyperlink:        a boolean telling if we're looking
                                 for an hyperlink
        """
        col_uris= self.get_column_uris()
        label_identifier_re = 'label\.%(dataset)s\.[a-zA-Z_]+\.%(label_name)s' % {
            'dataset': to_identifier(self.schema_name),
            'label_name': label_name,
        }

        for col_uri in col_uris['attributes'] + col_uris['facts']:
            col_json = self.get_column_detail(col_uri)
            for display in col_json['content'].get('displayForms', []):
                if re.match(label_identifier_re, display['meta']['identifier']):
                    if (not title or (title and display['meta']['title'] == title)):
                        if hyperlink and display['content'].get('type', '') != "GDC.link":
                            return False
                        return True

        return False

    def has_attribute(self, attr_name, **kwargs):
        return self.has_column(attr_name, attribute=True, **kwargs)

    def has_fact(self, fact_name, **kwargs):
        return self.has_column(fact_name, fact=True, **kwargs)

    def has_date(self, date_name, **kwargs):
        return self.has_column(date_name, date=True, **kwargs)

    def has_hyperlink(self, *args, **kwargs):
        return self.has_label(hyperlink=True, *args, **kwargs)

    def has_reference(self, reference_name):
        return self.has_column(reference_name, reference=True)


class DiffState(object):

    def __init__(self, old_state, new_state):
        self.old_state, self.new_state = old_state, new_state
        self.old_keys = set(old_state)
        self.new_keys = set(new_state)
        self.intersect = self.new_keys.intersection(self.old_keys)
        self.added_columns = self.get_added_columns()
        self.deleted_columns = self.get_deleted_columns()
        self.altered_columns = self.get_altered_columns()
        self.handle_factsof_if_needed()

    def get_diff_state(self):
        """
        A method to get the differences between two states of a dataset.

        :return:                a dict with three keys referencing columns:
                                - added
                                - altered
                                - deleted
        """
        return {
            'added': self.added_columns,
            'altered': self.altered_columns,
            'deleted': self.deleted_columns
        }

    def get_added_columns(self):
        added_keys = self.new_keys - self.intersect
        return dict(((key, self.new_state[key]) for key in added_keys))

    def get_deleted_columns(self):
        deleted_keys = self.old_keys - self.intersect
        return dict(((key, self.old_state[key]) for key in deleted_keys))

    def get_altered_columns(self):
        altered_columns = {}
        for key in self.intersect:
            if self.new_state[key] != self.old_state[key]:
                altered_columns[key] = {
                    'old': self.old_state[key],
                    'new': self.new_state[key],
                }

        return altered_columns

    def handle_factsof_if_needed(self):
        if 'factsof' in self.deleted_columns:
            cp_found = False
            for value in self.added_columns.itervalues():
                if isinstance(value, ConnectionPoint):
                    cp_found = True
                    break
            if not cp_found:
                del self.deleted_columns['factsof']
