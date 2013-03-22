from gooddataclient.text import to_identifier, to_title


class Column(object):

    ldmType = None
    IDENTIFIER = ''

    def __init__(self, title, folder=None, reference=None,
                 schemaReference=None, dataType=None, datetime=False, format=None):
        self.title = to_title(title)
        self.folder = to_identifier(folder)
        self.folder_title = to_title(folder)
        self.reference = to_identifier(reference)
        self.schemaReference = to_identifier(schemaReference)
        self.dataType = dataType
        self.datetime = datetime
        self.format = format

    def get_schema_values(self):
        values = []
        for key in ('name', 'title', 'folder', 'reference', 'schemaReference',
                    'dataType', 'datetime', 'format'):
            value = getattr(self, key)
            if value:
                if isinstance(value, bool):
                    value = 'true'
                values.append((key, value))
        return values

    @property
    def identifier(self):
        return self.IDENTIFIER % {
            'dataset': self.schema_name,
            'name': self.name,
        }


class Attribute(Column):

    ldmType = 'ATTRIBUTE'
    IDENTIFIER = 'd_%(dataset)s_%(name)s.nm_%(name)s'

    def get_maql(self):
        maql = []
        # create the attribute
        maql.append('CREATE ATTRIBUTE {attr.%(dataset)s.%(name)s} VISUAL(TITLE "%(title)s"%(folder)s) AS {%(identifier)s};'
                    % {
                        'dataset': self.schema_name,
                        'name': self.name,
                        'title': self.title,
                        'folder': self.folder_statement,
                        'identifier': self.identifier,
                    })
        # add it to the dataset
        maql.append('ALTER DATASET {dataset.%(dataset)s} ADD {attr.%(dataset)s.%(name)s};'
                    % {
                        'dataset': self.schema_name,
                        'name': self.name,
                    })
        # change the datatype if needed
        if self.dataType:
            data_type = 'VARCHAR(32)' if self.dataType == 'IDENTITY' else self.dataType
            maql.append('ALTER DATATYPE {%(identifier)s} %(data_type)s;'
                        % {
                            'identifier': self.identifier,
                            'data_type': data_type,
                        })
        else:
            maql.append('')
        return '\n'.join(maql)

    @property
    def folder_statement(self):
        if self.folder:
            return ', FOLDER {folder.%s.attr}' % self.folder
        return ''


class ConnectionPoint(Attribute):

    ldmType = 'CONNECTION_POINT'
    IDENTIFIER = 'f_%(dataset)s.nm_%(name)s'

    def get_maql(self):
        maql = []
        # create the attribute
        maql.append('CREATE ATTRIBUTE {attr.%(dataset)s.%(name)s} VISUAL(TITLE "%(title)s"%(folder)s) AS KEYS {f_%(dataset)s.id} FULLSET;'
                    % {
                        'dataset': self.schema_name,
                        'name': self.name,
                        'title': self.title,
                        'folder': self.folder_statement,
                    })
        # add it to the dataset
        maql.append('ALTER DATASET {dataset.%(dataset)s} ADD {attr.%(dataset)s.%(name)s};'
                    % {
                        'dataset': self.schema_name,
                        'name': self.name,
                    })
        return '\n'.join(maql)

    def get_original_label_maql(self):
        return 'ALTER ATTRIBUTE {attr.%(dataset)s.%(name)s} ADD LABELS {label.%(dataset)s.%(name)s} VISUAL(TITLE "%(title)s") AS {%(identifier)s};'\
                % ({
                        'dataset': self.schema_name,
                        'name': self.name,
                        'title': self.title,
                        'identifier': self.identifier,
                    })


class Fact(Column):

    ldmType = 'FACT'
    IDENTIFIER = 'f_%(dataset)s.f_%(name)s'

    def get_maql(self):
        maql = []
        # create the fact
        maql.append('CREATE FACT {fact.%(dataset)s.%(name)s} VISUAL(TITLE "%(title)s"%(folder)s) AS {%(identifier)s};'
                    % {
                        'dataset': self.schema_name,
                        'name': self.name,
                        'title': self.title,
                        'folder': self.folder_statement,
                        'identifier': self.identifier,
                    })
        # add it to the dataset
        maql.append('ALTER DATASET {dataset.%(dataset)s} ADD {fact.%(dataset)s.%(name)s};'
                    % {
                        'dataset': self.schema_name,
                        'name': self.name,
                    })
        # change the data type if needed
        if self.dataType:
            data_type = 'INT' if self.dataType == 'IDENTITY' else self.dataType
            maql.append('ALTER DATATYPE {%(identifier)s} %(data_type)s;'
                        % {
                            'dataset': self.schema_name,
                            'name': self.name,
                            'data_type': data_type,
                            'identifier': self.identifier,
                        })
        else:
            maql.append('')
        return '\n'.join(maql)

    @property
    def folder_statement(self):
        if self.folder:
            return ', FOLDER {folder.%s.fact}' % self.folder
        return ''


class Date(Fact):

    ldmType = 'DATE'

    def get_maql(self):
        maql = []
        # create the date attriibute
        maql.append('CREATE FACT {dt.%(dataset)s.%(name)s} VISUAL(TITLE "%(title)s (Date)"%(folder)s) AS {f_%(dataset)s.dt_%(name)s};'
                    % {
                        'dataset': self.schema_name,
                        'name': self.name,
                        'title': self.title,
                        'folder': self.folder_statement,
                    })
        # append the date attribute to the dataset
        maql.append('ALTER DATASET {dataset.%(dataset)s} ADD {dt.%(dataset)s.%(name)s};\n'\
                    % {
                        'dataset': self.schema_name,
                        'name': self.name
                    })

        # connect the date attribute to the date dimension
        maql.append('# CONNECT THE DATE TO THE DATE DIMENSION')
        maql.append('ALTER ATTRIBUTE {%(schema_ref)s.date} ADD KEYS {f_%(dataset)s.dt_%(name)s_id};\n'\
                    % {
                        'schema_ref': self.schemaReference,
                        'dataset': self.schema_name,
                        'name': self.name
                    })
        if self.datetime:
            maql = maql + self.time.get_maql()
        return '\n'.join(maql)


class Time(Fact):
    def get_maql(self):
        maql = []

        # create the time attriibute
        maql.append('CREATE FACT {tm.dt.%(dataset)s.%(name)s} VISUAL(TITLE "%(title)s (Time)"%(folder)s) AS {f_%(dataset)s.tm_%(name)s};'
                    % {
                        'dataset': self.schema_name,
                        'name': self.name,
                        'title': self.title,
                        'folder': self.folder_statement,
                    })
        # append the time attribute to the dataset
        maql.append('ALTER DATASET {dataset.%(dataset)s} ADD {tm.dt.%(dataset)s.%(name)s};\n'
                    % {
                        'dataset': self.schema_name,
                        'name': self.name
                    })


        # connect the time attribute to the date dimension
        maql.append('# CONNECT THE TIME TO THE TIME DIMENSION')
        maql.append('ALTER ATTRIBUTE {attr.time.second.of.day.%(schema_reference)s} ADD KEYS {f_%(dataset)s.tm_%(name)s_id};\n'
                    % {
                        'schema_ref': self.schemaReference,
                        'dataset': self.schema_name,
                        'name': self.name
                    })
        return maql

class Reference(Column):

    ldmType = 'REFERENCE'
    IDENTIFIER = 'f_%(dataset)s.%(name)s_id'

    def get_maql(self):
        maql = []
        maql.append('# CONNECT THE REFERENCE TO THE APPROPRIATE DIMENSION')
        maql.append('ALTER ATTRIBUTE {attr.%(schema_ref)s.%(reference)s} ADD KEYS {%(identifier)s};\n'\
                    % {
                        'schema_ref': self.schemaReference,
                        'reference': self.reference,
                        'dataset': self.schema_name,
                        'name': self.name,
                        'identifier': self.identifier,
                    })
        return '\n'.join(maql)


class Label(Column):

    ldmType = 'LABEL'
    IDENTIFIER = 'f_%(dataset)s.nm_%(name)s'

    def get_maql(self):
        maql = []
        maql.append('# ADD LABELS')
        maql.append('ALTER ATTRIBUTE {attr.%(dataset)s.%(reference)s} ADD LABELS {label.%(dataset)s.%(reference)s.%(name)s} VISUAL(TITLE "%(title)s") AS {%(identifier)s};'
                    % {
                        'dataset': self.schema_name,
                        'reference': self.reference,
                        'name': self.name,
                        'title': self.title,
                        'identifier': self.identifier,
                    })
        # TODO: DATATYPE
        maql.append('')
        return '\n'.join(maql)

    def get_maql_default(self):
        return 'ALTER ATTRIBUTE  {attr.%(dataset)s.%(reference)s} DEFAULT LABEL {label.%(dataset)s.%(reference)s.%(name)s};'\
                % {
                    'dataset': self.schema_name,
                    'reference': self.reference,
                    'name': self.name,
                }

    @classmethod
    def from_ldm_to_reference(cls, ldm_str):
        """
        Parses a string like {label.dataset.reference.name}
        and return the reference. This is useful when retrieving
        a SLI manifest.
        """
        return ldm_str.split('.')[2]
