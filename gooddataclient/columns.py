from gooddataclient.schema.maql import (
    # creation
    ATTRIBUTE_CREATE, ATTRIBUTE_DATATYPE,
    CP_CREATE, CP_DATATYPE, CP_LABEL,
    FACT_CREATE, FACT_DATATYPE, DATE_CREATE,
    TIME_CREATE, REFERENCE_CREATE, LABEL_CREATE,
    LABEL_DEFAULT, LABEL_DATATYPE, HYPERLINK_CREATE,
    # deletion
    FACT_DROP, ATTRIBUTE_DROP, DATE_DROP, TIME_DROP,
    REFERENCE_DROP, LABEL_DROP,
    # alteration
    ATTRIBUTE_ALTER_TITLE, FACT_ALTER_TITLE,
    DATE_ALTER_TITLE, LABEL_ALTER_TITLE,
    HYPERLINK_ALTER_TITLE, TIME_ALTER_TITLE
)
from gooddataclient.text import to_identifier, to_title


class Column(object):

    ldmType = None
    IDENTIFIER = ''
    referenceKey = False
    TEMPLATE_CREATE = None
    TEMPLATE_DATATYPE = None
    TEMPLATE_DROP = None
    TEMPLATE_TITLE = None
    folder_statement = ''

    def __init__(
        self, title, folder=None, reference=None, schemaReference=None,
        dataType=None, datetime=False, format=None, references_cp=False):
        self.title = to_title(title)
        self.folder = to_identifier(folder)
        self.folder_title = to_title(folder)
        self.reference = to_identifier(reference)
        self.schemaReference = to_identifier(schemaReference)
        self.dataType = dataType
        self.datetime = datetime
        self.format = format
        # an attribute useful for labels,
        # to know if they reference a connection point
        self.references_cp = references_cp

    def __getitem__(self, item):
        """
        Useful to do something like `'my string %(format)s' % self`.
        """
        return getattr(self, item)

    def get_schema_values(self):
        values = []
        for key in ('name', 'title', 'folder', 'ldmType', 'reference', 'schemaReference',
                    'dataType', 'datetime', 'format'):
            value = getattr(self, key)
            if value:
                if isinstance(value, bool):
                    value = 'true'
                values.append((key, value))
        return values

    @property
    def identifier(self):
        identifier = self.IDENTIFIER if not self.references_cp else self.IDENTIFIER_CP
        return identifier % self

    def get_sli_manifest_part(self):
        part = {"columnName": self.name,
                "mode": "FULL",
                }
        if self.referenceKey:
            part["referenceKey"] = 1
        if self.format:
            part['constraints'] = {'date': self.format}
        try:
            part['populates'] = self.populates()
        except NotImplementedError:
            pass

        # we return the part as a list of dict,
        # in simple cases the list has one element,
        # but in some cases it can have several parts
        # like Date columns for instance
        return [part]

    def populates(self):
        raise NotImplementedError

    def set_name_and_schema(self, name, schema_name):
        """
        This function can be seen as a hook, particularly
        useful for Date column, which needs to give the
        column name to its eventual subcolumn (time)
        """
        self.name = name
        self.schema_name = schema_name

    def get_maql(self, schema_name=None, name=None, label_references_cp=False):
        # this is useful for columns that are not embedded in datasets,
        # like in migrations
        if schema_name and name:
            self.set_name_and_schema(name, schema_name)
        # useful in case we create a label that references a connection point
        if label_references_cp:
            self.references_cp = label_references_cp

        maql = self.TEMPLATE_CREATE

        # Altering data type if needed
        if self.dataType and self.TEMPLATE_DATATYPE:
            maql += self.TEMPLATE_DATATYPE

        # Adding the time in the case of a date
        # with datetime set to True
        if isinstance(self, Date) and self.datetime:
            maql += self.time.get_maql()

        return maql % self

    def get_drop_maql(self, schema_name, name):
        """
        A function to retrieve the MAQL to drop a column.

        :param schema_name:       the name of the dataset
        :param name:              the name of the column
        """
        self.set_name_and_schema(name, schema_name)
        return self.TEMPLATE_DROP % self

    def get_alter_maql(self, schema_name, name, new_attributes, *args, **kwargs):
        """
        A function to retrieve the MAQL to alter a column.

        :param schema_name:       the name of the dataset
        :param name:              the name of the column
        :param new_attributes:    a dictionary of the new attributes
        """
        self.set_name_and_schema(name, schema_name)
        maql = ''

        try:
            self.title = new_attributes['title']
            if self.TEMPLATE_TITLE:
                maql += self.TEMPLATE_TITLE
        except KeyError:
            pass
        try:
            self.dataType = new_attributes['dataType']
            if self.TEMPLATE_DATATYPE:
                maql += self.TEMPLATE_DATATYPE
        except KeyError:
            pass

        return maql % self


class Attribute(Column):

    ldmType = 'ATTRIBUTE'
    IDENTIFIER = 'd_%(schema_name)s_%(name)s.nm_%(name)s'
    TEMPLATE_CREATE = ATTRIBUTE_CREATE
    TEMPLATE_TITLE = ATTRIBUTE_ALTER_TITLE
    TEMPLATE_DATATYPE = ATTRIBUTE_DATATYPE
    TEMPLATE_DROP = ATTRIBUTE_DROP
    referenceKey = True

    @property
    def folder_statement(self):
        if self.folder:
            return ', FOLDER {folder.%(folder)s.attr}' % self
        return ''

    def populates(self):
        return ["label.%(schema_name)s.%(name)s" % self]


class ConnectionPoint(Attribute):

    ldmType = 'CONNECTION_POINT'
    IDENTIFIER = 'f_%(schema_name)s.nm_%(name)s'
    TEMPLATE_CREATE = CP_CREATE
    TEMPLATE_DATATYPE = CP_DATATYPE

    def get_original_label_maql(self):
        return CP_LABEL % self


class Fact(Column):

    ldmType = 'FACT'
    IDENTIFIER = 'f_%(schema_name)s.f_%(name)s'
    TEMPLATE_CREATE = FACT_CREATE
    TEMPLATE_DATATYPE = FACT_DATATYPE
    TEMPLATE_TITLE = FACT_ALTER_TITLE
    TEMPLATE_DROP = FACT_DROP

    @property
    def folder_statement(self):
        if self.folder:
            return ', FOLDER {folder.%(folder)s.fact}' % self
        return ''

    def populates(self):
        return ["fact.%(schema_name)s.%(name)s" % self]


class Date(Fact):

    ldmType = 'DATE'
    referenceKey = True
    TEMPLATE_CREATE = DATE_CREATE
    TEMPLATE_DROP = DATE_DROP
    TEMPLATE_TITLE = DATE_ALTER_TITLE
    TEMPLATE_DATATYPE = None

    def __init__(self, **kwargs):
        super(Date, self).__init__(**kwargs)
        if self.datetime:
            self.time = Time(**kwargs)

    def populates(self):
        return ["%(schemaReference)s.date.mdyy" % self]

    def get_drop_maql(self, *args, **kwargs):
        maql = self.time.get_drop_maql(*args, **kwargs) if self.datetime else ''

        return maql + super(Date, self).get_drop_maql(*args, **kwargs)

    def get_alter_maql(self, *args, **kwargs):
        maql = self.time.get_alter_maql(*args, **kwargs) if self.datetime else ''

        return maql + super(Date, self).get_alter_maql(*args, **kwargs)

    def get_date_dt_column(self):
         name = '%(name)s_dt' % self
         populates = 'dt.%s.%s' % (to_identifier(self.schema_name), self.name)
         return {'populates': [populates], 'columnName': name, 'mode': 'FULL'}

    def get_sli_manifest_part(self):
        parts = super(Date, self).get_sli_manifest_part()
        parts.append(self.get_date_dt_column())

        if self.datetime:
            parts.extend(self.time.get_sli_manifest_part())

        return parts

    def set_name_and_schema(self, name, schema_name):
        super(Date, self).set_name_and_schema(name, schema_name)
        if self.datetime:
            self.time.set_name_and_schema(name, schema_name)


class Time(Fact):

    TEMPLATE_CREATE = TIME_CREATE
    TEMPLATE_DATATYPE = None
    TEMPLATE_DROP = TIME_DROP
    TEMPLATE_TITLE = TIME_ALTER_TITLE

    def get_time_tm_column(self):
        name = '%(name)s_tm' % self
        populates = 'tm.dt.%s.%s' % (to_identifier(self.schema_name), self.name)
        return {'populates': [populates], 'columnName': name, 'mode': 'FULL'}

    def get_tm_time_id_column(self):
        name = 'tm_%(name)s_id' % self
        populates = 'label.time.second.of.day.%(schemaReference)s' % self
        return {'populates': [populates], 'columnName': name, 'mode': 'FULL', 'referenceKey': 1}

    def get_sli_manifest_part(self):
        return list((self.get_time_tm_column(), self.get_tm_time_id_column()))


class Reference(Column):

    ldmType = 'REFERENCE'
    IDENTIFIER = 'f_%(schema_name)s.%(name)s_id'
    TEMPLATE_CREATE = REFERENCE_CREATE
    TEMPLATE_DROP = REFERENCE_DROP
    referenceKey = True

    def populates(self):
        return ["label.%(schemaReference)s.%(reference)s" % self]


class Label(Column):

    ldmType = 'LABEL'
    IDENTIFIER = 'd_%(schema_name)s_%(reference)s.nm_%(name)s'
    IDENTIFIER_CP = 'f_%(schema_name)s.nm_%(name)s'
    TEMPLATE_CREATE = LABEL_CREATE
    TEMPLATE_DATATYPE = LABEL_DATATYPE
    TEMPLATE_TITLE = LABEL_ALTER_TITLE
    TEMPLATE_DROP = LABEL_DROP

    def get_maql_default(self):
        return LABEL_DEFAULT % self

    def populates(self):
        return ["label.%(schema_name)s.%(reference)s.%(name)s" % self]

    def get_alter_maql(self, hyperlink_change, *args, **kwargs):
        if hyperlink_change:
            self.TEMPLATE_TITLE = HYPERLINK_ALTER_TITLE
        return super(self, Label).get_alter_maql(*args, **kwargs)


class HyperLink(Label):

    ldmType = 'HYPERLINK'
    TEMPLATE_TITLE = HYPERLINK_ALTER_TITLE

    def get_maql(self, schema_name=None, name=None):
        maql = super(HyperLink, self).get_maql(schema_name, name)

        return maql + HYPERLINK_CREATE % self

    def get_alter_maql(self, hyperlink_change, *args, **kwargs):
        if hyperlink_change:
            self.TEMPLATE_TITLE = LABEL_ALTER_TITLE
        return super(self, Label).get_alter_maql(*args, **kwargs)
