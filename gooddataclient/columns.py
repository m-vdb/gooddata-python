from gooddataclient.schema.maql import (
    #creation
    ATTRIBUTE_CREATE, ATTRIBUTE_DATATYPE,
    CP_CREATE, CP_DATATYPE, CP_LABEL,
    FACT_CREATE, FACT_DATATYPE, DATE_CREATE,
    TIME_CREATE, REFERENCE_CREATE, LABEL_CREATE,
    LABEL_DEFAULT, LABEL_DATATYPE, HYPERLINK_CREATE,
    #deletion
    FACT_DROP, ATTRIBUTE_DROP
)
from gooddataclient.text import to_identifier, to_title


class Column(object):

    ldmType = None
    IDENTIFIER = ''
    referenceKey = False
    TEMPLATE_CREATE = None
    TEMPLATE_DATATYPE = None
    TEMPLATE_DROP = None

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
        # an attribute useful for labels,
        # to now if they references a connection point
        self.references_cp = False

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
        return identifier % {
            'dataset': self.schema_name,
            'name': self.name,
            'reference': self.reference,
        }

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

    def get_maql(self, schema_name=None, name=None):
        # this is useful for columns that are not embedded in datasets,
        # like in migrations
        if schema_name and name:
            self.set_name_and_schema(name, schema_name)

        maql = self.TEMPLATE_CREATE

        # Altering data type if needed
        if self.dataType and self.TEMPLATE_DATATYPE:
            maql = maql + self.TEMPLATE_DATATYPE

        # Adding the time in the case of a date
        # with datetime set to True
        if isinstance(self, Date) and self.datetime:
            maql = maql + self.time.get_maql(name, schema_name)

        return maql % {
            'dataset': self.schema_name,
            'name': self.name,
            'title': self.title,
            'folder': self.folder_statement,
            'identifier': self.identifier,
            'data_type': self.dataType,
            'schema_ref': self.schemaReference,
            'reference': self.reference
        }

    def get_drop_maql(self, schema_name, name):
        return self.TEMPLATE_DROP % {
            'dataset': shema_name,
            'name': name,
        }


class Attribute(Column):

    ldmType = 'ATTRIBUTE'
    IDENTIFIER = 'd_%(dataset)s_%(name)s.nm_%(name)s'
    TEMPLATE_CREATE = ATTRIBUTE_CREATE
    TEMPLATE_DATATYPE = ATTRIBUTE_DATATYPE
    TEMPLATE_DROP = ATTRIBUTE_DROP
    referenceKey = True

    @property
    def folder_statement(self):
        if self.folder:
            return ', FOLDER {folder.%s.attr}' % self.folder
        return ''

    def populates(self):
        return ["label.%s.%s" % (self.schema_name, self.name)]


class ConnectionPoint(Attribute):

    ldmType = 'CONNECTION_POINT'
    IDENTIFIER = 'f_%(dataset)s.nm_%(name)s'
    TEMPLATE_CREATE = CP_CREATE
    TEMPLATE_DATATYPE = CP_DATATYPE

    def get_original_label_maql(self):
        return CP_LABEL % {
            'dataset': self.schema_name,
            'name': self.name,
            'title': self.title,
            'identifier': self.identifier,
        }


class Fact(Column):

    ldmType = 'FACT'
    IDENTIFIER = 'f_%(dataset)s.f_%(name)s'
    TEMPLATE_CREATE = FACT_CREATE
    TEMPLATE_DATATYPE = FACT_DATATYPE
    TEMPLATE_DROP = FACT_DROP

    @property
    def folder_statement(self):
        if self.folder:
            return ', FOLDER {folder.%s.fact}' % self.folder
        return ''

    def populates(self):
        return ["fact.%s.%s" % (self.schema_name, self.name)]


class Date(Fact):

    ldmType = 'DATE'
    referenceKey = True
    TEMPLATE_CREATE = DATE_CREATE

    def __init__(self, **kwargs):
        super(Date, self).__init__(**kwargs)
        if self.datetime:
            self.time = Time(**kwargs)

    def populates(self):
        return ["%s.date.mdyy" % self.schemaReference]

    def get_date_dt_column(self):
         name = '%s_dt' % self.name
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

    def get_time_tm_column(self):
        name = '%s_tm' % self.name
        populates = 'tm.dt.%s.%s' % (to_identifier(self.schema_name), self.name)
        return {'populates': [populates], 'columnName': name, 'mode': 'FULL'}

    def get_tm_time_id_column(self):
        name = 'tm_%s_id' % self.name
        populates = 'label.time.second.of.day.%s' % self.schemaReference
        return {'populates': [populates], 'columnName': name, 'mode': 'FULL', 'referenceKey': 1}

    def get_sli_manifest_part(self):
        return list((self.get_time_tm_column(), self.get_tm_time_id_column()))


class Reference(Column):

    ldmType = 'REFERENCE'
    IDENTIFIER = 'f_%(dataset)s.%(name)s_id'
    TEMPLATE_CREATE = REFERENCE_CREATE
    referenceKey = True

    def populates(self):
        return ["label.%s.%s" % (self.schemaReference, self.reference)]


class Label(Column):

    ldmType = 'LABEL'
    IDENTIFIER = 'd_%(dataset)s_%(reference)s.nm_%(name)s'
    IDENTIFIER_CP = 'f_%(dataset)s.nm_%(name)s'
    TEMPLATE_CREATE = LABEL_CREATE
    TEMPLATE_DATATYPE = LABEL_DATATYPE

    def get_maql_default(self):
        return LABEL_DEFAULT % {
            'dataset': self.schema_name,
            'reference': self.reference,
            'name': self.name,
        }

    def populates(self):
        return ["label.%s.%s.%s" % (self.schema_name, self.reference, self.name)]


class HyperLink(Label):

    ldmType = 'HYPERLINK'

    def get_maql(self, schema_name=None, name=None):
        maql = super(HyperLink, self).get_maql(schema_name, name)

        return maql + HYPERLINK_CREATE % {
            'dataset': self.schema_name,
            'reference': self.reference,
            'name': self.name,
            'title': self.title,
        }
