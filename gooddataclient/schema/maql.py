
SYNCHRONIZE = 'SYNCHRONIZE {dataset.%(schema_name)s};\n'
SYNCHRONIZE_PRESERVE = 'SYNCHRONIZE {dataset.%(schema_name)s} PRESERVE DATA;\n'
ALTER_DATATYPE = 'ALTER DATATYPE {%(identifier)s} %(dataType)s;\n'

###################
# Column creation #
###################

ATTRIBUTE_CREATE = ('CREATE ATTRIBUTE {attr.%(schema_name)s.%(name)s} '
                    'VISUAL(TITLE "%(title)s"%(folder_statement)s) AS KEYS '
                    '{d_%(schema_name)s_%(name)s.id} FULLSET, {f_%(schema_name)s.%(name)s_id};\n'
                    'ALTER DATASET {dataset.%(schema_name)s} ADD {attr.%(schema_name)s.%(name)s};\n'
                    'ALTER ATTRIBUTE {attr.%(schema_name)s.%(name)s} '
                    'ADD LABELS {label.%(schema_name)s.%(name)s} '
                    'VISUAL(TITLE "%(title)s") AS {%(identifier)s};\n')


ATTRIBUTE_DATATYPE = ALTER_DATATYPE

CP_CREATE = ('CREATE ATTRIBUTE {attr.%(schema_name)s.%(name)s} '
             'VISUAL(TITLE "%(title)s"%(folder_statement)s) AS KEYS '
             '{f_%(schema_name)s.id} FULLSET;\n'
             'ALTER DATASET {dataset.%(schema_name)s} ADD {attr.%(schema_name)s.%(name)s};\n')

CP_DATATYPE = ALTER_DATATYPE

CP_LABEL = ('ALTER ATTRIBUTE {attr.%(schema_name)s.%(name)s} '
            'ADD LABELS {label.%(schema_name)s.%(name)s} '
            'VISUAL(TITLE "%(title)s") AS {%(identifier)s};\n')

FACT_CREATE = ('CREATE FACT {fact.%(schema_name)s.%(name)s} '
               'VISUAL(TITLE "%(title)s"%(folder_statement)s) AS {%(identifier)s};\n'
               'ALTER DATASET {dataset.%(schema_name)s} ADD {fact.%(schema_name)s.%(name)s};\n')

FACT_DATATYPE = ALTER_DATATYPE

DATE_CREATE = ('CREATE FACT {dt.%(schema_name)s.%(name)s} '
               'VISUAL(TITLE "%(title)s (Date)"%(folder_statement)s)AS {f_%(schema_name)s.dt_%(name)s};\n'
               'ALTER DATASET {dataset.%(schema_name)s} ADD {dt.%(schema_name)s.%(name)s};\n'
               '# CONNECT THE DATE TO THE DATE DIMENSION\n'
               'ALTER ATTRIBUTE {%(schemaReference)s.date} ADD KEYS {f_%(schema_name)s.dt_%(name)s_id};\n')

TIME_CREATE = ('CREATE FACT {tm.dt.%(schema_name)s.%(name)s} '
               'VISUAL(TITLE "%(title)s (Time)"%(folder_statement)s) AS {f_%(schema_name)s.tm_%(name)s};\n'
               'ALTER DATASET {dataset.%(schema_name)s} ADD {tm.dt.%(schema_name)s.%(name)s};\n'
               '# CONNECT THE TIME TO THE TIME DIMENSION\n'
               'ALTER ATTRIBUTE {attr.time.second.of.day.%(schemaReference)s} '
               'ADD KEYS {f_%(schema_name)s.tm_%(name)s_id};\n')

REFERENCE_CREATE = ('# CONNECT THE REFERENCE TO THE APPROPRIATE DIMENSION\n'
                    'ALTER ATTRIBUTE {attr.%(schemaReference)s.%(reference)s} '
                    'ADD KEYS {%(identifier)s};\n')

LABEL_CREATE = ('# ADD LABELS\n'
                'ALTER ATTRIBUTE {attr.%(schema_name)s.%(reference)s} '
                'ADD LABELS {label.%(schema_name)s.%(reference)s.%(name)s} '
                'VISUAL(TITLE "%(title)s") AS {%(identifier)s};\n')

LABEL_DEFAULT = ('ALTER ATTRIBUTE  {attr.%(schema_name)s.%(reference)s} '
                 'DEFAULT LABEL {label.%(schema_name)s.%(reference)s.%(name)s};\n')

HYPERLINK_CREATE = ('ALTER ATTRIBUTE {attr.%(schema_name)s.%(reference)s} '
                    'ALTER LABELS {label.%(schema_name)s.%(reference)s.%(name)s} HYPERLINK;\n')

LABEL_DATATYPE = ALTER_DATATYPE

###################
# Column deletion #
###################

ATTRIBUTE_DROP = 'DROP IF EXISTS {attr.%(schema_name)s.%(name)s} CASCADE;\n'

FACT_DROP = 'DROP IF EXISTS {fact.%(schema_name)s.%(name)s} CASCADE;\n'

DATE_DROP = ('DROP IF EXISTS {dt.%(schema_name)s.%(name)s} CASCADE;\n'
             'ALTER ATTRIBUTE {%(schemaReference)s.date} '
             'DROP KEYS {f_%(schema_name)s.dt_%(name)s_id};\n')

TIME_DROP = ('DROP IF EXISTS {tm.dt.%(schema_name)s.%(name)s} CASCADE;\n'
             'ALTER ATTRIBUTE {attr.time.second.of.day.%(schemaReference)s} '
             'DROP KEYS {f_%(schema_name)s.tm_%(name)s_id};\n')

REFERENCE_DROP = 'ALTER ATTRIBUTE {attr.%(schemaReference)s.%(reference)s} DROP KEYS {%(identifier)s}\n;'

LABEL_DROP = 'ALTER ATTRIBUTE {attr.%(schema_name)s.%(reference)s} DROP LABELS {label.%(schema_name)s.%(reference)s.%(name)s};\n'

#####################
# Column alteration #
#####################

ATTRIBUTE_ALTER_TITLE = 'ALTER ATTRIBUTE {attr.%(schema_name)s.%(name)s} VISUAL(TITLE "%(title)s");\n'

FACT_ALTER_TITLE = 'ALTER FACT {fact.%(schema_name)s.%(name)s} VISUAL(TITLE "%(title)s");\n'


################
# Row deletion #
################

DELETE_ROW = 'DELETE FROM {attr.%(schema_name)s.%(connection_point)s} WHERE %(where_clause)s;'
