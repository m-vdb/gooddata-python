
###################
# Column creation #
###################

ATTRIBUTE_CREATE = ('CREATE ATTRIBUTE {attr.%(dataset)s.%(name)s} '
                    'VISUAL(TITLE "%(title)s"%(folder)s) AS KEYS '
                    '{d_%(dataset)s_%(name)s.id} FULLSET, {f_%(dataset)s.%(name)s_id};\n'
                    'ALTER DATASET {dataset.%(dataset)s} ADD {attr.%(dataset)s.%(name)s};\n'
                    'ALTER ATTRIBUTE {attr.%(dataset)s.%(name)s} '
                    'ADD LABELS {label.%(dataset)s.%(name)s} '
                    'VISUAL(TITLE "%(title)s") AS {%(identifier)s};\n')


ATTRIBUTE_DATATYPE = 'ALTER DATATYPE {%(identifier)s} %(data_type)s;\n'

CP_CREATE = ('CREATE ATTRIBUTE {attr.%(dataset)s.%(name)s} '
             'VISUAL(TITLE "%(title)s"%(folder)s) AS KEYS '
             '{f_%(dataset)s.id} FULLSET;\n'
             'ALTER DATASET {dataset.%(dataset)s} ADD {attr.%(dataset)s.%(name)s};\n')

CP_DATATYPE = ATTRIBUTE_DATATYPE

CP_LABEL = ('ALTER ATTRIBUTE {attr.%(dataset)s.%(name)s} '
            'ADD LABELS {label.%(dataset)s.%(name)s} '
            'VISUAL(TITLE "%(title)s") AS {%(identifier)s};\n')

FACT_CREATE = ('CREATE FACT {fact.%(dataset)s.%(name)s} '
               'VISUAL(TITLE "%(title)s"%(folder)s) AS {%(identifier)s};\n'
               'ALTER DATASET {dataset.%(dataset)s} ADD {fact.%(dataset)s.%(name)s};\n')

FACT_DATATYPE = ATTRIBUTE_DATATYPE

DATE_CREATE = ('CREATE FACT {dt.%(dataset)s.%(name)s} '
               'VISUAL(TITLE "%(title)s (Date)"%(folder)s)AS {f_%(dataset)s.dt_%(name)s};\n'
               'ALTER DATASET {dataset.%(dataset)s} ADD {dt.%(dataset)s.%(name)s};\n'
               '# CONNECT THE DATE TO THE DATE DIMENSION\n'
               'ALTER ATTRIBUTE {%(schema_ref)s.date} ADD KEYS {f_%(dataset)s.dt_%(name)s_id};\n')

TIME_CREATE = ('CREATE FACT {tm.dt.%(dataset)s.%(name)s} '
               'VISUAL(TITLE "%(title)s (Time)"%(folder)s) AS {f_%(dataset)s.tm_%(name)s};\n'
               'ALTER DATASET {dataset.%(dataset)s} ADD {tm.dt.%(dataset)s.%(name)s};\n'
               '# CONNECT THE TIME TO THE TIME DIMENSION\n'
               'ALTER ATTRIBUTE {attr.time.second.of.day.%(schema_ref)s} '
               'ADD KEYS {f_%(dataset)s.tm_%(name)s_id};\n')

REFERENCE_CREATE = ('# CONNECT THE REFERENCE TO THE APPROPRIATE DIMENSION\n'
                    'ALTER ATTRIBUTE {attr.%(schema_ref)s.%(reference)s} '
                    'ADD KEYS {%(identifier)s};\n')

LABEL_CREATE = ('# ADD LABELS\n'
                'ALTER ATTRIBUTE {attr.%(dataset)s.%(reference)s} '
                'ADD LABELS {label.%(dataset)s.%(reference)s.%(name)s} '
                'VISUAL(TITLE "%(title)s") AS {%(identifier)s};\n')

LABEL_DEFAULT = ('ALTER ATTRIBUTE  {attr.%(dataset)s.%(reference)s} '
                 'DEFAULT LABEL {label.%(dataset)s.%(reference)s.%(name)s};\n')

HYPERLINK_CREATE = ('ALTER ATTRIBUTE {attr.%(dataset)s.%(reference)s} '
                    'ALTER LABELS {label.%(dataset)s.%(reference)s.%(name)s} HYPERLINK;\n')

LABEL_DATATYPE = ATTRIBUTE_DATATYPE

###################
# Column deletion #
###################
