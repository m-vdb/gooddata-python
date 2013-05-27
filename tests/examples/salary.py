from datetime import datetime

from gooddataclient.dataset import Dataset
from gooddataclient.columns import ConnectionPoint, Date, Fact, Reference


payment_date = datetime(2006,1,1)


class Salary(Dataset):

    salary = ConnectionPoint(title='Salary', folder='Salary', dataType='VARCHAR(128)')
    worker = Reference(title='Worker', reference='worker', schemaReference='Worker', folder='Salary', dataType='VARCHAR(128)')
    payment = Fact(title='Payment', folder='Salary', dataType='INT')
    payday = Date(title='Pay Day', schemaReference='payment', folder='Salary')

    class Meta(Dataset.Meta):
        column_order = ('salary', 'worker', 'payment', 'payday')


    def data(self):
        return [{'salary': 's1', 'worker': 'e1', 'payment': 10230, 'payday': payment_date},
                 {'salary': 's2', 'worker': 'e2', 'payment': 4810, 'payday': payment_date},
                 {'salary': 's3', 'worker': 'e6', 'payment': 6080, 'payday': payment_date},
                 {'salary': 's4', 'worker': 'e7', 'payment': 5740, 'payday': payment_date},
                 {'salary': 's5', 'worker': 'e10', 'payment': 6630, 'payday': payment_date},
                 {'salary': 's9', 'worker': 'e23', 'payment': 4230, 'payday': payment_date},
                 {'salary': 's10', 'worker': 'e24', 'payment': 4230, 'payday': payment_date},
                 {'salary': 's11', 'worker': 'e25', 'payment': 3790, 'payday': payment_date},
                 {'salary': 's12', 'worker': 'e26', 'payment': 3420, 'payday': payment_date},
                 {'salary': 's13', 'worker': 'e27', 'payment': 4220, 'payday': payment_date},
                 {'salary': 's14', 'worker': 'e28', 'payment': 3330, 'payday': payment_date},
                 {'salary': 's15', 'worker': 'e29', 'payment': 3990, 'payday': payment_date},
                 {'salary': 's16', 'worker': 'e30', 'payment': 3610, 'payday': payment_date},
                 {'salary': 's17', 'worker': 'e31', 'payment': 4350, 'payday': payment_date},
                 {'salary': 's18', 'worker': 'e32', 'payment': 3340, 'payday': payment_date},
                 {'salary': 's19', 'worker': 'e33', 'payment': 3990, 'payday': payment_date},
                 {'salary': 's20', 'worker': 'e34', 'payment': 3630, 'payday': payment_date}
                 ]

dates = ['payday']
datetimes = []

maql = """
# THIS IS MAQL SCRIPT THAT GENERATES PROJECT LOGICAL MODEL.
# SEE THE MAQL DOCUMENTATION AT http://developer.gooddata.com/api/maql-ddl.html FOR MORE DETAILS

# CREATE DATASET. DATASET GROUPS ALL FOLLOWING LOGICAL MODEL ELEMENTS TOGETHER.
CREATE DATASET {dataset.salary} VISUAL(TITLE "Salary");

# CREATE THE FOLDERS THAT GROUP ATTRIBUTES AND FACTS
CREATE FOLDER {dim.salary} VISUAL(TITLE "Salary") TYPE ATTRIBUTE;

CREATE FOLDER {ffld.salary} VISUAL(TITLE "Salary") TYPE FACT;

# CREATE ATTRIBUTES.
# ATTRIBUTES ARE CATEGORIES THAT ARE USED FOR SLICING AND DICING THE NUMBERS (FACTS)
CREATE ATTRIBUTE {attr.salary.salary} VISUAL(TITLE "Salary", FOLDER {dim.salary}) AS KEYS {f_salary.id} FULLSET;
ALTER DATASET {dataset.salary} ADD {attr.salary.salary};

# CREATE FACTS
# FACTS ARE NUMBERS THAT ARE AGGREGATED BY ATTRIBUTES.
CREATE FACT {fact.salary.payment} VISUAL(TITLE "Payment", FOLDER {ffld.salary}) AS {f_salary.f_payment};
ALTER DATASET {dataset.salary} ADD {fact.salary.payment};

# CREATE DATE FACTS
# DATES ARE REPRESENTED AS FACTS
# DATES ARE ALSO CONNECTED TO THE DATE DIMENSIONS
CREATE FACT {dt.salary.payday} VISUAL(TITLE "Pay Day (Date)", FOLDER {ffld.salary}) AS {f_salary.dt_payday};
ALTER DATASET {dataset.salary} ADD {dt.salary.payday};

# CONNECT THE DATE TO THE DATE DIMENSION
ALTER ATTRIBUTE {payment.date} ADD KEYS {f_salary.dt_payday_id};

# CREATE REFERENCES
# REFERENCES CONNECT THE DATASET TO OTHER DATASETS
# CONNECT THE REFERENCE TO THE APPROPRIATE DIMENSION
ALTER ATTRIBUTE {attr.worker.worker} ADD KEYS {f_salary.worker_id};

ALTER ATTRIBUTE {attr.salary.salary} ADD LABELS {label.salary.salary} VISUAL(TITLE "Salary") AS {f_salary.nm_salary};
# SYNCHRONIZE THE STORAGE AND DATA LOADING INTERFACES WITH THE NEW LOGICAL MODEL
SYNCHRONIZE {dataset.salary};
"""

schema_xml = '''
<schema>
  <name>Salary</name>
  <columns>
    <column>
      <name>salary</name>
      <title>Salary</title>
      <ldmType>CONNECTION_POINT</ldmType>
      <folder>Salary</folder>
    </column>
    <column>
      <name>worker</name>
      <title>Worker</title>
      <ldmType>REFERENCE</ldmType>
      <reference>worker</reference>
      <schemaReference>Worker</schemaReference>
      <folder>Salary</folder>
    </column>
    <column>
      <name>payment</name>
      <title>Payment</title>
      <ldmType>FACT</ldmType>
      <folder>Salary</folder>
    </column>
    <column>
      <name>payday</name>
      <title>Pay Day</title>
      <ldmType>DATE</ldmType>
      <schemaReference>payment</schemaReference>
      <folder>Salary</folder>
    </column>
  </columns>
</schema>
'''

data_csv = '''"salary","worker","payment","payday","payday_dt"
"s1","e1","10230","2006-01-01","38717"
"s2","e2","4810","2006-01-01","38717"
"s3","e6","6080","2006-01-01","38717"
"s4","e7","5740","2006-01-01","38717"
"s5","e10","6630","2006-01-01","38717"
"s9","e23","4230","2006-01-01","38717"
"s10","e24","4230","2006-01-01","38717"
"s11","e25","3790","2006-01-01","38717"
"s12","e26","3420","2006-01-01","38717"
"s13","e27","4220","2006-01-01","38717"
"s14","e28","3330","2006-01-01","38717"
"s15","e29","3990","2006-01-01","38717"
"s16","e30","3610","2006-01-01","38717"
"s17","e31","4350","2006-01-01","38717"
"s18","e32","3340","2006-01-01","38717"
"s19","e33","3990","2006-01-01","38717"
"s20","e34","3630","2006-01-01","38717"
'''

# getSLIManifest in SLI.java
sli_manifest = {"dataSetSLIManifest": {
  "parts":   [
        {
      "columnName": "salary",
      "mode": "FULL",
      "populates": ["label.salary.salary"],
      "referenceKey": 1
    },
        {
      "columnName": "worker",
      "mode": "FULL",
      "populates": ["label.worker.worker"],
      "referenceKey": 1
    },
        {
      "columnName": "payment",
      "mode": "FULL",
      "populates": ["fact.salary.payment"]
    },
        {
      "columnName": "payday_dt",
      "mode": "FULL",
      "populates": ["dt.salary.payday"]
    },
        {
      "columnName": "payday",
      "mode": "FULL",
      "populates": ["payment.date.mdyy"],
      "constraints": {"date": "yyyy-MM-dd"},
      "referenceKey": 1
    }
  ],
  "file": "data.csv",
  "dataSet": "dataset.salary",
  "csvParams":   {
    "quoteChar": "\"",
    "escapeChar": "\"",
    "separatorChar": ",",
    "endOfLine": "\n"
  }
}}
