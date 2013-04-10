from gooddataclient.dataset import Dataset
from gooddataclient.columns import ConnectionPoint, Label, Reference

class Worker(Dataset):

    worker = ConnectionPoint(title='Worker', folder='Worker')
    firstname = Label(title='First Name', reference='worker', folder='Worker')
    lastname = Label(title='Last Name', reference='worker', folder='Worker')
    department = Reference(title='Department', reference='department', schemaReference='Department', folder='Worker')

    class Meta(Dataset.Meta):
        column_order = ('worker', 'firstname', 'lastname', 'department')


    def data(self):
        return [{'worker': 'e1', 'lastname': 'Nowmer', 'department': 'd1', 'firstname': 'Sheri'},
             {'worker': 'e2', 'lastname': 'Whelply', 'department': 'd1', 'firstname': 'Derrick'},
             {'worker': 'e6', 'lastname': 'Damstra', 'department': 'd2', 'firstname': 'Roberta'},
             {'worker': 'e7', 'lastname': 'Kanagaki', 'department': 'd3', 'firstname': 'Rebecca'},
             {'worker': 'e8', 'lastname': 'Brunner', 'department': 'd11', 'firstname': 'Kim'},
             {'worker': 'e9', 'lastname': 'Blumberg', 'department': 'd11', 'firstname': 'Brenda'},
             {'worker': 'e10', 'lastname': 'Stanz', 'department': 'd5', 'firstname': 'Darren'},
             {'worker': 'e11', 'lastname': 'Murraiin', 'department': 'd11', 'firstname': 'Jonathan'},
             {'worker': 'e12', 'lastname': 'Creek', 'department': 'd11', 'firstname': 'Jewel'},
             {'worker': 'e13', 'lastname': 'Medina', 'department': 'd11', 'firstname': 'Peggy'},
             {'worker': 'e14', 'lastname': 'Rutledge', 'department': 'd11', 'firstname': 'Bryan'},
             {'worker': 'e15', 'lastname': 'Cavestany', 'department': 'd11', 'firstname': 'Walter'},
             {'worker': 'e16', 'lastname': 'Planck', 'department': 'd11', 'firstname': 'Peggy'},
             {'worker': 'e17', 'lastname': 'Marshall', 'department': 'd11', 'firstname': 'Brenda'},
             {'worker': 'e18', 'lastname': 'Wolter', 'department': 'd11', 'firstname': 'Daniel'},
             {'worker': 'e19', 'lastname': 'Collins', 'department': 'd11', 'firstname': 'Dianne'}
             ]


maql = """
# THIS IS MAQL SCRIPT THAT GENERATES PROJECT LOGICAL MODEL.
# SEE THE MAQL DOCUMENTATION AT http://developer.gooddata.com/api/maql-ddl.html FOR MORE DETAILS

# CREATE DATASET. DATASET GROUPS ALL FOLLOWING LOGICAL MODEL ELEMENTS TOGETHER.
CREATE DATASET {dataset.worker} VISUAL(TITLE "Worker");

# CREATE THE FOLDERS THAT GROUP ATTRIBUTES AND FACTS
CREATE FOLDER {dim.worker} VISUAL(TITLE "Worker") TYPE ATTRIBUTE;


# CREATE ATTRIBUTES.
# ATTRIBUTES ARE CATEGORIES THAT ARE USED FOR SLICING AND DICING THE NUMBERS (FACTS)
CREATE ATTRIBUTE {attr.worker.worker} VISUAL(TITLE "Worker", FOLDER {dim.worker}) AS KEYS {f_worker.id} FULLSET;
ALTER DATASET {dataset.worker} ADD {attr.worker.worker};

# CREATE FACTS
# FACTS ARE NUMBERS THAT ARE AGGREGATED BY ATTRIBUTES.
# CREATE DATE FACTS
# DATES ARE REPRESENTED AS FACTS
# DATES ARE ALSO CONNECTED TO THE DATE DIMENSIONS
# CREATE REFERENCES
# REFERENCES CONNECT THE DATASET TO OTHER DATASETS
# CONNECT THE REFERENCE TO THE APPROPRIATE DIMENSION
ALTER ATTRIBUTE {attr.department.department} ADD KEYS {f_worker.department_id};

# ADD LABELS TO ATTRIBUTES
ALTER ATTRIBUTE {attr.worker.worker} ADD LABELS {label.worker.worker.firstname} VISUAL(TITLE "First Name") AS {f_worker.nm_firstname};

ALTER ATTRIBUTE  {attr.worker.worker} DEFAULT LABEL {label.worker.worker.firstname};
# ADD LABELS TO ATTRIBUTES
ALTER ATTRIBUTE {attr.worker.worker} ADD LABELS {label.worker.worker.lastname} VISUAL(TITLE "Last Name") AS {f_worker.nm_lastname};

ALTER ATTRIBUTE {attr.worker.worker} ADD LABELS {label.worker.worker} VISUAL(TITLE "Worker") AS {f_worker.nm_worker};
# SYNCHRONIZE THE STORAGE AND DATA LOADING INTERFACES WITH THE NEW LOGICAL MODEL
SYNCHRONIZE {dataset.worker};
"""

schema_xml = '''
<schema>
  <name>Worker</name>
  <columns>
    <column>
      <name>worker</name>
      <title>Worker</title>
      <ldmType>CONNECTION_POINT</ldmType>
      <folder>Worker</folder>
    </column>
    <column>
      <name>firstname</name>
      <title>First Name</title>
      <ldmType>LABEL</ldmType>
      <reference>worker</reference>
      <folder>Worker</folder>
    </column>
    <column>
      <name>lastname</name>
      <title>Last Name</title>
      <ldmType>LABEL</ldmType>
      <reference>worker</reference>
      <folder>Worker</folder>
    </column>
    <column>
      <name>department</name>
      <title>Department</title>
      <ldmType>REFERENCE</ldmType>
      <reference>department</reference>
      <schemaReference>Department</schemaReference>
      <folder>Worker</folder>
    </column>
  </columns>
</schema>
'''

data_csv = '''"worker","firstname","lastname","department"
"e1","Sheri","Nowmer","d1"
"e2","Derrick","Whelply","d1"
"e6","Roberta","Damstra","d2"
"e7","Rebecca","Kanagaki","d3"
"e8","Kim","Brunner","d11"
"e9","Brenda","Blumberg","d11"
"e10","Darren","Stanz","d5"
"e11","Jonathan","Murraiin","d11"
"e12","Jewel","Creek","d11"
"e13","Peggy","Medina","d11"
"e14","Bryan","Rutledge","d11"
"e15","Walter","Cavestany","d11"
"e16","Peggy","Planck","d11"
"e17","Brenda","Marshall","d11"
"e18","Daniel","Wolter","d11"
"e19","Dianne","Collins","d11"
'''

sli_manifest = {"dataSetSLIManifest": {
  "parts":   [
        {
      "columnName": "worker",
      "mode": "FULL",
      "populates": ["label.worker.worker"],
      "referenceKey": 1
    },
        {
      "columnName": "firstname",
      "mode": "FULL",
      "populates": ["label.worker.worker.firstname"]
    },
        {
      "columnName": "lastname",
      "mode": "FULL",
      "populates": ["label.worker.worker.lastname"]
    },
        {
      "columnName": "department",
      "mode": "FULL",
      "populates": ["label.department.department"],
      "referenceKey": 1
    }
  ],
  "file": "data.csv",
  "dataSet": "dataset.worker",
  "csvParams":   {
    "quoteChar": "\"",
    "escapeChar": "\"",
    "separatorChar": ",",
    "endOfLine": "\n"
  }
}}
