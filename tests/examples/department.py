from gooddataclient.dataset import Dataset
from gooddataclient import columns

class Department(Dataset):

    department = columns.ConnectionPoint(title='Department', folder='Department', dataType='VARCHAR(128)')
    name = columns.Label(title='Name', reference='department', folder='Department', dataType='VARCHAR(128)')
    city = columns.Attribute(title='City', folder='Department', dataType='VARCHAR(20)')

    def data(self):
        #data from django model like Department.object.values('department', 'name')
        return [{'department': u'd1', 'name': u'HQ General Management', 'city': 'San Diego'},
                {'department': u'd2', 'name': u'HQ Information Systems', 'city': 'Denver'},
                {'department': u'd3', 'name': u'HQ Marketing', 'city': 'Denver'},
                {'department': u'd4', 'name': u'HQ Human Resources', 'city': 'NYC'},
                {'department': u'd5', 'name': u'HQ Finance and Accounting', 'city': 'Boston'},
                {'department': u'd11', 'name': u'Store Management', 'city': 'Salt Lake City'},
                {'department': u'd14', 'name': u'Store Information Systems', 'city': 'Washington'},
                {'department': u'd15', 'name': u'Store Permanent Checkers', 'city': 'Houston'},
                {'department': u'd16', 'name': u'Store Temporary Checkers', 'city': 'Boston'},
                {'department': u'd17', 'name': u'Store Permanent Stockers', 'city': 'NYC'},
                {'department': u'd18', 'name': u'Store Temporary Stockers', 'city': 'Washington'},
                {'department': u'd19', 'name': u'Store Permanent Butchers', 'city': 'NYC'},
                ]

    def added_data(self):
        return [{'department': u'd1', 'name': u'HQ General Management', 'city': 'San Diego',
                 'boss': 'John', 'number_of_windows':'13'},
                {'department': u'd2', 'name': u'HQ Information Systems', 'city': 'Denver',
                 'boss': 'John', 'number_of_windows':'12'},
                {'department': u'd3', 'name': u'HQ Marketing', 'city': 'Denver',
                 'boss': 'Jack', 'number_of_windows':'4'},
                {'department': u'd4', 'name': u'HQ Human Resources', 'city': 'NYC',
                 'boss': 'John', 'number_of_windows':'12'},
                {'department': u'd5', 'name': u'HQ Finance and Accounting', 'city': 'Boston',
                 'boss': 'John', 'number_of_windows':'1'},
                {'department': u'd11', 'name': u'Store Management', 'city': 'Salt Lake City',
                 'boss': 'Jack', 'number_of_windows':'90'},
                {'department': u'd14', 'name': u'Store Information Systems', 'city': 'Washington',
                 'boss': 'Jack', 'number_of_windows':'35'},
                {'department': u'd15', 'name': u'Store Permanent Checkers', 'city': 'Houston',
                 'boss': 'Jack', 'number_of_windows':'12'},
                {'department': u'd16', 'name': u'Store Temporary Checkers', 'city': 'Boston',
                 'boss': 'Jacky', 'number_of_windows':'9'},
                {'department': u'd17', 'name': u'Store Permanent Stockers', 'city': 'NYC',
                 'boss': 'Rob', 'number_of_windows':'0'},
                {'department': u'd18', 'name': u'Store Temporary Stockers', 'city': 'Washington',
                 'boss': 'Rob', 'number_of_windows':'3'},
                {'department': u'd19', 'name': u'Store Permanent Butchers', 'city': 'NYC',
                 'boss': 'Rob', 'number_of_windows':'40'},
                ]

    def deleted_data(self):
        return [{'department': u'd1', 'name': u'HQ General Management'},
                {'department': u'd2', 'name': u'HQ Information Systems'},
                {'department': u'd3', 'name': u'HQ Marketing'},
                {'department': u'd4', 'name': u'HQ Human Resources'},
                {'department': u'd5', 'name': u'HQ Finance and Accounting'},
                {'department': u'd11', 'name': u'Store Management'},
                {'department': u'd14', 'name': u'Store Information Systems'},
                {'department': u'd15', 'name': u'Store Permanent Checkers'},
                {'department': u'd16', 'name': u'Store Temporary Checkers'},
                {'department': u'd17', 'name': u'Store Permanent Stockers'},
                {'department': u'd18', 'name': u'Store Temporary Stockers'},
                {'department': u'd19', 'name': u'Store Permanent Butchers'},
                ]


dates = datetimes = []

# maql generated by the java client from the department example 
maql = """
# THIS IS MAQL SCRIPT THAT GENERATES PROJECT LOGICAL MODEL.
# SEE THE MAQL DOCUMENTATION AT http://developer.gooddata.com/api/maql-ddl.html FOR MORE DETAILS

# CREATE DATASET. DATASET GROUPS ALL FOLLOWING LOGICAL MODEL ELEMENTS TOGETHER.
CREATE DATASET {dataset.department} VISUAL(TITLE "Department");

# CREATE THE FOLDERS THAT GROUP ATTRIBUTES AND FACTS
CREATE FOLDER {dim.department} VISUAL(TITLE "Department") TYPE ATTRIBUTE;


# CREATE ATTRIBUTES.
# ATTRIBUTES ARE CATEGORIES THAT ARE USED FOR SLICING AND DICING THE NUMBERS (FACTS)
CREATE ATTRIBUTE {attr.department.department} VISUAL(TITLE "Department", FOLDER {dim.department}) AS KEYS {f_department.id} FULLSET;
ALTER DATASET {dataset.department} ADD {attr.department.department};

# CREATE FACTS
# FACTS ARE NUMBERS THAT ARE AGGREGATED BY ATTRIBUTES.
# CREATE DATE FACTS
# DATES ARE REPRESENTED AS FACTS
# DATES ARE ALSO CONNECTED TO THE DATE DIMENSIONS
# CREATE REFERENCES
# REFERENCES CONNECT THE DATASET TO OTHER DATASETS
# ADD LABELS TO ATTRIBUTES
ALTER ATTRIBUTE {attr.department.department} ADD LABELS {label.department.department.name} VISUAL(TITLE "Name") AS {f_department.nm_name};

ALTER ATTRIBUTE  {attr.department.department} DEFAULT LABEL {label.department.department.name};
ALTER ATTRIBUTE {attr.department.department} ADD LABELS {label.department.department} VISUAL(TITLE "Department") AS {f_department.nm_department};
# SYNCHRONIZE THE STORAGE AND DATA LOADING INTERFACES WITH THE NEW LOGICAL MODEL
SYNCHRONIZE {dataset.department};
"""

# <!-- See documentation at http://developer.gooddata.com/gooddata-cl/xml-config.html -->
schema_xml = '''
<schema>
  <name>Department</name>
  <columns>
    <column>
      <name>department</name>
      <title>Department</title>
      <ldmType>CONNECTION_POINT</ldmType>
      <folder>Department</folder>
    </column>
    <column>
      <name>name</name>
      <title>Name</title>
      <ldmType>LABEL</ldmType>
      <reference>department</reference>
      <folder>Department</folder>
    </column>
    <column>
      <name>city</name>
      <title>City</title>
      <ldmType>ATTRIBUTE</ldmType>
      <dataType>VARCHAR(20)</dataType>
      <folder>Department</folder>
    </column>
  </columns>
</schema>'''

# data generated by the java client from the department example 
data_csv = '''"department","name","city"
"d1","HQ General Management","San Diego"
"d2","HQ Information Systems","Denver"
"d3","HQ Marketing","Denver"
"d4","HQ Human Resources","NYC"
"d5","HQ Finance and Accounting","Boston"
"d11","Store Management","Salt Lake City"
"d14","Store Information Systems","Washington"
"d15","Store Permanent Checkers","Houston"
"d16","Store Temporary Checkers","Boston"
"d17","Store Permanent Stockers","NYC"
"d18","Store Temporary Stockers","Washington"
"d19","Store Permanent Butchers","NYC"
'''

# getSLIManifest in SLI.java
sli_manifest = {"dataSetSLIManifest": {
                   "parts": [{"columnName": "department",
                              "mode": "FULL",
                              "populates": ["label.department.department"],
                              "referenceKey": 1
                              },
                              {"columnName": "name",
                               "mode": "FULL",
                               "populates": ["label.department.department.name"]
                               },
                              {"columnName": "city",
                               "mode": "FULL",
                               "populates": ["label.department.city"]
                               }
                              ],
                    "file": "data.csv",
                    "dataSet": "dataset.department",
                    "csvParams": {"quoteChar": '"',
                                  "escapeChar": '"',
                                  "separatorChar": ",",
                                  "endOfLine": "\n"
                                  }
                    }}
