import re
from xml.dom.minidom import parseString

from gooddataclient.columns import (
    Attribute, Label, HyperLink, ConnectionPoint, Reference, Date, Fact
)


def get_xml_schema(dataset):
    '''Create XML schema from list of columns in dicts. It's used to create
    MAQL through the Java Client.
    '''
    dom = parseString('<schema><name>%s</name><columns></columns></schema>' % dataset.schema_name)
    for _, column in dataset._columns:
        xmlcol = dom.createElement('column')
        for key, val in column.get_schema_values():
            k = dom.createElement(key)
            v = dom.createTextNode(val)
            k.appendChild(v)
            xmlcol.appendChild(k)
        dom.childNodes[0].childNodes[1].appendChild(xmlcol)
    return dom.toxml()


def retrieve_column_tuples(column_json, category, pk_identifier, dlc_info):
    """
    A utility function, that, given a column_json, returns
    a tuple of `column_name`, `Column`.

    :param column_json:     the json of the column
    :param category:        the category of the column
                            (fact, attribute)
    :param pk_identifier:   the pk identifier of the column (None for facts)
    :param dlc_info:        additional information to find references / dataTypes
    """
    if category == 'attributes':
        return retrieve_attr_tuples(column_json, pk_identifier, dlc_info)
    return retrieve_fact_tuples(column_json, dlc_info)


def retrieve_attr_tuples(column_json, pk_identifier, dlc_info):
    """
    Retrive a tuple of `attr_name`, `Attribute` from a json.
    It will also retrieve labels / hyperlink tuples.
    """
    _, dataset, column_name = column_json['meta']['identifier'].split('.')
    column_title = column_json['meta']['title']
    tuples = []

    # manage labels and hyperlinks
    default_label = 'label.%s.%s' % (dataset, column_name)
    for label_json in column_json['content']['displayForms']:
        if label_json['meta']['identifier'] == default_label:
            continue

        _, __, label_reference, label_name = label_json['meta']['identifier'].split('.')
        label_title = label_json['meta']['title']
        label_type = label_json['content'].get('type', None)
        data_type= dlc_info.get(label_name, {}).get('dataType', None)

        if label_type == 'GDC.link':
            label_column = HyperLink(
                title=label_title, reference=label_reference,
                dataType=data_type, references_cp=attr_is_cp(pk_identifier, dataset)
            )
        else:
            label_column = Label(
                title=label_title, reference=label_reference,
                dataType=data_type, references_cp=attr_is_cp(pk_identifier, dataset)
            )
        tuples.append((label_name, label_column))

    # ConnectionPoint or Attribute
    data_type = dlc_info.get(column_name, {}).get('dataType', None)
    if attr_is_cp(pk_identifier, dataset):
        tuples.append((column_name, ConnectionPoint(title=column_title, dataType=data_type)))
    else:
        tuples.append((column_name, Attribute(title=column_title, dataType=data_type)))

    return tuples


def attr_is_cp(pk_identifier, dataset):
    """
    A utility function to find out if an attribute is a connection point.
    """
    cp_identifier = 'col.f_%s.id' % dataset
    return pk_identifier == cp_identifier


def get_column_id(col_json):
    """
    Retrieve API Id for the column.

    :param col_json:      a json representation of the column
    """
    return col_json['meta']['uri'].split('/')[-1]


def get_user_cp_info(user_cp_json):
    """
    A function to retrieve the dataset and the column
    that use a connection point, based on the json of
    such a column.
    """
    match = re.match("col\.f_([a-z_]+)\.([a-z_]+)_id", user_cp_json["title"])
    if not match:
        return None, None
    return match.group(1), match.group(2)


def retrieve_fact_tuples(column_json, dlc_info):
    """
    Retrive a tuple of `fact_name`, `Fact` from a json. The fact
    can also be a date.
    It will also retrieve labels / hyperlink tuples.
    """
    identifier = column_json['meta']['identifier'].split('.')
    column_title = column_json['meta']['title']

    if len(identifier) > 3:
        return []

    category, dataset, column_name = identifier

    if category == 'dt':
        column_title = column_title.replace(' (Date)', '')
        datetime = dlc_info.get('%s__time' % column_name, {}).get('datetime', False)
        return [
            (column_name, Date(
                    title=column_title,
                    schemaReference=dlc_info[column_name]['schemaReference'],
                    datetime=datetime
            ))
        ]

    data_type = dlc_info.get(column_name, {}).get('dataType', None)
    return [(column_name, Fact(title=column_title, dataType=data_type))]


def retrieve_dlc_info(dataset_name, column_json, sli_manifest):
    """
    A function to retrieve information about data loading column.
    """
    identifier = column_json['meta']['identifier']
    column_type = column_json['content']['columnType']
    column_length = column_json['content']['columnLength']
    precision = column_json['content']['columnPrecision']

    precision = ',%s' % precision if precision else ''
    if column_length:
        data_type = '%s(%s%s)' % (column_type, column_length, precision)
    else:
        data_type = column_type

    match = re.match("d_[a-z_1-9]+\.nm_([a-z_1-9]+)", identifier)
    match_fact = re.match("f_[a-z_1-9]+.f_([a-z_1-9]+)", identifier)
    match_cp = re.match("f_[a-z_1-9]+\.nm_([a-z_1-9]+)", identifier)
    match_dt = re.match("f_%s\.dt_([a-z_1-9]+)_id" % dataset_name, identifier)
    match_tm = re.match("f_%s\.tm_([a-z_1-9]+)" % dataset_name, identifier)
    match_id = re.match("f_%s\.([a-z_1-9]+)_id" % dataset_name, identifier)

    match = match or match_cp or match_fact
    if match:

        return (match.group(1), {
            'dataType': data_type,
            'identifier': identifier,
        })

    # retrieve datetime = True if needed
    if match_tm:
        return ('%s__time' % match_tm.group(1), {
            'datetime': True,
            'identifier': identifier,
        })

    # retrieve the date reference
    if match_dt:
        for part in sli_manifest:
            if part['columnName'] == identifier:
                schema_ref, _, __ = part['populates'][0].split('.')
                return (match_dt.group(1), {
                    'schemaReference': schema_ref,
                    'identifier': identifier
                })

    if match_id:
        return (match_id.group(1), {
            'identifier': identifier,
            'is_ref': True,
        })

    return None


def get_references(dataset_name, sli_manifest):
    """
    A function to read the SLI manifest and retrieve
    all the references in it, that points to other datasets.

    :param dataset_name:         the name of the dataset
    :param sli_manifest:         the SLI manifest of the dataset
    """
    ref_list = []
    pattern = r'f_([a-z]+)\.nm_[a-z_]+'
    for part in sli_manifest:
        match = re.match(pattern, part["columnName"])
        if match:
            if match.group(1) != dataset_name:
                _, schema_ref, reference = part["populates"][0].split('.')[:3]
                ref_list.append((schema_ref, reference))

    return dict(ref_list)
