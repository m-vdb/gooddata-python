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


def retrieve_column_tuples(column_json, category, pk_identifier):
    """
    A utility function, that, given a column_json, returns
    a tuple of `column_name`, `Column`.

    :param column_json:     the json of the column
    :param category:        the category of the column
                            (fact, attribute)
    :param pk_identifier:   the pk identifier of the column (None for facts)
    """
    if category == 'attributes':
        return retrieve_attr_tuples(column_json, pk_identifier)
    return retrieve_fact_tuples(column_json)


def retrieve_attr_tuples(column_json, pk_identifier):
    """
    Retrive a tuple of `attr_name`, `Attribute` from a json.
    It will also retrieve labels / hyperlink tuples.
    """
    _, dataset, column_name = column_json['meta']['identifier'].split('.')
    column_title = column_json['meta']['title']

    tuples = []
    default_label = 'label.%s.%s' % (dataset, column_nameq)
    for label_json in column_json['content']['displayForms']:
        if label_json['meta']['identifier'] == default_label:
            continue

        _, __, label_reference, label_name = label_json['meta']['identifier'].split('.')
        label_title = label_json['meta']['title']
        # FIXME: have DLC before + HyperLink
        tuples.append((label_name, Label(title=label_title, reference=label_reference)))

    # ConnectionPoint or Attribute
    cp_identifier = 'col.f_%s.id' % dataset
    if pk_identifier == cp_identifier:
        tuples.append((column_name, ConnectionPoint(title=column_title)))
    else:
        tuples.append((column_name, Attribute(title=column_title)))

    return tuples


def retrieve_fact_tuples(column_json):
    """
    Retrive a tuple of `fact_name`, `Fact` from a json. The fact
    can also be a date.
    It will also retrieve labels / hyperlink tuples.
    """
    identifier = column_json['meta']['identifier'].split('.')

    if identifier[0] == 'fact':
        category = 'fact'
        column_title = column_json['meta']['title']
    elif identifier[0] == 'dt':
        category = 'date'
        column_title = column_json['meta']['title'].replace(' (Date)', '')
    else:
        identifier.pop(0)
        category = 'time'

    _, dataset, column_name = identifier

    # in case of datetime = True
    if category == 'time':
        return [('%s__time' % column_name, ('datetime', True))]

    if category == 'date':
        # FIXME: need dlc to have reference
        return [(column_name, Date(title=column_title, format='yyyy-MM-dd'))]

    return [(column_name, Fact(title=column_title))]


def retrieve_dlc_info(column_json):
    """
    A function to retrieve information about data loading column.
    """
    return []
