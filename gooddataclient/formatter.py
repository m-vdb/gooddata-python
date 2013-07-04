from datetime import datetime

from dateutil.parser import parse


NULL = 'NULL'
DATE_NULL = ''
FALSY_DATES = (None, '', 'NULL')
BOOL_TRUE = 'yes'
BOOL_FALSE = 'no'
# GD doesn't support dates > 2049
GD_MAX_YEAR = 2049


def get_date_id(date):
    if not date:
        return 0
    # beginning of GD date ids
    td = datetime(1900, 1, 1)
    delta = date - td

    return delta.days + 1


def get_seconds(date):
    if not date:
        return 0
    return date.hour * 3600 + date.minute * 60 + date.second


def format_dates(line, dates, datetimes):
    """
    A utility function to properly format dates
    """
    # some incredible magic due to GD incompetence
    for date_field in dates + datetimes:
        date_value = line[date_field]
        if date_value in FALSY_DATES:
            date_value = DATE_NULL
        # GD doesn't support dates > GD_MAX_YEAR
        if date_value.year > GD_MAX_YEAR:
            date_value = date_value.replace(year=GD_MAX_YEAR)

        # date id
        line['%s_dt' % date_field] = get_date_id(date_value)

        if date_field in datetimes:
            number_of_seconds = get_seconds(date_value)
            # time id
            line['%s_tm' % date_field] = number_of_seconds
            # time value
            line['tm_%s_id' % date_field] = number_of_seconds

            # date value
            if isinstance(date_value, datetime):
                line[date_field] = date_value.strftime('%Y-%m-%d %H:%M:%S')
            else:
                line[date_field] = DATE_NULL
        else:
            # date value
            if isinstance(date_value, datetime):
                line[date_field] = date_value.strftime('%Y-%m-%d')
            else:
                line[date_field] = DATE_NULL

    return line


def csv_encode(val):
    # handle null values
    if val is None:
        val = NULL
    # handle boolean
    elif isinstance(val, bool):
        val = BOOL_TRUE if val else BOOL_FALSE
    # handle unicode encoding
    elif isinstance(val, unicode):
        val = val.encode('utf-8')
    # handle int, bigint, decimal
    elif not isinstance(val, basestring):
        val = str(val)

    return val


def csv_encode_dict(dict_data):
    """
    A function to encode / format a dictionary values.
    """

    for key, value in dict_data.iteritems():
        dict_data[key] = csv_encode(value)

    return dict_data


def csv_decode(val):
    val = val.decode('utf-8')

    if val == NULL or not val:
        return None
    if val == BOOL_TRUE:
        return True
    if val == BOOL_FALSE:
        return False

    # int & float
    try:
        i = int(val)
        f = float(val)
    except:
        pass
    else:
        return i if i == f else f

    # dates
    try:
        val = parse(val)
    except:
        pass
    else:
        return val

    return val


def csv_decode_dict(dict_data):
    """
    A function to decode / unformat a dictionary values
    """

    for key, value in dict_data.iteritems():
        dict_data[key] = csv_decode(value)

    return dict_data
