from datetime import datetime

NULL = 'NULL'
DATE_NULL = ''
FALSY_DATES = (None, '', 'NULL')
BOOL_TRUE = 'yes'
BOOL_FALSE = 'no'


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

        # date id
        line['%s_dt' % date_field] = get_date_id(date_value)
        # date value
        if isinstance(date_value, datetime):
            line[date_field] = date_value.strftime('%Y-%m-%d')
        else:
            line[date_field] = DATE_NULL

        if date_field in datetimes:
            number_of_seconds = get_seconds(date_value)
            # time id
            line['%s_tm' % date_field] = number_of_seconds
            # time value
            line['tm_%s_id' % date_field] = number_of_seconds

    return line


def csv_encode(val):
    #handle null values
    if val is None:
        val = NULL
    elif isinstance(val, bool):
        val = BOOL_TRUE if val else BOOL_FALSE
    #handle int, bigint, decimal
    elif not isinstance(val, basestring):
        val = str(val)
    return '"' + val.replace('"', '""') + '"'
