from collections import Iterable
import csv
from datetime import timedelta, datetime
import hashlib
import os
import shutil
from tempfile import mkstemp
from zipfile import ZipFile

import simplejson as json

from gooddataclient.formatter import csv_encode_dict, csv_decode_dict, format_dates


DLI_MANIFEST_FILENAME = 'upload_info.json'
CSV_DATA_FILENAME = 'data.csv'
DEFAULT_ARCHIVE_NAME = 'upload.zip'


def write_tmp_file(content):
    '''Write any data to a temporary file.
    Remember to os.remove(filename) after use.

    @param content: data to be written to a file

    return filename of the created temporary file
    '''
    fp, filename = mkstemp()
    os.write(fp, content)
    os.close(fp)

    return filename


def write_tmp_csv_file(csv_data, sli_manifest, dates, datetimes):
    '''
    Write a CSV temporary file with values in csv_data - list of dicts.

    @param csv_data: list of dicts
    @param sli_manifest: json sli_manifest
    @param dates: list of date fields
    @param datetimes: list of datetime fields
    '''
    fieldnames = [part['columnName'] for part in sli_manifest['dataSetSLIManifest']['parts']]
    fp, filename = mkstemp()
    # FIXME: shouldn't close and reopen the file
    os.close(fp)

    with open(filename, 'w+b') as file:
        writer = csv.DictWriter(
            file, fieldnames=fieldnames,
            delimiter=sli_manifest['dataSetSLIManifest']['csvParams']['separatorChar'],
            quotechar=sli_manifest['dataSetSLIManifest']['csvParams']['quoteChar'],
            quoting=csv.QUOTE_ALL
        )
        headers = dict((n, n) for n in fieldnames)
        writer.writerow(headers)

        for line in csv_data:
            line = format_dates(line, dates, datetimes)
            line = csv_encode_dict(line)
            writer.writerow(line)

    return filename


def write_tmp_zipfile(files):
    '''Zip files into a single file.
    Remember to os.remove(filename) after use.

    @param files: list of tuples (path_to_the_file, name_of_the_file)

    return filename of the created temporary zip file
    '''
    fp, filename = mkstemp()
    # FIXME: shouldn't close and reopen the file
    os.close(fp)

    with ZipFile(filename, "w") as zip_file:
        for path, name in files:
            zip_file.write(path, name)

    return filename


def create_archive(
    data, sli_manifest, dates, datetimes, keep_csv=False,
    csv_file=None, csv_input_path=None
):
    """
    Zip the data and sli_manifest files to an archive.
    Remember to os.remove(filename) after use.

    @param data: csv data
    @param sli_manifest: json sli_manifest
    @param csv_input_path: create archive with this
                           csv data instead of data

    return the filename to the temporary zip file
    """
    if csv_input_path:
        data_path = csv_input_path
    elif isinstance(data, str):
        data_path = write_tmp_file(data)
    elif isinstance(data, Iterable):
        data_path = write_tmp_csv_file(data, sli_manifest, dates, datetimes)
    else:
        raise TypeError('Data should be either a string or an iterable')

    if isinstance(sli_manifest, dict):
        sli_manifest = json.dumps(sli_manifest)

    sli_manifest_path = write_tmp_file(sli_manifest)
    archive = write_tmp_zipfile((
        (data_path, CSV_DATA_FILENAME),
        (sli_manifest_path, DLI_MANIFEST_FILENAME)
    ))

    if keep_csv:
        if not csv_file:
            raise TypeError('Keep csv option with no csv file path')
        shutil.move(data_path, csv_file)
    # do not delete the csv file if push of static data
    elif not csv_input_path:
        os.remove(data_path)
    os.remove(sli_manifest_path)
    return archive


def csv_to_list(data_csv):
    '''
    Create list of dicts from CSV string.

    @param data_csv: CSV in a string
    '''
    reader = csv.DictReader(data_csv.strip().split('\n'))
    data_list = []

    for line in reader:
        line = csv_decode_dict(line)
        data_list.append(line)
    return data_list


def csv_to_iterator(data_csv):
    """
    Create a generator from CSV string.

    @param data_csv: CSV in a string
    """
    reader = csv.DictReader(data_csv.strip().split('\n'))

    for line in reader:
        yield csv_decode_dict(line)
