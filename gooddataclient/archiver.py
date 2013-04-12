import os
from tempfile import mkstemp
from zipfile import ZipFile
import datetime
import hashlib
from collections import Iterable

import simplejson as json


DLI_MANIFEST_FILENAME = 'upload_info.json'
CSV_DATA_FILENAME = 'data.csv'
DEFAULT_ARCHIVE_NAME = 'upload.zip'


def csv_encode(val):
    if not isinstance(val, basestring):
        val = str(val)
    return '"' + val.replace('"', '""') + '"'


def value_list_from_dict(values, fields):
    return [values[field] for field in fields]


def write_csv_line(file_handle, values):
    """
    A function to write a csv line,
    as csv python module does not handle
    """
    fmt_values = map(csv_encode, values)
    file_handle.write((','.join(fmt_values)).encode('UTF-8'))
    file_handle.write("\n")


def write_tmp_file(content):
    '''Write any data to a temporary file.
    Remember to os.remove(filename) after use.

    @param content: data to be written to a file

    return filename of the created temporary file
    '''
    fp, filename = mkstemp()

    with open(filename, 'w+b') as file:
        file.write(content)

    return filename


def write_tmp_csv_file(csv_data, sli_manifest):
    '''Write a CSV temporary file with values in csv_data - list of dicts.

    @param csv_data: list of dicts
    @param sli_manifest: json sli_manifest
    '''
    fieldnames = [part['columnName'] for part in sli_manifest['dataSetSLIManifest']['parts']]
    fp, filename = mkstemp()

    with open(filename, 'w+b') as file:

        write_csv_line(file, fieldnames)

        for line in csv_data:
            for key in fieldnames:
                #some incredible magic with additional date field
                if not key in line and key.endswith('_dt'):
                    h = hashlib.md5()
                    h.update(line[key[:-3]])
                    line[key] = h.hexdigest()[:6]
                #formatting the date properly
                if isinstance(line[key], datetime.datetime):
                    line[key] = line[key].strftime("%Y-%m-%d")
                #make 0/1 from bool
                if isinstance(line[key], bool):
                    line[key] = int(line[key])

            write_csv_line(file, value_list_from_dict(line, fieldnames))

    return filename


def write_tmp_zipfile(files):
    '''Zip files into a single file.
    Remember to os.remove(filename) after use.
    
    @param files: list of tuples (path_to_the_file, name_of_the_file)
    
    return filename of the created temporary zip file
    '''
    fp, filename = mkstemp()

    with ZipFile(filename, "w") as zip_file:
        for path, name in files:
            zip_file.write(path, name)

    return filename


def create_archive(data, sli_manifest):
    """
    Zip the data and sli_manifest files to an archive.
    Remember to os.remove(filename) after use.

    @param data: csv data
    @param sli_manifest: json sli_manifest

    return the filename to the temporary zip file
    """
    if isinstance(data, str):
        data_path = write_tmp_file(data)
    elif isinstance(data, Iterable):
        data_path = write_tmp_csv_file(data, sli_manifest)
    else:
        raise TypeError('Data should be either a string or an iterable')

    if isinstance(sli_manifest, dict):
        sli_manifest = json.dumps(sli_manifest)

    sli_manifest_path = write_tmp_file(sli_manifest)
    archive = write_tmp_zipfile((
        (data_path, CSV_DATA_FILENAME),
        (sli_manifest_path, DLI_MANIFEST_FILENAME)
    ))
    os.remove(data_path)
    os.remove(sli_manifest_path)
    return archive


def csv_to_list(data_csv):
    '''Create list of dicts from CSV string.
    
    @param data_csv: CSV in a string
    '''
    reader = csv.reader(data_csv.strip().split('\n'))
    header = reader.next()
    data_list = []
    for line in reader:
        l = {}
        for i, value in enumerate(header):
            l[value] = line[i]
        data_list.append(l)
    return data_list

