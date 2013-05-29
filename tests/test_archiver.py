import sys
import os
import unittest
from tempfile import mkstemp
from zipfile import ZipFile


from gooddataclient.project import Project
from gooddataclient.archiver import (
    create_archive, write_tmp_csv_file,
    csv_to_list, write_tmp_file
)

from tests import logger, examples


logger.set_log_level(debug=('-v' in sys.argv))


class TestArchiver(unittest.TestCase):

    def test_csv(self):
        for (example, ExampleDataset) in examples.examples:
            dataset = ExampleDataset(Project(None))
            csv_filename = write_tmp_csv_file(
                dataset.data(), example.sli_manifest,
                example.dates, example.datetimes
            )
            f = open(csv_filename, 'r')
            content = f.read()
            f.close()
            os.remove(csv_filename)
            self.assertListEqual(csv_to_list(example.data_csv), csv_to_list(content))


    def test_archive(self):
        for (example, ExampleDataset) in examples.examples:
            filename = create_archive(
                example.data_csv, example.sli_manifest, [], []
            )
            zip_file = ZipFile(filename, "r")
            self.assertEquals(None, zip_file.testzip())
            self.assertEquals(zip_file.namelist(), ['data.csv', 'upload_info.json'])
            zip_file.close()
            os.remove(filename)

    def test_keep_csv(self):
        csv_file = os.path.join(os.path.abspath('./'), 'tmp.csv')
        for (example, ExampleDataset) in examples.examples:
            filename = create_archive(
                example.data_csv, example.sli_manifest, [], [],
                keep_csv=True, csv_file=csv_file
            )
            with open(csv_file, 'r') as f:
                content = f.read()
            os.remove(csv_file)
            self.assertListEqual(csv_to_list(example.data_csv), csv_to_list(content))
            self.assertRaises(
                TypeError, create_archive, example.data_csv,
                example.sli_manifest, [], [], keep_csv=True
            )

    def test_mkstemp_file_management(self):
        initial_number_of_opened_files = get_open_fds()

        fp, filename = mkstemp()
        current_number_of_opened_files = get_open_fds()
        self.assertTrue(initial_number_of_opened_files < current_number_of_opened_files)

        os.write(fp, 'fake_content')
        with open(filename) as f:
            self.assertEquals('fake_content', f.read())
        os.close(fp)

        final_number_of_opened_files = get_open_fds()
        self.assertEquals(initial_number_of_opened_files, final_number_of_opened_files)





if __name__ == '__main__':
    unittest.main()


def get_open_fds():
    '''
    return the number of open file descriptors for current process

    .. warning: will only work on UNIX-like os-es.
    '''
    import subprocess
    import os

    pid = os.getpid()
    procs = subprocess.check_output( 
        [ "lsof", '-w', '-Ff', "-p", str( pid ) ] )

    nprocs = len( 
        filter( 
            lambda s: s and s[ 0 ] == 'f' and s[1: ].isdigit(),
            procs.split( '\n' ) )
        )
    return nprocs
