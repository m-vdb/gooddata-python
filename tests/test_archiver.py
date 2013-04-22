import sys
import os
import unittest
from zipfile import ZipFile

from gooddataclient.project import Project
from gooddataclient.archiver import create_archive, write_tmp_csv_file, \
    csv_to_list

from tests import logger, examples, get_parser


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


if __name__ == '__main__':
    args = get_parser().parse_args()
    logger.logger.setLevel(args.loglevel)
    del sys.argv[1:]
    unittest.main()
