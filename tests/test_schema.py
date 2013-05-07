import sys
import unittest
from xml.dom.minidom import parseString

from gooddataclient.schema.utils import get_xml_schema
from gooddataclient.project import Project

from tests import logger, examples, get_parser


logger.set_log_level(debug=('-v' in sys.argv))


class TestSchema(unittest.TestCase):

    def test_xml_schema(self):
        for (example, ExampleDataset) in examples.examples:
            schema = parseString(example.schema_xml.replace(' ', '').replace('\n', ''))
            dataset = ExampleDataset(Project(None))
            gen_schema = parseString(get_xml_schema(dataset))
            # TODO: test for XML content (not so easily comparable)
            self.assertEqual(len(schema.childNodes), len(gen_schema.childNodes))
            self.assertEqual(len(schema.childNodes[0].childNodes), len(gen_schema.childNodes[0].childNodes),
                             '%s != %s' % (', '.join(n.nodeName for n in schema.childNodes[0].childNodes),
                                           ', '.join(n.nodeName for n in gen_schema.childNodes[0].childNodes)))
            self.assertEqual(len(schema.childNodes[0].childNodes[1].childNodes),
                             len(gen_schema.childNodes[0].childNodes[1].childNodes),
                             '%s != %s (%s)' % (', '.join(n.nodeName for n in schema.childNodes[0].childNodes[1].childNodes),
                                                ', '.join(n.nodeName for n in gen_schema.childNodes[0].childNodes[1].childNodes),
                                                example))


if __name__ == '__main__':
    unittest.main()
