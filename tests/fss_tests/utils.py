import os
from lxml import etree

parser = etree.XMLParser(encoding='cp1251', remove_comments=True)


class Input:
    def __init__(self, filename: str = None, xml_data: bytes = None) -> None:
        self.filename = filename
        self.content = xml_data
        self.xml_obj = etree.fromstring(self.content, parser=parser)
        self.xsd_schema = None

    def resolve_file(self, xml_filename: str, xsd_root: str) -> 'Input':
        xsd_file = os.path.join(xsd_root, 'f4form_2017_3.xsd')
        with open(xsd_file, 'r', encoding='cp1251') as fd:
            self.xsd_schema = etree.parse(fd, parser).getroot()

        return self
