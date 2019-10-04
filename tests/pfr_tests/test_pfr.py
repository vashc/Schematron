import os
import asyncio
from lxml import etree
from time import time
from src.schemachecker.pfr.pfr_checker import PfrChecker
from tests.utils import get_file_list
from tests.pfr_tests.utils import Input

root = os.path.dirname(os.path.abspath(__file__))
xsd_root = os.path.join(root, 'xsd')
xml_root = os.path.join(root, 'xml')


class TestPfrChecker:
    checker = PfrChecker(root=xsd_root)
    checker.setup_compendium()
    # На тесте только одна бд, вручную изменяем поле класса для работы
    checker.db_num = 1

    # TODO: use mock to simulate basex client
    def test_pfr_checker_szv(self):
        szv_root = os.path.join(xml_root, 'СЗВ')
        files = get_file_list(szv_root)

        for file in files:
            with open(os.path.join(szv_root, file), 'rb') as fd:
                xml_data = fd.read()
                xml_content = etree.fromstring(xml_data, parser=self.checker.utf_parser)
                input = Input(file, xml_data, xml_content)
                self.checker.check_file(input, os.path.join(szv_root, file))
                print(input.verify_result)

    def test_pfr_checker_adv(self):
        szv_root = os.path.join(xml_root, 'АДВ')
        files = get_file_list(szv_root)

        for file in files:
            with open(os.path.join(szv_root, file), 'rb') as fd:
                xml_data = fd.read()
                xml_content = etree.fromstring(xml_data, parser=self.checker.cp_parser)
                input = Input(file, xml_data, xml_content)
                self.checker.check_file(input, os.path.join(szv_root, file))
                print(input.verify_result)


async def _test_new():
    for file in files:
        with open(os.path.join(szv_root, file), 'rb') as fd:
            xml_data = fd.read()
            xml_content = etree.fromstring(xml_data, parser=p.utf_parser)
            input = Input(file, xml_data, xml_content)
            await p.check_file(input, os.path.join(szv_root, file))
            pprint(input.verify_result)

if __name__ == '__main__':
    from pprint import pprint
    root = '/home/vasily/PyProjects/FLK/Schematron/tests/pfr_tests'
    xsd_root = os.path.join(root, 'xsd')
    xml_root = os.path.join(root, 'xml')
    p = PfrChecker(root=xsd_root)
    p.db_num = 1
    p.setup_compendium()
    szv_root = os.path.join(xml_root, 'СЗВ')
    files = get_file_list(szv_root)

    start_time = time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_test_new())
    loop.close()
    print('Elapsed time:', time() - start_time)

