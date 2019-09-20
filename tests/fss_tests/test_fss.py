import os
from src.schemachecker.fss.fss_checker import FssChecker
from tests.utils import get_file_list
from tests.fss_tests.utils import Input

root = os.path.dirname(os.path.abspath(__file__))
xml_root = os.path.join(root, 'xml')
xsd_root = os.path.join(root, 'xsd')


class TestFssChecker:
    checker = FssChecker()

    def test_fss_checker(self):
        files = get_file_list(xml_root)

        for file in files:
            with open(os.path.join(xml_root, file), 'rb') as fd:
                input = Input(file, fd.read()).resolve_file(file, xsd_root)
                self.checker.check_file(input)
                print(input.verify_result)

