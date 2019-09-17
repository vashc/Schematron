import os
from lxml import etree
from glob import glob
from src.schematron.fns.tokenizer import Tokenizer
from src.schematron.fns.interpreter import Interpreter
from src.schematron.fns.fns_checker import FnsChecker
from utils import assert_list_equality, get_file_list, FileResolver  # .

root = os.path.dirname(os.path.abspath(__file__))


class TestFnsTokenizer:
    def test_fns_tokenizer(self):
        tokenizer = Tokenizer()
        token_expr = tokenizer.create_tokenizer()

        tests = [
            ('usch:getFileName() = @ИдФайл', ['usch:getFileName', '@ИдФайл', '=']),
            ('usch:iif(@ПрПодп=2, count(СвПред)!=0, (count(СвПред)!=0 or count(СвПред)=0))',
             ['@ПрПодп', '2', '=', 'СвПред', 'count', '0', '!=', 'СвПред', 'count', '0',
              '!=', 'СвПред', 'count', '0', '=', 'or', 'usch:iif']),
        ]

        for test in tests:
            assert assert_list_equality(tokenizer.tokenize_expression(test[0], token_expr), test[1])


class TestFnsInterpreter:
    interpreter = Interpreter()

    def test_fns_interpreter(self):
        tests = []
        curr_dir = os.curdir

        # Filling in test matrix
        os.chdir(os.path.join(curr_dir, 'tests/fns_tests/xml'))

        for file in glob('*'):
            with open(file, 'rb') as fd:
                xml_content = etree.fromstring(fd.read())
                tests.append((file, xml_content))

        os.chdir(curr_dir)

        # Test matrix traversing
        for test in tests:
            print(self.interpreter.evaluate_expr(['usch:getFileName', '@ИдФайл', '='], test[1], test[0], ''))


class TestFnsChecker:
    xsd_root = os.path.join(root, 'xsd')
    xml_root = os.path.join(root, 'xml')
    checker = FnsChecker()

    def test_fns_checker(self):
        tests = get_file_list(self.xml_root)
        for test in tests:
            input = FileResolver().resolve_file(test, self.xsd_root)
            self.checker.check_file(input)
            print(input.verify_result)
