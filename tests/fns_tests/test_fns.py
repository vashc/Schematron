import os
from lxml import etree
from src.schematron.fns.tokenizer import Tokenizer
from src.schematron.fns.interpreter import Interpreter
from src.schematron.fns.fns_checker import FnsChecker
from tests.utils import assert_list_equality, get_file_list
from tests.fns_tests.utils import Input  # .

root = os.path.dirname(os.path.abspath(__file__))
xsd_root = os.path.join(root, 'xsd')
xml_root = os.path.join(root, 'xml')


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
        files = get_file_list(xml_root)

        for file in files:
            with open(os.path.join(xml_root, file), 'rb') as fd:
                xml_content = etree.fromstring(fd.read())
                tests.append((file, xml_content))

        # Test matrix traversing
        for test in tests:
            print(self.interpreter.evaluate_expr(['usch:getFileName', '@ИдФайл', '='], test[1], test[0], ''))


class TestFnsChecker:
    checker = FnsChecker()

    def test_fns_checker(self):
        files = get_file_list(xml_root)
        for file in files:
            input = Input().resolve_file(os.path.join(xml_root, file), xsd_root)
            self.checker.check_file(input)
            print(input.verify_result)
