import operator
import os
from time import time
from lxml import etree
from pyparsing import Literal, Suppress, Forward, Word, \
    Group, ZeroOrMore, Optional, oneOf, nums, srange, Combine

_root = os.path.dirname(os.path.abspath(__file__))


class SchematronChecker(object):
    def __init__(self, xsd_root=_root, xml_root=_root):
        self._nullary_map = {
            'usch:getFileName': self._usch_file_name
        }

        self._unary_map = {
            'not':      operator.not_,
            'count':    self._count_func,
            'round':    self._round_func,
            'sum':      self._sum_func,
            'number':   self._number_func
        }

        self._binary_map = {
            '<':                operator.lt,
            '=':                operator.eq,
            '>':                operator.gt,
            '!=':               operator.ne,
            '*':                operator.mul,
            'and':              operator.and_,
            'or':               operator.or_,
            'usch:compareDate': self._usch_compare_date
        }

        self._ternary_map = {
            'substring': self._substring_func,
            'usch:iif': self._usch_iif
        }

        self._stack = []
        self._expr = self._create_tokenizer()
        self._xsd_root = xsd_root
        self._xml_root = xml_root
        self._parser = etree.XMLParser(encoding='cp1251', remove_comments=True)

        self._xml_file = None
        self._xml_content = None
        self._xsd_content = None
        self._context = None

    def _get_asserts(self, content):
        assertions = content.findall('.//xs:appinfo', namespaces=content.nsmap)
        assert_list = []

        for assertion in assertions:
            for pattern in assertion:
                name = pattern.attrib.get('name', None)
                if not name:
                    continue

                for rule in pattern:
                    # Пропуск проверок, родительский элемент
                    # которых не встречается
                    context = rule.attrib['context']
                    par_element = self._xsd_content.xpath(f'//*[@name="{context}"]')
                    min_occurs = par_element[0].xpath('@minOccurs')
                    if min_occurs and min_occurs[0] == '0':
                        continue

                    for sch_assert in rule:
                        assert_list.append({
                            'name':     name,
                            'assert':   sch_assert.attrib['test'],
                            'context':  context
                        })

        return assert_list

    def _push(self, toks):
        self._stack.append(toks[0])
        print('=>', self._stack)

    def _push_not(self, toks):
        for tok in toks:
            if tok == 'not':
                self._stack.append(tok)
        # print('=>', self._stack)

    def _create_tokenizer(self):
        general_comp = oneOf('< > = !=')
        bool_and = Literal('and')
        bool_or = Literal('or')
        bool_not = Literal('not')
        lpar, rpar = map(Suppress, '()')
        tick = Literal("'")
        comma = Suppress(',')

        alphabet = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
        element = (Word(alphabet + alphabet.upper() + nums + '@/:')
                   .setParseAction(self._push))
        integer = Word(nums).setParseAction(self._push)
        string = Word(alphabet + alphabet.upper() + nums + srange('[a-zA-Z]'))
        quoted_string = Combine(tick + string + tick).setParseAction(self._push)

        expr = Forward()
        node = element + ZeroOrMore(('*' + element).setParseAction(self._push))
        parenthesized_node = Group(lpar + node + rpar)
        parenthesized_expr = Group(lpar + expr + rpar)

        count_func = Literal('count') + parenthesized_node
        round_func = Literal('round') + parenthesized_node
        sum_func = Literal('sum') + parenthesized_node
        number_func = Literal('number') + parenthesized_expr
        substring_func = (Literal('substring') +
                          Group(lpar + node + comma + integer +
                                Optional(comma + integer) + rpar))
        usch_filename = Literal('usch:getFileName') + Group(lpar + rpar)
        usch_iif = (Literal('usch:iif') +
                   Group(lpar + expr + comma + expr + comma + expr + rpar))
        usch_compare_date = (Literal('usch:compareDate') +
                             Group(lpar + node + comma + node + rpar))
        funcs = (count_func | round_func | sum_func |
                 number_func | substring_func | usch_filename |
                 usch_iif | usch_compare_date).setParseAction(self._push)

        atom = (funcs | node | (Optional(bool_not) + parenthesized_expr)
                .setParseAction(self._push_not))
        factor = atom + ZeroOrMore((general_comp +
                                    (integer | atom | quoted_string))
                                   .setParseAction(self._push))
        term = factor + ZeroOrMore((bool_and + factor)
                                   .setParseAction(self._push))
        expr <<= term + ZeroOrMore((bool_or + term).setParseAction(self._push))
        return expr

    def _evaluate_node(self, node):
        if '@' in node:
            # Работаем с атрибутом, возвращаем значение
            value = self._xml_content.xpath(f'//{self._context}/{node}')[0]
            return value
        else:
            # Работаем с элементом, возвращаем наличие
            if self._xml_content.xpath(f'//{self._context}/{node}'):
                # Элемент присутствует в файле
                return True
            return False

    def _evaluate_stack(self):
        op = self._stack.pop()

        if op in self._nullary_map:
            return self._nullary_map[op]()
        elif op in self._unary_map:
            if op == 'count':
                arg = self._stack.pop()
            else:
                arg = self._evaluate_stack()
            # print('unary:', op, arg, self._unary_map[op](arg))
            return self._unary_map[op](arg)
        elif op in self._binary_map:
            arg1 = self._evaluate_stack()
            arg2 = self._evaluate_stack()
            # print('binary:', op, arg1, arg2, self._binary_map[op](arg1, arg2))
            return self._binary_map[op](arg1, arg2)
        elif op in self._ternary_map:
            arg3 = self._evaluate_stack()
            arg2 = self._evaluate_stack()
            if op == 'substring' and not arg2.isdigit():
                # Получили substring без опционального третьего аргумента
                return self._ternary_map[op](arg2, arg3)
            arg1 = self._evaluate_stack()
            # print('tern:', self._ternary_map[op](arg1, arg2, arg3))
            return self._ternary_map[op](arg1, arg2, arg3)
        elif op.isdigit():
            # Возвращаем найденное число
            return op
        elif op.startswith('\''):
            # Возвращаем строку без кавычек
            return op[1:-1]
        else:
            # print('node:', op, self._evaluate_node(op))
            return self._evaluate_node(op)

    def _parse(self, assertion):
        print(assertion['assert'])
        self._context = assertion['context']
        self.tokenize(assertion['assert'])
        return self._evaluate_stack()

    # Функции

    def _count_func(self, node):
        if '@' in node:
            return len(self._xml_content.xpath(f'.//{self._context}/*[{node}]'))
        return len(self._xml_content.findall(f'.//{self._context}/{node}'))

    def _round_func(self, node):
        return round(node)

    def _sum_func(self, node):
        return node

    def _number_func(self, node):
        return node

    def _substring_func(self, node, start, length='0'):
        start, length = int(start), int(length)
        return node[start:start + int(length)] if length else node[start:]

    def _usch_file_name(self):
        return ''.join(self._xml_file.split('.')[:-1])

    def _usch_iif(self, cond, true, false):
        return true if cond else false

    def _usch_compare_date(self, first_node, second_node):
        return (first_node == second_node)

    # Публичное API

    def tokenize(self, text):
        self._stack = []
        return self._expr.parseString(text).asList()

    def check_file(self, xml_file):
        start_time = time()
        # xml_filename = os.path.split(xml_file_path)[-1]
        prefix = '_'.join(xml_file.split('_')[:2])

        with open(os.path.join(self._xml_root, xml_file), 'rb') as xml_file_handler:
            xml_content = etree.fromstring(xml_file_handler.read())

        with open(os.path.join(self._xsd_root, f'{prefix}.xsd'),
                  'r', encoding='cp1251') as xsd_file_handler:
            xsd_content = etree.parse(xsd_file_handler, self._parser).getroot()
            xsd_schema = etree.XMLSchema(xsd_content)

        self._xml_content = xml_content
        self._xsd_content = xsd_content
        self._xml_file = xml_file

        asserts = self._get_asserts(xsd_content)

        # for _ in asserts:
        #     print('\n', _)

        if not asserts:
            return '+'

        results = []
        for assertion in asserts:
            results.append({
                'name':     assertion['name'],
                'result':   self._parse(assertion)
            })
            if results[-1]['result']:
                print(assertion['name'], '\u001b[32mOk\u001b[0m')
            else:
                print(assertion['name'], '\u001b[31mFail\u001b[0m')

        if all(result['result'] for result in results):
            print('\u001b[32mTest passed\u001b[0m')
        else:
            print('\u001b[31mTest failed\u001b[0m')

        elapsed_time = time() - start_time
        print('Elapsed time:', elapsed_time)

        return results
