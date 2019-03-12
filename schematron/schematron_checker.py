import operator
import os
import asyncio
from functools import wraps
from database import db
from time import time
from lxml import etree
from pyparsing import Literal, Suppress, Forward, Word, \
    Group, ZeroOrMore, Optional, oneOf, nums, srange, Combine

_root = os.path.dirname(os.path.abspath(__file__))


class SchematronChecker(object):
    def __init__(self, *, xsd_root=_root, xml_root=_root):
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
            '<=':               operator.le,
            '=':                operator.eq,
            '>':                operator.gt,
            '>=':               operator.ge,
            '!=':               operator.ne,
            '*':                self._op_mul,
            '-':                self._op_sub,
            '+':                self._op_add,
            'and':              operator.and_,
            'or':               operator.or_,
            'usch:compareDate': self._usch_compare_date
        }

        self._ternary_map = {
            'substring':    self._substring_func,
            'usch:iif':     self._usch_iif
        }

        self._varargs_map = {
            'concat':   self._concat_func
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
        self._cache = dict()

    def _cached_node(func):
        # Кэшируется только один элемент - узел
        @wraps(func)
        def _inner(*args, **kwargs):
            self = args[0]
            element = f'//{self._context}/{args[1]}'
            if element not in self._cache:
                self._cache[element] = func(*args, **kwargs)
            return self._cache[element]
        return _inner

    def _cached_func(func):
        @wraps(func)
        def _inner(*args, **kwargs):
            self = args[0]
            # Нужен контекст, могут быть идентичные сигнатуры
            # в разных контекстах
            element = f'{func.__name__}/{self._context}/{args[1:]}'
            if element not in self._cache:
                self._cache[element] = func(*args, **kwargs)
            return self._cache[element]
        return _inner

    def _return_error(self, text):
        return f'\u001b[31mError. {text}.\u001b[0m'

    async def _get_xsd_scheme(self, xml_info):
        query = '''SELECT xsd
                FROM documents
                WHERE knd = $1
                AND version = $2'''

        return await db.fetchval(query, xml_info['knd'], xml_info['version'])

    def _get_xml_info(self, xml_content):
        xml_info = {}
        document_node = xml_content.find('Документ')
        if document_node is not None:
            xml_info['knd'] = document_node.get('КНД')
            xml_info['version'] = xml_content.attrib.get('ВерсФорм')
        else:
            # Ошибка, элемент "Документ" не найден
            raise Exception(f'Элемент "Документ" в файле '
                            f'{self._xml_file} не найден')

        return xml_info

    def _get_error(self, node):
        error = {}
        replacing = []
        error['code'] = node.get('code')

        text = ''
        if node.text:
            text = node.text

        for child in node:
            select = child.get('select', None)
            replacing.append(select)
            text += select

            if child.tail is not None:
                # Конец текста ошибки перед закрывающим тегом
                text += child.tail

        # Убираем спецсимволы
        text = ' '.join((text.replace('\n', '').replace('\t', '')).split())
        error['text'] = text
        error['replacing'] = replacing

        return error

    def _get_error_text(self, assertion):
        error = assertion['error']
        error_text = error['text']
        for replacement in error['replacing']:
            error_text = error_text.replace(
                replacement, str(self._parse(replacement, assertion['context'])))
        return error_text

    def _get_asserts(self, content):
        assertions = content.findall('.//xs:appinfo', namespaces=content.nsmap)
        assert_list = []

        for assertion in assertions:
            for pattern in assertion:
                name = pattern.attrib.get('name', None)
                if not name:
                    continue

                for rule in pattern:
                    context = rule.attrib['context']

                    # Пропуск проверок, родительский элемент
                    # которых может не встречаться, minOccurs=0
                    occurs_elements = assertion.xpath(
                        f'ancestor::*[@minOccurs=0]')
                    if len(occurs_elements):
                        continue

                    # Проверка, присутствует ли контекст в xml файле
                    if len(self._xml_content.xpath(f'//{context}')) == 0:
                        # Не найден контекст в xml файле

                        # Пропуск опциональных проверок, choice
                        choice_elements = assertion.xpath(f'ancestor::xs:choice',
                                                          namespaces=content.nsmap)
                        if len(choice_elements):
                            # Опциональная проверка, пропускаем
                            continue
                        # Ошибка, проверка обязательна, контекст не найден
                        raise Exception(f'Контекст {context} в файле '
                                        f'{self._xml_file} не найден')

                    for sch_assert in rule:
                        for error_node in sch_assert:
                            error = self._get_error(error_node)

                            assert_list.append({
                                'name':     name,
                                'assert':   sch_assert.attrib['test'],
                                'context':  context,
                                'error': error
                            })

        return assert_list

    def _push(self, toks):
        self._stack.append(toks[0])
        # print('=>', self._stack)

    def _push_not(self, toks):
        for tok in toks:
            if tok == 'not':
                self._stack.append(tok)
        # print('=>', self._stack)

    def _create_tokenizer(self):
        general_comp = oneOf('< > = != <= >=')
        bool_and = Literal('and')
        bool_or = Literal('or')
        bool_not = Literal('not')
        lpar, rpar = map(Suppress, '()')
        tick = Literal("'")
        minus = Literal('-')
        plus = Literal('+')
        mul = Literal('*')
        comma = Suppress(',')

        alphabet = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
        element = (Word(alphabet + alphabet.upper() + nums + '@/:._-')
                   .setParseAction(self._push))
        integer = Word(nums).setParseAction(self._push)
        string = Word(alphabet + alphabet.upper() +
                      nums + srange('[a-zA-Z]' + '.'))
        quoted_string = Combine(tick + string + tick).setParseAction(self._push)
        date = Combine(tick + Word(nums, exact=2) + '.' +
                       Word(nums, exact=2) + '.' + Word(nums, exact=4) +
                       tick).setParseAction(self._push)

        expr = Forward()
        node = element + ZeroOrMore(((mul | minus) + element)
                                    .setParseAction(self._push))
        parenthesized_node = Group(lpar + node + rpar)
        node_or_parnode = node | parenthesized_node
        # Переменное (не менее двух) количество узлов, разделённых запятыми
        variadic_node = Group(node_or_parnode + comma + node_or_parnode +
                              ZeroOrMore(comma + node_or_parnode))
        parenthesized_expr = Group(lpar + expr + rpar)

        count_func = Literal('count') + parenthesized_node
        round_func = Literal('round') + parenthesized_node
        sum_func = Literal('sum') + parenthesized_node
        number_func = Literal('number') + parenthesized_expr
        substring_func = (Literal('substring') +
                          Group(lpar + node + comma + integer +
                                Optional(comma + integer) + rpar))
        concat_func = (Literal('concat') + Group(lpar + variadic_node + rpar))
        usch_filename = Literal('usch:getFileName') + Group(lpar + rpar)
        usch_iif = (Literal('usch:iif') +
                   Group(lpar + expr + comma + expr + comma + expr + rpar))
        usch_compare_date = (Literal('usch:compareDate') +
                             Group(lpar + node + comma + node + rpar))
        funcs = (count_func | round_func | sum_func | number_func |
                 substring_func | concat_func | usch_filename |
                 usch_iif | usch_compare_date).setParseAction(self._push)

        atom = (funcs | node | (Optional(bool_not) + parenthesized_expr)
                .setParseAction(self._push_not))
        left_expr = atom + ZeroOrMore(((mul | minus | plus) + atom)
                                      .setParseAction(self._push))

        factor = (left_expr +
                  ZeroOrMore((general_comp +
                              (integer | atom | quoted_string | date))
                             .setParseAction(self._push)))
        term = factor + ZeroOrMore((bool_and + factor)
                                   .setParseAction(self._push))
        expr <<= term + ZeroOrMore((bool_or + term).setParseAction(self._push))
        return expr

    @_cached_node
    def _evaluate_node(self, node):
        element = f'//{self._context}/{node}'
        value = self._xml_content.xpath(element)
        if not value:
            # Элемент/атрибут не найден
            raise Exception(f'Элемент/атрибут {self._context}/{node} в '
                            f'файле {self._xml_file} не найден')
        if '@' in node:
            # Работаем с атрибутом, возвращаем значение
            value = value[0]
            return value
        else:
            # Работаем с элементом, возвращаем наличие
            return True if value else False

    def _evaluate_stack(self):
        op = self._stack.pop()

        if op in self._nullary_map:
            return self._nullary_map[op]()
        elif op in self._unary_map:
            if op == 'count':
                arg = self._stack.pop()
            else:
                arg = self._evaluate_stack()
            return self._unary_map[op](arg)
        elif op in self._binary_map:
            arg1 = self._evaluate_stack()
            arg2 = self._evaluate_stack()
            return self._binary_map[op](arg2, arg1)
        elif op in self._ternary_map:
            arg3 = self._evaluate_stack()
            arg2 = self._evaluate_stack()
            if op == 'substring' and not arg2.isdigit():
                # Получили substring без опционального третьего аргумента
                return self._ternary_map[op](arg2, arg3)
            arg1 = self._evaluate_stack()
            return self._ternary_map[op](arg1, arg2, arg3)
        elif op in self._varargs_map:
            pass
        elif op.isdigit():
            # Возвращаем найденное число
            return op
        elif op.startswith('\''):
            # Возвращаем строку без кавычек
            return op[1:-1]
        else:
            node = self._evaluate_node(op)
            return node

    def _parse(self, expression, context):
        print(expression)
        self._context = context
        self.tokenize(expression)
        try:
            parsing_result = self._evaluate_stack()
            return parsing_result
        except Exception as ex:
            print(f'\u001b[35mError. {ex}.\u001b[0m')

    # Функции

    # Обёртки для некоторых арифметических операций
    def _op_mul(self, a, b):
        try:
            val = float(a) * float(b)
            return val
        #TODO: Ошибка приведения к типу
        except Exception as ex:
            raise Exception(f'Ошибка при приведении к вещественному типу: {ex}')

    def _op_sub(self, a, b):
        try:
            val= float(a) - float(b)
            return val
        # TODO: Ошибка приведения к типу
        except Exception as ex:
            raise Exception(f'Ошибка при приведении к вещественному типу: {ex}')

    def _op_add(self, a, b):
        try:
            val = float(a) + float(b)
            return val
        # TODO: Ошибка приведения к типу
        except Exception as ex:
            raise Exception(f'Ошибка при приведении к вещественному типу: {ex}')

    @_cached_func
    def _count_func(self, node):
        # if '@' in node:
            # return str(len(self._xml_content.xpath(f'.//{self._context}/*[{node}]')))
        # return str(len(self._xml_content.findall(f'//{self._context}/{node}')))
        return str(len(self._xml_content.xpath(f'//{self._context}/{node}')))

    @_cached_func
    def _round_func(self, node):
        return round(node)

    @_cached_func
    def _sum_func(self, node):
        return node

    @_cached_func
    def _number_func(self, node):
        return node

    @_cached_func
    def _substring_func(self, node, start, length='0'):
        start, length = int(start) - 1, int(length)
        return node[start:start + int(length)] if length else node[start:]

    def _concat_func(self, *args):
        return ''.join(args)

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

    async def check_file(self, xml_file):
        # Очищаем кэш
        self._cache = dict()
        start_time = time()
        prefix = '_'.join(xml_file.split('_')[:2])

        with open(os.path.join(self._xml_root, xml_file), 'rb') as xml_file_handler:
            xml_content = etree.fromstring(xml_file_handler.read())

        try:
            xml_info = self._get_xml_info(xml_content)
        except Exception as ex:
            # Ошибка при получении информации (КНД, версия и т.д.)
            print(self._return_error(ex))
            test_result = 'failed'
            return test_result

        try:
            xsd_file = await self._get_xsd_scheme(xml_info)
        except Exception as ex:
            # Ошибка при получении имени xsd схемы из БД
            print(self._return_error(f'Ошибка при получении имени xsd схемы из'
                                     f' БД при проверке файла {xml_file}.\u001b[0m'))
            test_result = 'failed'
            return test_result
        if not xsd_file:
            # На найдена xsd схема для проверки
            print(self._return_error(f'Не найдена xsd схема для проверки '
                                     f'файла {xml_file}.'))
            test_result = 'failed'
            return test_result
        print('XSD FILE:', xsd_file)

        with open(os.path.join(self._xsd_root, xsd_file),
                  'r', encoding='cp1251') as xsd_file_handler:
            xsd_content = etree.parse(xsd_file_handler, self._parser).getroot()
            xsd_schema = etree.XMLSchema(xsd_content)

        self._xml_content = xml_content
        self._xsd_content = xsd_content
        self._xml_file = xml_file

        try:
            asserts = self._get_asserts(xsd_content)
        except Exception as ex:
            print(self._return_error(ex))
            test_result = 'failed'
            return test_result

        if not asserts:
            test_result = 'passed'
            return test_result

        results = []
        for assertion in asserts:
            results.append({
                'name':     assertion['name'],
                'result':   self._parse(assertion['assert'],
                                        assertion['context'])
            })
            if results[-1]['result']:
                print(assertion['name'], ': \u001b[32mOk\u001b[0m')
            else:
                print(assertion['name'], ': \u001b[31mError\u001b[0m', end='. ')
                print(f'\u001b[31m{self._get_error_text(assertion)}\u001b[0m')

        if all(result['result'] for result in results):
            print('\u001b[32mTest passed\u001b[0m')
            test_result = 'passed'
        else:
            print('\u001b[31mTest failed\u001b[0m')
            test_result = 'failed'

        elapsed_time = time() - start_time
        print(f'Elapsed time: {round(elapsed_time, 4)} s')
        print('Cache:', self._cache)

        return test_result
