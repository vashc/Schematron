import os
import asyncio
from lxml import etree
from time import time
from src.schemachecker.pfr.pfr_checker import PfrChecker
from tests.utils import get_file_list
from tests.pfr_tests.utils import Input

import BaseXClient
import sys
import re
import io
import string
from abc import ABC, abstractmethod
from typing import Type, Tuple, Union, Dict, List
from pyparsing import *

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


# async def _test_new():
#     for file in files:
#         with open(os.path.join(szv_root, file), 'rb') as fd:
#             xml_data = fd.read()
#             xml_content = etree.fromstring(xml_data, parser=p.cp_parser)
#             input = Input(file, xml_data, xml_content)
#             await p.check_file(input, os.path.join(szv_root, file))
#             pprint(input.verify_result)

########################################################################################################################
# class QueryHandler(ABC):
#     def __init__(self, pattern: str) -> None:
#         self.pattern = re.compile(pattern)
#
#     @abstractmethod
#     def handle(self, input: str) -> None:
#         raise NotImplementedError(f'Not implemented: {sys._getframe().f_code.co_name}')
#
#
# class DefaultNamespaceHandler(QueryHandler):
#     def handle(self, input: str) -> Union[None, Tuple[str, ...]]:
#         match = self.pattern.match(input)
#         return match.groups()
#
# class NamespaceHandler(QueryHandler):
#     def handle(self, input: str) -> Union[None, Tuple[str, ...]]:
#         pass
#
#
# class QueryHandlerFactory:
#     def __init__(self):
#         self.handlers = {}
#
#     def register_handler(self, handler_name: str, handler: QueryHandler) -> None:
#         self.handlers[handler_name] = handler
#
#     def get_handler(self, handler_name: str) -> Type[QueryHandler]:
#         handler = self.handlers.get(handler_name)
#         if handler is None:
#             raise ValueError(handler_name)
#
#         return handler
#
#     def get_handlers(self) -> Dict[str, QueryHandler]:
#         return self.handlers
#
# factory = QueryHandlerFactory()
# default_namespace_pattern = 'declare default element namespace [\'"](.*)[\'"];'
# factory.register_handler('default_namespace', DefaultNamespaceHandler(default_namespace_pattern))
# namespace_pattern = 'declare namespace (.*) = [\'"](.*)[\'"];'


class Query:
    def __init__(self) -> None:
        # Пространство имён по умолчанию
        self.default_namespaces: List[str] = []
        # Дополнительные пространства имён
        self.namespaces: Dict[str, str] = {}
        # Переменные
        self.variables: Dict[str, Tuple[str, str]] = {}
        # Объявленные функции
        self.functions: Dict[str, Tuple[str, str]] = {}
        # Блоки проверок
        self.blocks = []

        self.tokenizer = self._create_tokenizer()

    def _push_default_ns(self, toks: List[List[str]]) -> None:
        """
        Метод заполняет список пространств имён по умолчанию.
        :param toks:
        :return:
        """
        self.default_namespaces.append(toks[0][0])

    def _push_ns(self, toks: List[List[str]]) -> None:
        """
        Метод заполняет словарь пространств имён {"Имя_пространства": "URI"}.
        :param toks:
        :return:
        """
        toks = toks[0]
        self.namespaces.update({toks[0]: toks[1]})

    def _push_var(self, toks: List[List[str]]) -> None:
        """
        Метод заполняет словарь переменных {"Имя_переменной": ("as" | ":=", "Значение_переменной")}.
        :param toks:
        :return:
        """
        toks = toks[0]
        self.variables.update({toks[0]: (toks[1], ' '.join(toks[2:]))})

    def _push_func(self, toks: List[List[str]]) -> None:
        """
        Метод заполняет словарь функций {"Имя_функции": ("Аргументы_и_возвращаемое_значение", "Тело_функции")}.
        toks[0]:
            0: имя функции;
            1: список аргументов и возвращаемый тип;
            2: тело функции;
        :param toks:
        :return:
        """
        toks = toks[0]
        self.functions.update({toks[0]: toks[1:]})

    def _push_block(self, toks: List[List[str]]) -> None:
        """
        Метод заполняет список проверочных блоков. Вставляются в конце результирующего запроса.
        :param toks:
        :return:
        """
        toks = toks[0]
        self.blocks.append(''.join(toks))

    def _create_tokenizer(self) -> Group:
        """
        Метод для токенизации входящего xquery скрипта.
        Использует Suppress для отбрасывания ненужных ключевых слов и информации.
        :return:
        """
        decl_l = Suppress('declare')
        default_ns_l = Suppress('default element namespace')
        ns_l = Suppress('namespace')
        var_l = Suppress('variable')
        func_l = Suppress('function')
        external_l = Literal('external')
        local_l = Suppress('local')
        as_l = Literal('as')
        assign = Literal(':=')
        quote = Suppress("'")
        d_quote = Suppress('"')
        semicolon = Suppress(';')
        colon = Suppress(':')
        equal = Suppress('=')

        any_quote = quote | d_quote

        alphabet = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
        # TODO: избавиться от лишних символов в словах в соответствии со стандартом.
        element = Word(alphabet + alphabet.upper() + alphanums + '$@/:._-()?*')

        var_name = Word(alphanums + '$')
        var_value = Word(alphabet + alphabet.upper() + alphanums + '$@/:._-()[]?*"\'= ')

        func_name = Word(alphanums + '-_')
        func_args = Word(alphanums + '$:()?*,' + string.whitespace)
        func_body = Word(alphabet + alphabet.upper() + ''.join(c for c in string.printable if c != ';'))

        block = Word(alphabet + alphabet.upper() + string.printable + '–№«»≤')

        default_ns_decl = Group(decl_l + default_ns_l + any_quote + element + any_quote + semicolon)\
            .setParseAction(self._push_default_ns)
        ns_decl = Group(decl_l + ns_l + element + equal + any_quote + element + any_quote + semicolon)\
            .setParseAction(self._push_ns)
        var_decl = Group(decl_l + var_l + var_name + (assign | as_l) + var_value + Optional(external_l) + semicolon)\
            .setParseAction(self._push_var)
        func_decl = Group(decl_l + func_l + local_l + colon + func_name + func_args + func_body + semicolon)\
            .setParseAction(self._push_func)
        block_decl = Group(block).setParseAction(self._push_block)
        query = (default_ns_decl + ZeroOrMore(ns_decl) + ZeroOrMore(var_decl) + ZeroOrMore(func_decl) +
                 ZeroOrMore(block_decl))
        return query

    def reset_query(self) -> None:
        """
        Метод очистки внутренних структур.
        Вызывается перед формированием xquery запроса.
        :return:
        """
        self.default_namespaces: List[str] = []
        self.namespaces: Dict[str, str] = {}
        self.variables: Dict[str, Tuple[str, str]] = {}
        self.functions: Dict[str, List[str]] = {}
        self.blocks = []

    def tokenize_query(self, query: str):
        self.tokenizer.parseString(query)

    def makeup_query(self) -> str:
        with io.StringIO() as buffer:
            # Добавляем пространство по умолчанию
            buffer.write(f'declare default element namespace "{self.default_namespaces[0]}";\n')

            # Собираем пространства имён
            for ns_name, ns_descr in self.namespaces.items():
                buffer.write(f'declare namespace {ns_name} = "{ns_descr}";\n')

            # Собираем переменные
            for var_name, var_descr in self.variables.items():
                buffer.write(f'declare variable {var_name} {var_descr[0]} {var_descr[1]};\n')

            # Собираем функции
            for func_name, func_descr in self.functions.items():
                buffer.write(f'declare function local:{func_name}{func_descr[0]}{func_descr[1]};\n')

            # Добавляем блоки (необходима обёртка <БлокПроверок></БлокПроверок>)
            buffer.write('<БлокПроверок>\n')
            for block in self.blocks:
                buffer.write(f'{block}\n')
            buffer.write('</БлокПроверок>')

            query = buffer.getvalue()

        return query


if __name__ == '__main__':
    from pprint import pprint
    t = Query()
    # path = '/home/vasily/PyProjects/FLK/pfr/compendium/АДВ+АДИ+ДСВ 1.17.12д/XQuery'
    # path = '/home/vasily/PyProjects/FLK/pfr/compendium/ЗНП+ЗДП 2.24д/XQuery'
    path = '/home/vasily/PyProjects/FLK/pfr/compendium/СЗВ-М+ИС+УПП АФ.2.32д/XQuery'
    res_path = '/home/vasily/PyProjects/FLK/pfr/xquery_test'
    num = 0  # 0, 8, 13
    for dirpath, dirnames, filenames in os.walk(path):
        t.reset_query()
        if len(filenames) > 0:
            for file in filenames:
                filepath = os.path.join(dirpath, file)
                with open(filepath) as fd:
                    t.tokenize_query(fd.read())
            # with open(os.path.join(res_path, f'{dirpath.split("/")[-1]}.xquery'), 'w') as fd:
            with open(os.path.join(res_path, f'{num}.xquery'), 'w') as fd:
                fd.write(t.makeup_query())
            num += 1

    # query_file = '/home/vasily/PyProjects/FLK/pfr/xquery_test/С_СЗВ-М_2017-01-01.xquery'
    # file = '/home/vasily/PyProjects/FLK/pfr/__/ПФР_087-203-007379_087203_СЗВ-М_20190329_760E3595-47DC-91AA-3A56-F8687FD953E2.XML'
    # session = BaseXClient.Session('localhost', 1984, 'admin', 'admin')
    # query = session.query(query_file)
    # query.bind('$doc', file)
    # print(query.execute())
    # session.close()

    # root = '/home/vasily/PyProjects/FLK/Schematron/tests/pfr_tests'
    # xsd_root = os.path.join(root, 'xsd')
    # xml_root = os.path.join(root, 'xml')
    # p = PfrChecker(root=xsd_root)
    # p.db_num = 1
    # p.setup_compendium()
    # szv_root = os.path.join(xml_root, 'АДВ')
    # files = get_file_list(szv_root)
    #
    # start_time = time()
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(_test_new())
    # loop.close()
    # print('Elapsed time:', time() - start_time)
