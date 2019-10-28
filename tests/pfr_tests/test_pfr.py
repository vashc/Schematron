import os
import asyncio
from lxml import etree
from time import time
from src.schemachecker.pfr.pfr_checker import PfrChecker
from src.schemachecker.pfr.xquery import Query, ExternalVar
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


# if __name__ == '__main__':
#     from pprint import pprint
#     t = Query()
#     # path = '/home/vasily/PyProjects/FLK/pfr/compendium/АДВ+АДИ+ДСВ 1.17.12д/XQuery'
#     # path = '/home/vasily/PyProjects/FLK/pfr/compendium/ЗНП+ЗДП 2.24д/XQuery'
#     path = '/home/vasily/PyProjects/FLK/pfr/compendium/СЗВ-М+ИС+УПП АФ.2.32д/XQuery'
#     res_path = '/home/vasily/PyProjects/FLK/pfr/xquery_test'
#     num = 0  # 0, 8, 13
#     for dirpath, dirnames, filenames in os.walk(path):
#         t.reset_query()
#         if len(filenames) > 0:
#             for file in filenames:
#                 filepath = os.path.join(dirpath, file)
#                 with open(filepath) as fd:
#                     t.tokenize_query(fd.read())
#             # with open(os.path.join(res_path, f'{dirpath.split("/")[-1]}.xquery'), 'w') as fd:
#             with open(os.path.join(res_path, f'{num}.xquery'), 'w') as fd:
#                 fd.write(t.makeup_query())
#             num += 1

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

if __name__ == '__main__':
    v = ExternalVar(xq_file='1.xquery',
                    xml_file='ПФР_099-099-123456_099099_СЗВ-М_20190408_C159C865-FCB0-41AC-BC8E-8459202224A9.xml')
    print(v.execute_xq())
