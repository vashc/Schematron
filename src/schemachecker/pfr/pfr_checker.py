import BaseXClient
import os
import re
import signal
# noinspection PyUnresolvedReferences
from lxml import etree
from urllib.parse import unquote
from struct import pack, unpack
from typing import List, Dict, Tuple, Any, ClassVar
from .utils import Flock, RegisterCleanupFunction
from .xquery import Query
from .exceptions import *


class PfrChecker:
    def __init__(self, *, root: str):
        self.root = root
        # Корневая директория для файлов валидации
        self.xsd_root = os.path.join(root, 'compendium/pfr/compendium/')
        # Список существующих направлений
        self.directions = ['АДВ+АДИ+ДСВ 1.17.12д',
                           'СЗВ-М+ИС+УПП 2.36д',
                           'ЗНП+ЗДП 2.24д']
        # Направление xml файла:
        # 0 - АДВ, 1 - СЗВ, 2 - ЗНП
        self.direction = 0
        # Префикс файла (Атрибут "Код" в компендиуме)
        self.prefix: str = None
        # Справочники для проверок КОРР файлов
        # (нужны для передачи в переменную $dictFile)
        self.dict_file = os.path.join(self.xsd_root,
                                      self.directions[1],
                                      'Справочники/Справочники.xml')
        # Содержимое файла спровчников
        self.dict_file_content = None

        # Название основного валидационного файла
        self.comp_file = 'ПФР_КСАФ.xml'

        # Название файла ПФР
        self.xml_file = None
        # Содержимое файла
        self.content = None
        # Содержимое файла в виде etree Element
        self.xml_content = None
        # Тип документа (например, АНКЕТА_ЗЛ)
        self.doc_type = None

        # Используемые парсеры
        self.cp_parser = etree.XMLParser(encoding='cp1251',
                                         recover=True,
                                         remove_comments=True)
        self.utf_parser = etree.XMLParser(encoding='utf-8',
                                          recover=True,
                                          remove_comments=True)
        # Текущий парсер
        self.parser = self.utf_parser
        # Компендиум проверочных схем и скриптов
        self.compendium = dict()

        self.session = BaseXClient.Session('localhost', 1984, 'admin', 'admin')

        # Корневая директория BaseX
        self.db_data = os.path.join(root, 'basex/data/')
        # Синхронизация воркеров
        self._sync_worker()

        # Сборщик xquery запросов
        self.query = Query()

        # Регистрируем обработчик сигналов
        register_cleanup_function = RegisterCleanupFunction(
            signals=[signal.SIGTERM, signal.SIGINT, signal.SIGQUIT, signal.SIGHUP, signal.SIGUSR2]
        )
        register_cleanup_function(self._finalize)

    @staticmethod
    def _set_error_struct(err_list: List[Tuple[str, str]], file: ClassVar[Dict[str, Any]]) -> None:
        """ Метод заполнения структуры ошибки для вывода. """
        for error in err_list:
            file.verify_result['asserts'].append({
                'error_code': error[0],
                'description': error[1],
                'inspection_items': []
            })

    def _sync_worker(self) -> None:
        """ Метод синхронизации записи в базы данных BaseX для избежания write lock. """
        with Flock(os.path.join(self.db_data, '.sync')) as fd:
            self.db_num = unpack('I', os.read(fd, 4))[0]
            if self.db_num < os.cpu_count():
                self.db_num += 1
                self.db_root = self.db_data + f'xml_db{self.db_num}'
                os.lseek(fd, 0, os.SEEK_SET)
                os.write(fd, pack('I', self.db_num))
            else:
                raise Exception('Too many BaseX workers')

    # Регистрируем обработку сигналов supervisor
    def _finalize(self):
        """ Метод для синхронизации процессов через .sync файл при завершении/рестарте. """
        with Flock(os.path.join(self.db_data, '.sync')) as fd:
            num = unpack('I', os.read(fd, 4))[0]
            if num > 0:
                num -= 1
                os.lseek(fd, 0, os.SEEK_SET)
                os.write(fd, pack('I', num))
            else:
                raise Exception('Incorrect synchronization value')

        if self.session:
            self.session.close()

    def _get_compendium_definitions(self, direction: int) -> Dict[str, str]:
        """ Метод для получения из компендиума словаря {definition: prefix}. """
        definitions = dict()
        for prefix, prefix_dict in self.compendium[self.directions[direction]].items():
            definitions.update({prefix_dict['definition']: prefix})

        return definitions

    def _get_adv_prefix(self) -> str:
        """ Метод для определения префикса файлов АДВ направлений. """
        nsmap = self.xml_content.nsmap
        if nsmap.get(None):
            nsmap['d'] = nsmap.pop(None)

        if not any(nsmap.values()) or 'd' not in nsmap.keys():
            raise WrongNamespace()

        try:
            doc_type = self.xml_content.find('.//d:ТипДокумента', namespaces=nsmap).text
        except AttributeError:
            raise DocTypeNotFound()

        prefix = None

        definitions = self._get_compendium_definitions(self.direction)
        for definition, _prefix in definitions.items():
            if doc_type in definition:
                prefix = _prefix
                break

        return prefix

    def _get_nonadv_prefix(self) -> str:
        """ Метод для определения префикса файлов НЕ АДВ направлений. """
        # Префикс файла (СЗВ-М, СТАЖ и т.д.). None для АДВ направлений
        prefix_list = self.xml_file.split('_')
        prefix = None
        self.direction = 0
        if 'СЗВ' in self.xml_file or 'ОДВ' in self.xml_file:
            prefix = prefix_list[3]
            self.direction = 1
        elif 'УППО' in self.xml_file:
            prefix = prefix_list[1]
            self.direction = 1
        elif 'ЗНП' in self.xml_file or 'ЗДП' in self.xml_file:
            prefix = prefix_list[2]
            self.direction = 2

        return prefix

    def _set_prefix(self) -> None:
        """
        Метод для установки префикса файла для поиска в компендиуме и
        определения направления файла (0 - АДВ, 1 - СЗВ, 2 - ЗНП).
        :return:
        """
        prefix = self._get_nonadv_prefix()
        if prefix is None:
            if self.direction == 0:
                prefix = self._get_adv_prefix()
            if prefix is None:
                raise PrefixNotFound()

        self.prefix = prefix

    def _get_compendium_schemes(self, direction: int, prefix: str) -> Dict[str, Any]:
        """ Метод для получения словаря проверочных схем по направлению и префиксу файла. """
        try:
            return self.compendium[self.directions[direction]].get(prefix).get('schemes')
        except AttributeError:
            raise SchemesNotFound(prefix)

    def _get_compendium_queries(self, direction: int, prefix: str) -> Dict[str, Any]:
        """ Метод для получения словаря проверочных xquery скриптов по направлению и префиксу файла. """
        try:
            return self.compendium[self.directions[direction]].get(prefix).get('queries')
        except AttributeError:
            raise QueriesNotFound(prefix)

    def _validate_xsd(self, file: ClassVar[Dict[str, Any]]) -> bool:
        """ Метод для валидации файла по XSD. """
        success = True
        schemes = self._get_compendium_schemes(self.direction, self.prefix)
        if schemes is None:
            raise SchemesNotFound(self.prefix)

        ret_list = []
        for scheme in schemes.values():
            try:
                scheme.assertValid(self.xml_content)
            except etree.DocumentInvalid:
                for error in scheme.error_log:
                    ret_list.append((str(error.line), error.message))
                    self._set_error_struct(ret_list, file)

                file.verify_result['result'] = 'failed_xsd'
                file.verify_result['description'] = (
                    f'Ошибка при валидации по xsd схеме файла '
                    f'{self.xml_file}.')
                success = False

        return success

    def _execute_query(self, query_file: str, binds: Dict[str, str]) -> str:
        query = self.session.query(query_file)

        for key, value in binds.items():
            query.bind(key, value)

        return query.execute()

    def _checkup_adv(self, checkups, q_nsmap, input):
        """ Метод для получения результатов проверки (ошибок) для АДВ направлений """
        for checkup in checkups:
            code_presence = checkup.find('./d:КодРезультата', namespaces=q_nsmap)
            if len(code_presence):
                code = code_presence.text
            else:
                code = '50'
            prot_code = self.doc_type or ''
            description = checkup.find('./d:Описание', namespaces=q_nsmap).text or ''
            results = checkup.findall('.//d:Результат', namespaces=q_nsmap)
            element_objs = []
            for result in results:
                element_path = result.text or ''
                element_objs.append({'element_path': element_path,
                                     'expected_value': '',
                                     'name': '',
                                     'value': ''})
            input.verify_result['asserts'].append({
                'pfr_code': code,
                'error_code': prot_code,
                'description': description,
                'inspection_items': element_objs
            })

    @staticmethod
    def _checkup_nonadv(checkups: etree.ElementTree,
                        q_nsmap: Dict[str, str],
                        block_code: str,
                        file: ClassVar[Dict[str, Any]]) -> None:
        """ Метод для получения результатов проверки (ошибок) для НЕ АДВ направлений """
        for checkup in checkups:
            check_code = checkup.attrib['ID']
            prot_code = '.'.join((block_code, check_code))
            code = checkup.find('./d:КодРезультата', namespaces=q_nsmap).text or '50'
            description = checkup.find('./d:Описание', namespaces=q_nsmap).text or ''
            results = checkup.findall('.//d:Результат', namespaces=q_nsmap)
            element_objs = []
            for result in results:
                element_path = result.find('./d:ПутьДоЭлемента',
                                           namespaces=q_nsmap).text
                expected_value = result.find('./d:ОжидаемоеЗначение',
                                             namespaces=q_nsmap).text
                element_name = result.find('./d:Объект/d:Наименование',
                                           namespaces=q_nsmap).text
                element_value = result.find('./d:Объект/d:Значение',
                                            namespaces=q_nsmap).text
                element_objs.append({'element_path': element_path,
                                     'expected_value': expected_value or '',
                                     'name': element_name or '',
                                     'value': element_value or ''})

            file.verify_result['asserts'].append({
                'pfr_code': code,
                'error_code': prot_code,
                'description': description,
                'inspection_items': element_objs
            })

    def _validate_xquery(self, file: ClassVar[Dict[str, Any]]) -> None:
        """ Метод для валидации файла по xquery выражениям. """
        binds = {'$doc': f'{self.content}'}
        if self.direction == 1:
            binds.update({'$dictFile': f'{self.dict_file}'})

        queries = self._get_compendium_queries(self.direction, self.prefix)
        if queries is None:
            raise QueriesNotFound(self.prefix)

        for query in queries.values():
            query_result = self._execute_query(query, binds)

            if query_result:
                check_result = etree.fromstring(query_result, parser=self.parser)
                q_nsmap = check_result.nsmap
                q_nsmap['d'] = q_nsmap.pop(None)
                # Запрос возвращает ответ в xml формате, проверяем,
                # вернулась ли ошибка (Результат != 0)
                # blocks = check_result.xpath('//БлокПроверок')
                # for block in blocks:
                block_code = check_result.attrib['ID']
                checkups = check_result.xpath(
                    '//d:Проверка[d:РезультатЗапроса/d:Результат[text()!=0]]',
                    namespaces=q_nsmap
                )
                if self.direction:
                    self._checkup_nonadv(checkups, q_nsmap, block_code, file)
                else:
                    self._checkup_adv(checkups, q_nsmap, file)
                # Обнаружили ошибки
                if checkups:
                    file.verify_result['result'] = 'failed_xqr'
                    file.verify_result['description'] = (
                        f'Ошибка при валидации по xquery выражению файла '
                        f'{self.xml_file}.')
            else:
                raise QueryResultError()

    def _get_comp_file(self, direction: str) -> Tuple[etree.ElementTree, Dict[str, Any]]:
        """ Метод для получения дерева файла компендиума и простанства имён для указанного направления. """
        with open(os.path.join(self.xsd_root, direction, self.comp_file), 'rb') as handler:
            comp_file = etree.fromstring(handler.read(), parser=self.utf_parser)

        nsmap = comp_file.nsmap
        if nsmap.get(None):
            nsmap['d'] = nsmap.pop(None)

        return comp_file, nsmap

    @staticmethod
    def _get_doc_types(comp_file: etree.ElementTree, nsmap: Dict[str, Any]) -> List[etree.ElementTree]:
        """ Метод для получения списка всех действующих проверочных документов. """
        # Нужен действующий формат со статусом "ПоУмолчанию"
        doc_types_xpath = f'//d:ТипДокумента[d:Форматы/d:Формат[@Статус="Действующий" and @ПоУмолчанию="true"]]'
        doc_types = comp_file.xpath(doc_types_xpath, namespaces=nsmap)

        return doc_types

    @staticmethod
    def _get_definition(doc_type: etree.ElementTree, nsmap: Dict[str, Any]) -> str:
        """ Метод для получения содержимого ноды "ОпределениеДокумента". """
        try:
            return doc_type.xpath(f'.//d:Валидация/d:ОпределениеДокумента/text()', namespaces=nsmap)[0]
        # У некоторых направлений нет определения документа, возвращаем пустую строку
        except IndexError:
            return ''

    def _get_schemes(self, doc_type: etree.ElementTree,
                     direction: str,
                     nsmap: Dict[str, Any]) -> Dict[str, etree.ElementTree]:
        """
        Метод для получения словаря проверочных XSD схем:
            {
                'name.xsd': etree.ElementTree,
                ...
            }
        :return:
        """
        schemes_dict = dict()
        schemes = doc_type.xpath('.//d:Валидация/d:Схема/text()', namespaces=nsmap)
        for scheme in schemes:
            scheme = unquote(scheme).replace('\\', '/')
            with open(os.path.join(self.xsd_root, direction, scheme.lstrip('/')), 'rb') as xsd_handler:
                parser = self.utf_parser
                try:
                    xsd_content = etree.parse(xsd_handler, parser).getroot()
                except etree.XMLSyntaxError:
                    parser = self.cp_parser
                    xsd_handler.seek(0, 0)
                    try:
                        xsd_content = etree.parse(xsd_handler, parser).getroot()
                    except etree.XMLSyntaxError as ex:
                        raise Exception(f'Ошибка при разборе XSD схемы: {ex}')
                xsd_scheme = etree.XMLSchema(xsd_content)

            schemes_dict.update({scheme: xsd_scheme})

        return schemes_dict

    def _get_scenario(self, direction: str, scenario_file: str) -> Tuple[etree.ElementTree, Dict[str, Any]]:
        """ Метод для получения содержимого сценария и пространства имён. """
        with open(os.path.join(self.xsd_root, direction, scenario_file), 'rb') as handler:
            try:
                scenario = etree.fromstring(handler.read(), parser=self.utf_parser)
            except etree.XMLSyntaxError as ex:
                raise Exception(f'Ошибка при разборе файла сценария: {ex}')

        s_nsmap = scenario.nsmap
        if s_nsmap.get(None):
            s_nsmap['d'] = s_nsmap.pop(None)

        return scenario, s_nsmap

    def _get_query_file(self, validator: etree.ElementTree,
                        direction: str,
                        s_nsmap: Dict[str, Any],
                        scenario_dir: str) -> Tuple[str, str]:
        """ Метод для получения имени и пути xquery скрипта по валидатору - протоколируемой проверке. """
        validator_file = validator.xpath('./d:Файл/text()', namespaces=s_nsmap)[0]
        # Используем не .xml файл для проверки, а сразу сырой .xquery
        validator_file = validator_file.split('\\')[-1].split('.')[0] + '.xquery'
        query_file = os.path.join(self.xsd_root, direction, f'XQuery/{scenario_dir}', validator_file)

        return query_file, validator_file

    def _makeup_queries(self, queries: Dict[str, str]) -> str:
        """ Метод собирает единый запрос для переданного словаря xquery выражений. """
        self.query.reset_query()
        # TODO: добавить общую обработку ошибок при сборке компендиума
        for query in queries.values():
            try:
                self.query.tokenize_query(query)
            except Exception as ex:
                raise

        return self.query.makeup_query()

    def _get_query_validators(self, doc_type: etree.ElementTree,
                              direction: str,
                              nsmap: Dict[str, Any]) -> Dict[str, Any]:
        """
        Метод для получения словаря проверочных xquery скриптов:
            {
                'name.xquery': str,
                ...
            }
        :param doc_type:
        :param direction:
        :param nsmap:
        :return:
        """
        queries_dict = dict()
        scenario_file = doc_type.xpath('.//d:Сценарий/text()', namespaces=nsmap)
        # Сценарий проверки не всегда присутствует
        if scenario_file:
            scenario_file = scenario_file[0]
            # Замена слэшей в пути
            scenario_dir = scenario_file.split('\\')[-1].split('.')[0]
            scenario_file = scenario_file[1:].replace('\\', '/')

            # Получение содержимого сценария
            scenario, s_nsmap = self._get_scenario(direction, scenario_file)

            # Получение всех протоколируемых проверок
            validators = scenario.xpath('//d:Проверки/d:Проверка[not(@Протоколируемая="0")]',
                                        namespaces=s_nsmap)
            for validator in validators:
                query_file, validator_file = self._get_query_file(validator, direction, s_nsmap, scenario_dir)
                with open(query_file, 'r', encoding="utf-8") as q_handler:
                    queries_dict.update({validator_file: q_handler.read()})

            # queries_dict = {'query.xquery': self._makeup_queries(queries_dict)}

        return queries_dict

    def _set_dict_file_content(self) -> None:
        """ Устанавливает содержимое файла справочников. """
        with open(self.dict_file) as fd:
            self.dict_file_content = fd.read()

    def setup_compendium(self) -> None:
        """
        Сборка компендиума в памяти. Для каждого из трёх направлений:
            - Получение etree.ElementTree для файла компендиума ПФР_КСАФ.xml и пространства имён;
            - Получение списка всех деуствующих проверочных документов (@Статус="Действующий" и @ПоУмолчанию="true")
            - Для каждого префикса в направлении:
                - Получение словаря проверочных XSD схем;
                - Получение словаря проверочных xquery скриптов;
                - Получение содержимого ноды "ОпределениеДокумента" для АДВ направлений;

        Компендиум проверочных схем и скриптов имеет следующую структуру:
        {
            "АДВ+АДИ+ДСВ 1.17.12д": {  # Направление
                "СЗВ-М": {  # Префикс проверяемого файла
                    'schemes': {  # Словарь проверочных XSD схем
                        "name.xsd": etree.ElementTree,
                        ...
                    },
                    'queries': {  # Словарь содержимого xquery скриптов
                        "name.xquery": str,
                        ...
                    },
                    'definition': str  # Определение документа, нода "ОпределениеДокумента" в ПФР_КСАФ
                }
            }
        }
        """
        # TODO: отлов исключений?
        self.compendium = dict()

        self._set_dict_file_content()

        for direction in self.directions:
            comp_file, nsmap = self._get_comp_file(direction)

            doc_types = self._get_doc_types(comp_file, nsmap)

            prefix_dict = dict()

            for doc_type in doc_types:
                prefix = doc_type.get('Код')
                if prefix is None:
                    raise Exception('Не найден код для типа документа')

                prefix_dict.update({prefix: dict()})

                schemes_dict = self._get_schemes(doc_type, direction, nsmap)
                prefix_dict[prefix].update({'schemes': schemes_dict})

                # Получение протоколируемых проверок в сценарии
                queries_dict = self._get_query_validators(doc_type, direction, nsmap)
                prefix_dict[prefix].update({'queries': queries_dict})

                definition = self._get_definition(doc_type, nsmap)
                prefix_dict[prefix].update({'definition': definition})

            self.compendium.update({direction: prefix_dict})

    def check_file(self, file: ClassVar[Dict[str, Any]]) -> None:
        self.xml_file = file.filename
        # Серверная сторона BaseX некорректно работает с кодировкой cp1251
        if file.charset == 'cp1251':
            self.content = file.content.encode().decode('utf-8')
            self.content = re.sub('encoding="windows-1251"',
                                  'encoding="utf-8"',
                                  self.content,
                                  flags=re.IGNORECASE)
        else:
            self.content = file.content
        self.xml_content = file.xml_tree

        file.verify_result = dict()

        file.verify_result['result'] = 'passed'
        file.verify_result['asserts'] = []

        # Открытие сессии BaseX
        self.session.execute(f'open xml_db{self.db_num}')

        self._set_prefix()

        # Проверка по XSD
        if not self._validate_xsd(file):
            return

        # Проверка по xquery выражениям
        self._validate_xquery(file)
