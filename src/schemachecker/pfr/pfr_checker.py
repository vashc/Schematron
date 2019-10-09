import os
import BaseXClient
from typing import List, Dict, Tuple, Any, ClassVar
from lxml import etree
from urllib.parse import unquote
from atexit import register
from struct import pack, unpack
from .utils import Flock  # .
from .xquery import Query
from .exceptions import *


class PfrChecker:
    def __init__(self, *, root):
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
        # Синхронизация записи в базы данных BaseX для избежания write lock
        with Flock(os.path.join(self.db_data, '.sync')) as fd:
            self.db_num = unpack('I', os.read(fd, 4))[0]
            if self.db_num < os.cpu_count():
                self.db_num += 1
                self.db_root = self.db_data + f'xml_db{self.db_num}'
                os.lseek(fd, 0, os.SEEK_SET)
                os.write(fd, pack('I', self.db_num))
            else:
                raise Exception('Too many BaseX workers')

        # Сборщик xquery запросов
        self.query = Query()

        # Регистрируем метод финализации
        register(self._finalize)

    def _finalize(self):
        """
        Метод для синхронизации процессов через .sync файл
        при завершении/рестарте
        """
        with Flock(os.path.join(self.db_data, '.sync')) as fd:
            num = unpack('I', os.read(fd, 4))[0]
            if num > 0:
                num -= 1
                os.lseek(fd, 0, os.SEEK_SET)
                os.write(fd, pack('I', num))
            else:
                raise Exception('Incorrect synchronisation value')

        if self.session:
            self.session.close()

    async def _get_compendium_definitions(self, direction: int) -> Dict[str, str]:
        """
        Метод для получения из компендиума словаря {definition: prefix}.
        :param direction:
        :return:
        """
        definitions = dict()
        for prefix, prefix_dict in self.compendium[self.directions[direction]].items():
            definitions.update({prefix_dict['definition']: prefix})

        return definitions

    async def _get_adv_prefix(self) -> str:
        """
        Метод для определения префикса файлов АДВ направлений.
        """
        nsmap = self.xml_content.nsmap
        if nsmap.get(None):
            nsmap['d'] = nsmap.pop(None)

        try:
            doc_type = self.xml_content.find('.//d:ТипДокумента', namespaces=nsmap).text
        except AttributeError:
            raise DocTypeNotFound()

        prefix = None

        definitions = await self._get_compendium_definitions(self.direction)
        for definition, _prefix in definitions.items():
            if doc_type in definition:
                prefix = _prefix
                break

        return prefix

    async def _get_nonadv_prefix(self) -> str:
        """
        Метод для определения префикса файлов НЕ АДВ направлений.
        :return:
        """
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

    async def _set_prefix(self) -> None:
        """
        Метод для установки префикса файла для поиска в компендиуме и
        определения направления файла (0 - АДВ, 1 - СЗВ, 2 - ЗНП).
        :return:
        """
        prefix = await self._get_nonadv_prefix()
        if prefix is None:
            if self.direction == 0:
                prefix = await self._get_adv_prefix()
            if prefix is None:
                raise PrefixNotFound()

        self.prefix = prefix

    async def _get_compendium_schemes(self, direction: int, prefix: str) -> Dict[str, Any]:
        """
        Метод для получения словаря проверочных схем по направлению и префиксу файла.
        :param direction:
        :param prefix:
        :return:
        """
        try:
            return self.compendium[self.directions[direction]].get(prefix).get('schemes')
        except AttributeError:
            raise SchemesNotFound(prefix)

    async def _get_compendium_queries(self, direction: int, prefix: str) -> Dict[str, Any]:
        """
        Метод для получения словаря проверочных xquery скриптов по направлению и префиксу файла.
        :param direction:
        :param prefix:
        :return:
        """
        try:
            return self.compendium[self.directions[direction]].get(prefix).get('queries')
        except AttributeError:
            raise QueriesNotFound(prefix)

    async def _validate_xsd(self, input: ClassVar[Dict[str, Any]]) -> bool:
        """
        Метод для валидации файла по XSD.
        :return:
        """
        success = True
        schemes = await self._get_compendium_schemes(self.direction, self.prefix)
        if schemes is None:
            raise SchemesNotFound(self.prefix)

        for scheme in schemes.values():
            try:
                scheme.assertValid(self.xml_content)
            except etree.DocumentInvalid:
                for error in scheme.error_log:
                    input.verify_result['xsd_asserts'].append(f'{error.message} (строка {error.line})')

                input.verify_result['result'] = 'failed_xsd'
                input.verify_result['description'] = (
                    f'Ошибка при валидации по xsd схеме файла '
                    f'{self.xml_file}.')
                success = False

        return success

    async def _execute_query(self, query_file: str, binds: Dict[str, str]) -> str:
        query = self.session.query(query_file)

        for key, value in binds.items():
            query.bind(key, value)

        return query.execute()

    async def _checkup_adv(self, checkups, q_nsmap, input):
        """
        Метод для получения результатов проверки (ошибок) для АДВ направлений
        """
        for checkup in checkups:
            code_presence = checkup.find('./d:КодРезультата', namespaces=q_nsmap)
            if len(code_presence):
                code = code_presence.text
            else:
                code = 50
            prot_code = self.doc_type or ''
            description = checkup.find('./d:Описание', namespaces=q_nsmap).text or ''
            results = checkup.findall('.//d:Результат', namespaces=q_nsmap)
            element_objs = []
            for result in results:
                element_path = result.text or ''
                element_objs.append({'Путь до элемента': element_path})
            input.verify_result['xqr_asserts'].append({
                'Код ошибки': code,
                'Код проверки': prot_code,
                'Описание': description,
                'Объекты': element_objs
            })

    async def _checkup_nonadv(self, checkups, q_nsmap, block_code, input):
        """
        Метод для получения результатов проверки (ошибок) для НЕ АДВ направлений
        """
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
                element_objs.append({'Путь до элемента': element_path,
                                     'Ожидаемое значение': expected_value or '',
                                     'Наименование': element_name or '',
                                     'Значение': element_value or ''})

            input.verify_result['xqr_asserts'].append({
                'Код ошибки': code,
                'Код проверки': prot_code,
                'Описание': description,
                'Объекты': element_objs
            })

    async def _validate_xquery(self, input: ClassVar[Dict[str, Any]], xml_file_path: str) -> None:
        """
        Метод для валидации файла по xquery выражениям.
        :return:
        """
        binds = {'$doc': f'{xml_file_path}'}
        if self.direction == 1:
            binds.update({'$dictFile': f'{self.dict_file}'})

        queries = await self._get_compendium_queries(self.direction, self.prefix)
        if queries is None:
            raise QueriesNotFound(self.prefix)

        for query in queries.values():
            query_result = await self._execute_query(query, binds)

            if query_result:
                check_result = etree.fromstring(query_result, parser=self.parser)
                q_nsmap = check_result.nsmap
                q_nsmap['d'] = q_nsmap.pop(None)
                # Запрос возвращает ответ в xml формате, проверяем,
                # вернулась ли ошибка (Результат != 0)
                block_code = check_result.attrib['ID']
                checkups = check_result.xpath(
                    '//d:Проверка[d:РезультатЗапроса/d:Результат[text()!=0]]',
                    namespaces=q_nsmap
                )
                if self.direction:
                    await self._checkup_nonadv(checkups, q_nsmap, block_code, input)
                else:
                    await self._checkup_adv(checkups, q_nsmap, input)
                # Обнаружили ошибки
                if checkups:
                    input.verify_result['result'] = 'failed_xqr'
                    input.verify_result['description'] = (
                        f'Ошибка при валидации по xquery выражению файла '
                        f'{self.xml_file}.')
            else:
                raise QueryResultError()

    def _get_comp_file(self, direction: str) -> Tuple[etree.ElementTree, Dict[str, Any]]:
        """
        Метод для получения дерева файла компендиума и простанства имён для указанного направления.
        :param direction:
        :return:
        """
        with open(os.path.join(self.xsd_root, direction, self.comp_file), 'rb') as handler:
            comp_file = etree.fromstring(handler.read(), parser=self.utf_parser)

        nsmap = comp_file.nsmap
        if nsmap.get(None):
            nsmap['d'] = nsmap.pop(None)

        return comp_file, nsmap

    def _get_doc_types(self, comp_file: etree.ElementTree, nsmap: Dict[str, Any]) -> List[etree.ElementTree]:
        """
        Метод для получения списка всех действующих проверочных документов.
        :param comp_file:
        :param nsmap:
        :return:
        """
        # Нужен действующий формат со статусом "ПоУмолчанию"
        doc_types_xpath = f'//d:ТипДокумента[d:Форматы/d:Формат[@Статус="Действующий" and @ПоУмолчанию="true"]]'
        doc_types = comp_file.xpath(doc_types_xpath, namespaces=nsmap)

        return doc_types

    def _get_definition(self, doc_type: etree.ElementTree, nsmap: Dict[str, Any]) -> str:
        """
        Метод для получения содержимого ноды "ОпределениеДокумента".
        :return:
        """
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
        """
        Метод для получения содержимого сценария и пространства имён.
        :return:
        """
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
        """
        Метод для получения имени и пути xquery скрипта по валидатору - протоколируемой проверке.
        :return:
        """
        validator_file = validator.xpath('./d:Файл/text()', namespaces=s_nsmap)[0]
        # Используем не .xml файл для проверки, а сразу сырой .xquery
        validator_file = validator_file.split('\\')[-1].split('.')[0] + '.xquery'
        query_file = os.path.join(self.xsd_root, direction, f'XQuery/{scenario_dir}', validator_file)

        return query_file, validator_file

    def _makeup_queries(self, queries: Dict[str, str]) -> str:
        """
        Метод собирает единый запрос для переданного словаря xquery выражений.
        :param queries:
        :return:
        """
        self.query.reset_query()
        # TODO: добавить общую обработку ошибок при сборке компендиума
        for query in queries.values():
            try:
                self.query.tokenize_query(query)
            except Exception as ex:
                pass

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

            queries_dict = {'query.xquery': self._makeup_queries(queries_dict)}

        return queries_dict

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
            'АДВ+АДИ+ДСВ 1.17.12д': {  # Направление
                'СЗВ-М': {  # Префикс проверяемого файла
                    'schemes': {  # Словарь проверочных XSD схем
                        'name.xsd': etree.ElementTree,
                        ...
                    },
                    'queries': {  # Словарь содержимого xquery скриптов
                        'name.xquery': str,
                        ...
                    },
                    'definiton': str  # Определение документа, нода "ОпределениеДокумента" в ПФР_КСАФ
                }
            }
        }
        :return:
        """
        # TODO: отлов исключений?
        self.compendium = dict()

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

    async def check_file(self, input, xml_file_path):
        self.xml_file = input.filename
        self.content = input.content
        self.xml_content = input.xml_tree

        input.verify_result = dict()

        input.verify_result['result'] = 'passed'
        input.verify_result['xsd_asserts'] = []
        input.verify_result['xqr_asserts'] = []

        # Открытие сессии BaseX
        self.session.execute(f'open xml_db{self.db_num}')

        await self._set_prefix()

        # Проверка по XSD
        if not await self._validate_xsd(input):
            return

        # Проверка по xquery выражениям
        await self._validate_xquery(input, xml_file_path)
