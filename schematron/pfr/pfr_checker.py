import os
import BaseXClient
from lxml import etree
from .utils import Flock


class PfrChecker:
    def __init__(self, *, root):
        self.root = root
        # Корневая директория для файлов валидации
        self.xsd_root = os.path.join(root, 'compendium/pfr/compendium/'
                                           'СЗВ-М+ИС+УПП АФ.2.32д/')
        # Справочники для проверок КОРР файлов
        # (нужны для передачи в переменную $dictFile)
        self.dict_file = os.path.join(self.xsd_root, 'Справочники/Справочники.xml')
        # Корневая директория BaseX
        self.db_data = os.path.join(root, 'basex/data/')
        # Синхронизация записи в базы данных BaseX
        # для избежания write lock
        with Flock(os.path.join(self.db_data, '.sync')) as handler:
            self.db_num = int(handler.read(1))
            if self.db_num < os.cpu_count():
                self.db_num += 1
                self.db_root = self.db_data + f'xml_db{self.db_num}'
                handler.seek(0)
                handler.write(str(self.db_num))
            else:
                raise Exception('Too many BaseX workers')

        # Название основного валидационного файла
        self.compendium_file = 'ПФР_КСАФ_20190205_a95f0eee-1556-488e-8d3e-27e89122314d.xml'

        # Название файла ПФР
        self.xml_file = None
        # Содержимое файла
        self.content = None
        # Содержимое файла в виде etree Element
        self.xml_content = None

        self.parser = etree.XMLParser(encoding='utf-8',
                                      recover=True,
                                      remove_comments=True)

        # Достаём информацию из компендиума
        with open(os.path.join(self.xsd_root, self.compendium_file), 'rb') as handler:
            try:
                self.compendium = etree.fromstring(handler.read(), parser=self.parser)
            except etree.XMLSyntaxError as ex:
                print(f'Error xml file parsing: {ex}')

        self.session = BaseXClient.Session('localhost', 1984, 'admin', 'admin')

    def __del__(self):
        # Воркер завершил работу, синхронизируем
        with Flock(os.path.join(self.db_data, '.sync')) as handler:
            num = int(handler.read(1))
            if num > 0:
                num -= 1
                handler.seek(0)
                handler.write(str(num))
            else:
                raise Exception('Incorrect synchronisation value')

        # Закрываем сессию
        if self.session:
            self.session.close()

    def check_file(self, input, xml_file_path):
        self.xml_file = input.filename
        self.content = input.content
        self.xml_content = input.xml_obj

        input.verify_result = dict()

        input.verify_result['result'] = 'passed'
        input.verify_result['xsd_asserts'] = []
        input.verify_result['xqr_asserts'] = []

        # Открытие сессии BaseX
        self.session.execute(f'open xml_db{self.db_num}')

        # Префикс файла (СЗВ-М, СТАЖ и т.д.)
        if 'СЗВ' in self.xml_file:
            prefix = self.xml_file.split('_')[3]
        elif 'УППО' in self.xml_file:
            prefix = self.xml_file.split('_')[1]
        elif 'ЗНП' in self.xml_file or 'ЗДП' in self.xml_file:
            prefix = self.xml_file.split('_')[2]


        # Пришлось модифицировать пространство имён,
        # т.к. не съедает пустые ключи в словаре
        nsmap = self.compendium.nsmap
        nsmap['d'] = nsmap.pop(None)

        # Нужен действующий формат со статусом "ПоУмолчанию"
        doc_format = self.compendium.xpath(
            f'//d:ТипДокумента[@Код="{prefix}"]//d:Формат'
            f'[@Статус="Действующий" and @ПоУмолчанию="true"]',
            namespaces=nsmap)
        # Путь к валидационной схеме(ам)
        schemes = doc_format[0].xpath('.//d:Валидация/d:Схема/text()', namespaces=nsmap)

        # Пути в схемах как в винде, поэтому меняем слэши
        for idx, scheme in enumerate(schemes):
            schemes[idx] = scheme[1:].replace('\\', '/')

        # Работаем с проверяемым .xml файлом
        # Проверка, есть ли уже такой в базе
        query = self.session.query(f'db:exists("xml_db{self.db_num}", '
                                   f'"{self.db_root}/{self.xml_file}")')
        query_result = query.execute()
        if query_result == 'false':
            # Файл не найден, добавляем
            self.session.add(f'{self.db_root}/{self.xml_file}',
                             self.content.decode('utf-8'))
        if query:
            query.close()

        # Пробегаем по всем .xsd схемам и проверяем файл
        for scheme in schemes:
            with open(os.path.join(self.xsd_root, scheme), 'r') as xsd_handler:
                try:
                    xsd_content = etree.parse(xsd_handler, self.parser).getroot()
                    xsd_scheme = etree.XMLSchema(xsd_content)
                    try:
                        xsd_scheme.assertValid(self.xml_content)
                    except etree.DocumentInvalid as ex:
                        for error in xsd_scheme.error_log:
                            input.verify_result['xsd_asserts'] \
                                .append(f'{error.message} (строка {error.line})')

                        input.verify_result['result'] = 'failed_xsd'
                        input.verify_result['description'] = (
                            f'Ошибка при валидации по xsd схеме файла '
                            f'{self.xml_file}: {ex}.')
                        return

                except etree.XMLSyntaxError as ex:
                    #TODO: logger
                    # print(f'Error xsd file parsing: {ex}')
                    return

        # Проверка по xsd пройдена, дальше проверяем сценарий
        scenario_file = doc_format[0].xpath('.//d:Сценарий/text()', namespaces=nsmap)[0]
        # Те же танцы с бубнами для слэшей
        scenario_dir = scenario_file.split('\\')[-1].split('.')[0]
        scenario_file = scenario_file[1:].replace('\\', '/')

        # Получение содержимого сценария
        with open(os.path.join(self.xsd_root, scenario_file), 'rb') as handler:
            try:
                scenario = etree.fromstring(handler.read(), parser=self.parser)
            except etree.XMLSyntaxError as ex:
                print(f'Error scenario file parsing: {ex}')

        # Та же история с пространством имён, не работает с пустыми ключами
        nsmap = scenario.nsmap
        nsmap['d'] = nsmap.pop(None)

        # Получение всех протоколируемых проверок
        validators = scenario.xpath('//d:Проверки/d:Проверка[not(@Протоколируемая="0")]', namespaces=nsmap)

        # Проход по всем проверкам
        for validator in validators:
            validator_file = validator.xpath('./d:Файл/text()', namespaces=nsmap)[0]
            # Используем не .xml файл для проверки, а сразу сырой .xquery
            validator_file = validator_file.split('\\')[-1].split('.')[0] + '.xquery'

            query_file = os.path.join(self.xsd_root, f'XQuery/{scenario_dir}', validator_file)

            with open(query_file, 'r', encoding="utf-8") as handler:
                query = self.session.query(handler.read())

            # Передача external переменных в xquery запрос
            query.bind('$doc', f'{xml_file_path}')
            query.bind('$dictFile', f'{self.dict_file}')

            query_result = query.execute()

            if query_result:
                check_result = etree.fromstring(query_result, parser=self.parser)
                q_nsmap = check_result.nsmap
                q_nsmap['d'] = q_nsmap.pop(None)
                # Запрос возвращает ответ в xml формате, проверяем, вернулась ли ошибка (Результат != 0)
                block_code = check_result.attrib['ID']
                checkups = check_result.xpath('//d:Проверка[d:РезультатЗапроса/d:Результат[text()!=0]]',
                                              namespaces=q_nsmap)
                # Обнаружили ошибки
                for checkup in checkups:
                    check_code = checkup.attrib['ID']
                    prot_code = '.'.join((block_code, check_code))
                    code = checkup.find('./d:КодРезультата', namespaces=q_nsmap).text
                    description = checkup.find('./d:Описание', namespaces=q_nsmap).text
                    results = checkup.findall('.//d:Результат', namespaces=q_nsmap)
                    element_objs = []
                    for result in results:
                        element_path = result.find('./d:ПутьДоЭлемента', namespaces=q_nsmap).text
                        expected_value = result.find('./d:ОжидаемоеЗначение', namespaces=q_nsmap).text
                        element_name = result.find('./d:Объект/d:Наименование', namespaces=q_nsmap).text
                        element_value = result.find('./d:Объект/d:Значение', namespaces=q_nsmap).text
                        element_objs.append((element_path, expected_value, element_name, element_value))

                    input.verify_result['xqr_asserts'].append((
                        code, prot_code, description, element_objs
                    ))
                    input.verify_result['result'] = 'failed_xqr'
                    input.verify_result['description'] = (
                        f'Ошибка при валидации по xquery выражению файла '
                        f'{self.xml_file}.')
                    return
            #TODO: check if no result has been returned
