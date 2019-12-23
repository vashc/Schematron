import os
from typing import Dict, Any, ClassVar, Union, Tuple
# noinspection PyUnresolvedReferences
from lxml import etree
from .interpreter import Interpreter
from .tokenizer import Tokenizer
from .exceptions import *


class FnsChecker:
    def __init__(self, *, root: str) -> None:
        self.root = root
        # Корневая директория для файлов валидации
        self.xsd_root = os.path.join(root, 'compendium/fns/compendium/')
        # Файл компендиума с информацией о проверочных схемах
        self.comp_file = 'astral_formatCompendium.xml'
        self.compendium = dict()

        self.filename = None
        self.xml_content = None
        self.xsd_content: etree.ElementTree = None
        self.xsd_scheme: etree.XMLSchema = None

        self.charset = 'cp1251'
        self.parser = etree.XMLParser(encoding=self.charset,
                                      recover=True,
                                      remove_comments=True)

        # Подготовка лексера и интерпретатора
        self.tokenizer = Tokenizer().create_tokenizer()
        self.interpreter = Interpreter()

    @staticmethod
    def _get_error(node: etree.Element) -> Dict[str, Any]:
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

    @staticmethod
    def _return_error(text: Union[str, Exception]) -> str:
        return f'Error. {text}.'

    @staticmethod
    def _set_error_struct(err_list: List[Tuple[str, str]], file: ClassVar[Dict[str, Any]]) -> None:
        """ Заполнение структуры ошибки для вывода. """
        for error in err_list:
            file.verify_result['asserts'].append({
                'error_code': error[0],
                'description': error[1],
                'inspection_items': []
            })

    def _get_error_text(self, assertion: Dict[str, Any]) -> str:
        error = assertion['error']
        error_text = error['text']
        for replacement in error['replacing']:
            error_text = error_text.replace(
                replacement, str(
                    self.interpreter.evaluate_expr(replacement,
                                                   self.xml_content,
                                                   self.filename,
                                                   assertion['context'])
                )
            )
        return error_text

    # TODO: вынести в создание компендиума
    def _get_asserts(self, content: etree.ElementTree) -> List[Dict[str, Any]]:
        """ Получение списка проверок Schematron-выражений. """
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
                    if len(self.xml_content.xpath(f'//{context}')) == 0:
                        # Не найден контекст в xml файле

                        # Пропуск опциональных проверок, choice
                        choice_elements = assertion.xpath(f'ancestor::xs:choice',
                                                          namespaces=content.nsmap)
                        if len(choice_elements):
                            # Опциональная проверка, пропускаем
                            continue
                        # Ошибка, проверка обязательна, контекст не найден
                        raise ContextError(context, self.filename)

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

    def _set_scheme(self, file: ClassVar[Dict[str, Any]]) -> None:
        """ Метод для установки XSD схемы. """
        try:
            file_info = file.get_result()['add_info']
            knd = file_info['knd']
            version = file_info['version']
        except AttributeError:
            raise FileAttributeError()

        comp_info = self.get_compendium_info(knd, version)
        self.xsd_content = comp_info['xsd_scheme']
        self.xsd_scheme = etree.XMLSchema(self.xsd_content)

    def _validate_xsd(self, file: ClassVar[Dict[str, Any]]) -> bool:
        ret_list = []
        try:
            self.xsd_scheme.assertValid(self.xml_content)
            return True
        except etree.DocumentInvalid as ex:
            for error in self.xsd_scheme.error_log:
                ret_list.append((str(error.line), error.message))
                self._set_error_struct(ret_list, file)

            file.verify_result['result'] = 'failed_xsd'
            file.verify_result['description'] = (
                f'Ошибка при валидации по xsd схеме файла '
                f'{self.filename}: {ex}.')
            return False

    def _validate_schematron(self, file: ClassVar[Dict[str, Any]]) -> None:
        try:
            asserts = self._get_asserts(self.xsd_content)
        except Exception as ex:
            file.verify_result['result'] = 'failed_sch'
            file.verify_result['description'] = self._return_error(ex)
            return

            # Нет выражений для проверки
        if not asserts:
            return

        ret_list = []
        for assertion in asserts:
            try:
                assertion_result = self.interpreter.evaluate_expr(assertion['assert'],
                                                                  self.xml_content,
                                                                  self.filename,
                                                                  assertion['context'])
                if not assertion_result:
                    ret_list.append((assertion['name'], self._get_error_text(assertion)))
            except ParserError:
                # FIXME (ParserError)
                pass
            except Exception as ex:
                file.verify_result['result'] = 'failed_sch'
                file.verify_result['description'] = ex
                return

        if len(ret_list):
            file.verify_result['result'] = 'failed_sch'
            file.verify_result['description'] = 'Ошибки при проверке fns'
            self._set_error_struct(ret_list, file)

    def _get_comp_file(self) -> etree.ElementTree:
        """
        Метод возвращает содержимое файла компендиума astral_formatCompendium.xml
        в виде etree.ElementTree.
        """
        try:
            with open(os.path.join(self.xsd_root, self.comp_file), 'rb') as xml_handler:
                compendium = etree.fromstring(xml_handler.read())
            return compendium
        except (IOError, Exception) as ex:
            raise CompendiumParseError(self.comp_file, ex)

    def _get_xsd_scheme(self, xsd_name: str) -> etree.ElementTree:
        """ Метод возвращает проверочную схему в виде etree.ElementTree. """
        try:
            with open(os.path.join(self.xsd_root, xsd_name), 'r', encoding=self.charset) as file:
                xsd_schema = etree.parse(file, self.parser).getroot()
                return xsd_schema
        except (IOError, Exception) as ex:
            # raise XsdParseError(xsd_name, ex)
            pass

    def _get_versions(self, format_node: etree.ElementTree) -> Dict[str, Dict[str, Any]]:
        """ Метод формирует словарь версий для заданного КНД. """
        version_dict = dict()
        for subformat in format_node:
            if subformat.get('XSD'):
                version = subformat.text
                xsd_name = subformat.get('XSD')
                xsd_scheme = self._get_xsd_scheme(xsd_name)
                date_from = subformat.get('dateFrom')
                date_till = subformat.get('dateTill')
                info_format = subformat.get('infoFormat')

                version_dict.update({version: {
                    'xsd_name':     xsd_name,
                    'xsd_scheme':   xsd_scheme,
                    'date_from':    date_from,
                    'date_till':    date_till,
                    'info_format':  info_format
                }})

        return version_dict

    def _check_filename(self) -> Tuple[bool, str]:
        """ Метод проверяет соответствие имени файла и атрибута ИдФайл. """
        filename = os.path.splitext(self.filename)[0]
        attr_filename = self.xml_content.attrib['ИдФайл']
        return filename == attr_filename, attr_filename

    def _mandatory_verification(self, file: ClassVar[Dict[str, Any]]) -> bool:
        """ Метод реализует обязательные проверки, например, проверку соответствия имени файла. """
        correct_filename, attr_filename = self._check_filename()
        if not correct_filename:
            ret_list = [('', f'Имя файла обмена {self.filename} не совпадает со значением '
                             f'атрибута ИдФайл {attr_filename}')]
            self._set_error_struct(ret_list, file)

            file.verify_result['result'] = 'failed_ver'
            file.verify_result['description'] = (
                f'Ошибка при проведении обязательных проверок {self.filename}')
            return False

        return True

    def setup_compendium(self) -> None:
        """
        Сборка компендиума в памяти.
        Компендиум имеет следующую структуру:
        {
            '0710099': {  # КНД
                'alias_short': 'Бухгалтерская отчетность',
                'alias_full': 'Бухгалтерская отчетность',
                'versions': {  # Cловарь версий
                    '5.01': {  # Версия
                        'xsd_name': 'NO_BUHOTCH_1_105_00_05_01_01.xsd',
                        'xsd_scheme': etree.ElementTree,
                        'date_from': '01.01.2016',
                        'date_till': '01.01.2019',
                        'info_format': ''
                    }
                }
            }
        }
        """
        compendium = self._get_comp_file()
        formats = compendium.xpath('//format[@direction="ФНС" or @direction=""]')

        self.compendium = dict()
        for _format in formats:
            if _format.get('obsolete') != 'true':
                knd = _format.get('searchKey')
                alias_short = _format.get('aliasShort')
                alias_full = _format.get('aliasFull')
                versions = self._get_versions(_format)
                self.compendium.update({knd: {
                    'alias_short':  alias_short,
                    'alias_full':   alias_full,
                    'versions':     versions
                }})

    def get_compendium_info(self, knd: str, version: str) -> Dict[str, Any]:
        """ Метод получения информации по КНД и версии из компендиума в памяти. """
        try:
            res = {'knd': knd, 'version': version}
            knd_node = self.compendium.get(knd)
            res.update({'alias_short': knd_node['alias_short'],
                       'alias_full': knd_node['alias_full']})

            version_node = knd_node['versions'].get(version)
            res.update(**version_node)
            return res
        except (AttributeError, TypeError):
            raise SchemeNotFound(knd, version)

    def check_file(self, file: ClassVar[Dict[str, Any]]) -> None:
        self.filename = file.filename
        self.xml_content = file.xml_tree

        file.verify_result = dict()

        file.verify_result['result'] = 'passed'
        file.verify_result['asserts'] = []

        self._set_scheme(file)

        # Обязательные проверки
        if not self._mandatory_verification(file):
            return

        # Проверка по xsd
        if not self._validate_xsd(file):
            return

        # Проверка выражений fns
        self._validate_schematron(file)
