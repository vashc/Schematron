import os
import re
# noinspection PyUnresolvedReferences
from lxml import etree
from typing import List, Tuple, ClassVar, Dict, Any
from .exceptions import *


class RarChecker:
    def __init__(self, *, root: str) -> None:
        self.root = root
        # Корневая директория для файлов валидации
        self.xsd_root = os.path.join(root, 'compendium/')
        self.compendium = dict()

        self.filename = None
        self.xml_content = None
        self.xsd_content: etree.ElementTree = None
        self.xsd_scheme: etree.XMLSchema = None

        self.charset = 'cp1251'
        self.parser = etree.XMLParser(encoding=self.charset,
                                      recover=True,
                                      remove_comments=True)

        # Регулярка для имени xsd схемы
        self.xsd_regex = re.compile('(\d+)-o-(\d+)_(\d+)\.xsd')

    @staticmethod
    def _set_error_struct(err_list: List[Tuple[str, str]], file: ClassVar[Dict[str, Any]]) -> None:
        """ Заполнение структуры ошибки для вывода. """
        for error in err_list:
            file.verify_result['asserts'].append({
                'error_code': error[0],
                'description': error[1],
                'inspection_items': []
            })

    def _set_scheme(self) -> None:
        """ Метод установки xsd схемы. """
        try:
            form_ver = self.xml_content.xpath('//Файл/@ВерсФорм')[0]
            form_num = self.xml_content.xpath('//Файл/ФормаОтч/@НомФорм')[0]
        except IndexError:
            raise SchemeNotFound(self.filename)

        self.xsd_scheme = self.compendium[f'{form_num}.{form_ver}']

    def _validate_xsd(self, file: ClassVar[Dict[str, Any]]) -> None:
        ret_list = []
        try:
            self.xsd_scheme.assertValid(self.xml_content)
        except etree.DocumentInvalid:
            file.verify_result['result'] = 'failed_xsd'
            for error in self.xsd_scheme.error_log:
                ret_list.append((error.line, error.message))

            self._set_error_struct(ret_list, file)

    def setup_compendium(self) -> None:
        self.compendium = dict()

        for root, dirs, files in os.walk(self.xsd_root):
            for file in files:
                with open(os.path.join(root, file), 'r', encoding='cp1251') as fd:
                    try:
                        match_groups = self.xsd_regex.match(file).groups()
                        xsd_comp_name = '.'.join(match_groups)
                        xsd_content = etree.parse(fd, self.parser).getroot()
                        xsd_scheme = etree.XMLSchema(xsd_content)
                        self.compendium[xsd_comp_name] = xsd_scheme
                    except etree.XMLSyntaxError as ex:
                        raise XsdParseError(file, ex)

    def check_file(self, file: ClassVar[Dict[str, Any]]) -> None:
        self.filename = file.filename
        self.xml_content = file.xml_tree

        file.verify_result = dict()

        file.verify_result['result'] = 'passed'
        file.verify_result['asserts'] = []

        self._set_scheme()

        # Проверка по xsd
        self._validate_xsd(file)
