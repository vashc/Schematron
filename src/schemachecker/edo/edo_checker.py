import os
# noinspection PyUnresolvedReferences
from lxml import etree
from typing import Tuple, ClassVar, Dict, Any
from .exceptions import *


class EdoChecker:
    def __init__(self, *, root):
        self.parser = etree.XMLParser(encoding='cp1251', remove_comments=True)
        self.root = root

        # Компендиум проверочных схем
        self.compendium = dict()

        # Данные файла
        self.filename = None
        self.xml_content = None

    def _check_filename(self) -> Tuple[bool, str]:
        """ Метод проверяет соответствие имени файла и атрибута ИдФайл. """
        filename = self.filename.split('.')[0]
        attr_filename = self.xml_content.attrib['ИдФайл']
        return filename == attr_filename, attr_filename

    def setup_compendium(self) -> None:
        self.compendium = dict()

        comp_root = os.path.join(self.root, 'compendium')
        for root, dirs, files in os.walk(comp_root):
            for file in files:
                filename = file.split('.')[0]
                with open(os.path.join(root, file), 'r',
                          encoding='cp1251') as handler:
                    try:
                        xsd_name = '_'.join(filename.split('_')[:2])
                        xsd_content = etree.parse(handler, self.parser).getroot()
                        xsd_scheme = etree.XMLSchema(xsd_content)
                        self.compendium[xsd_name] = xsd_content, xsd_scheme
                    except etree.XMLSyntaxError as ex:
                        raise XsdSchemeError(ex)

    def check_file(self, file: ClassVar[Dict[str, Any]]) -> None:
        self.filename = file.filename
        self.xml_content = file.xml_tree

        file.verify_result = dict()

        file.verify_result['result'] = 'passed'
        file.verify_result['sch_asserts'] = []
        file.verify_result['xsd_asserts'] = []

        # Определение проверочной схемы
        prefix = '_'.join(self.filename.split('_')[:2])
        if 'mark' in prefix.lower() or 'pros' in prefix.lower():
            prefix = prefix[:-4]
        xsd_content, xsd_scheme = self.compendium[prefix]

        # Проверка имени файла
        correct_filename, attr_filename = self._check_filename()
        if not correct_filename:
            file.verify_result['result'] = 'failed_sch'
            file.verify_result['sch_asserts'].append((
                'Проверка имени файла на соответствие значению атрибута @ИдФайл',
                '0400400007',
                f'Имя файла обмена {self.filename} не совпадает со значением '
                f'атрибута ИдФайл {attr_filename}'
            ))

        # Проверка по xsd
        try:
            xsd_scheme.assertValid(self.xml_content)
        except etree.DocumentInvalid:
            file.verify_result['result'] = 'failed_xsd'
            for error in xsd_scheme.error_log:
                file.verify_result['xsd_asserts']\
                    .append(f'{error.message} (строка {error.line})')
