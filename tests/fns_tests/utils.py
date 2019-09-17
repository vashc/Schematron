import os
import asyncio
from typing import List, Dict, Awaitable, Union
from lxml import etree
from tests.database import db


def assert_list_equality(first: List[str],
                         second: List[str]) -> bool:
    return all(f == s for f, s in zip(first, second))


def get_file_list(path: str) -> List[str]:
    tests = []
    for dirpath, dirnames, filenames in os.walk(path):
        tests.extend(filenames)
        break

    return tests


class FileResolver:
    def __init__(self, xsd_content=None, xml_content=None, xml_file=None):
        self.xsd_schema = xsd_content
        self.xml_obj = xml_content
        self.filename = xml_file

    @staticmethod
    def return_error(text: Union[str, Exception]) -> str:
        return f'\u001b[31mError. {text}.\u001b[0m'

    @staticmethod
    def get_xml_info(xml_file: str,
                     xml_content: etree.ElementTree) -> Dict[str, str]:
        xml_info = {}
        document_node = xml_content.find('Документ')
        if document_node is not None:
            xml_info['knd'] = document_node.get('КНД', None)
            if not xml_info['knd']:
                xml_info['knd'] = document_node.get('Индекс', None)
            if not xml_info['knd']:
                xml_info['knd'] = xml_content.xpath('//Документ/ОписПерСвед/@КНД')[0]
            xml_info['version'] = xml_content.attrib.get('ВерсФорм')
        else:
            # Ошибка, элемент "Документ" не найден
            raise Exception(f'Элемент "Документ" в файле '
                            f'{xml_file} не найден')
        return xml_info

    @staticmethod
    async def get_xsd_scheme(xml_info: Dict[str, str]) -> Awaitable:
        query = '''SELECT xsd
                FROM documents
                WHERE knd = $1
                AND version = $2'''

        return await db.fetchval(query, xml_info['knd'], xml_info['version'])

    @staticmethod
    async def get_xsd_file(xml_path: str, xsd_root: str) -> 'FileResolver':
        parser = etree.XMLParser(encoding='cp1251', remove_comments=True)
        xml_file = os.path.basename(xml_path)

        # Формирование результата
        result = {'file': xml_path,
                  'result': 'failed',
                  'description': '',
                  'asserts': []}

        with open(xml_path, 'rb') as xml_file_handler:
            try:
                xml_content = etree.fromstring(xml_file_handler.read())
            except etree.XMLSyntaxError:
                print('Error xml file parsing:', xml_path)
                return FileResolver()

        try:
            xml_info = FileResolver.get_xml_info(xml_file, xml_content)
        except Exception as ex:
            # Ошибка при получении информации (КНД, версия и т.д.)
            print(FileResolver.return_error(ex))
            result['description'] = FileResolver.return_error(ex)
            return FileResolver()
            # return result

        try:
            xsd_file = await FileResolver.get_xsd_scheme(xml_info)
        except Exception as ex:
            # Ошибка при получении имени xsd схемы из БД
            print(FileResolver.return_error(f'Ошибка при получении имени xsd схемы из'
                                            f' БД при проверке файла {xml_file}: {ex}.'))
            result['description'] = f'Ошибка при получении имени xsd ' \
                                    f'схемы из БД при проверке файла {xml_file}.'
            return FileResolver()
            # return result

        if not xsd_file:
            # На найдена xsd схема для проверки
            print('_' * 80)
            print('FILE:', xml_file)
            print(FileResolver.return_error(f'Не найдена xsd схема для проверки '
                                            f'файла {xml_file}.'))
            result['description'] = f'Не найдена xsd схема для проверки файла {xml_file}.'
            return FileResolver()
            # return result

        result['xsd_scheme'] = xsd_file

        try:
            with open(os.path.join(xsd_root, result['xsd_scheme']),
                      'r', encoding='cp1251') as xsd_file_handler:
                xsd_content = etree.parse(xsd_file_handler, parser).getroot()
            return FileResolver(xsd_content, xml_content, xml_file)
        except FileNotFoundError as ex:
            return FileResolver()

    def resolve_file(self, xml_path: str, xsd_root: str) -> 'FileResolver':
        loop = asyncio.get_event_loop()
        resolver = loop.run_until_complete(self.get_xsd_file(xml_path, xsd_root))
        return resolver
