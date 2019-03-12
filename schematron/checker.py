import os
from time import time
from lxml import etree

from database import db
from .parser import parse
from config import CONFIG


def _return_error(text):
    return f'\u001b[31mError. {text}.\u001b[0m'


async def _get_xsd_scheme(xml_info):
    query = '''SELECT xsd
            FROM documents
            WHERE knd = $1
            AND version = $2'''

    return await db.fetchval(query, xml_info['knd'],
                                    xml_info['version'])


def _get_xml_info(xml_content, xml_file):
    xml_info = {}
    document_node = xml_content.find('Документ')
    if document_node is not None:
        xml_info['knd'] = document_node.get('КНД')
        xml_info['version'] = xml_content.attrib.get('ВерсФорм')
    else:
        # Ошибка, элемент "Документ" не найден
        raise Exception(f'Элемент "Документ" в файле '
                        f'{xml_file} не найден')

    return xml_info


def _get_error(node):
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


def _get_error_text(assertion):
    error = assertion['error']
    error_text = error['text']
    for replacement in error['replacing']:
        assertion['name'] = replacement
        error_text = error_text.replace(
            replacement, str(parse(assertion)))
    return error_text


def _get_asserts(content, xml_content, xml_file):
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
                if len(xml_content.xpath(f'//{context}')) == 0:
                    # Не найден контекст в xml файле

                    # Пропуск опциональных проверок, choice
                    choice_elements = assertion.xpath(f'ancestor::xs:choice',
                                                      namespaces=content.nsmap)
                    if len(choice_elements):
                        # Опциональная проверка, пропускаем
                        continue
                    # Ошибка, проверка обязательна, контекст не найден
                    raise Exception(f'Контекст {context} в файле '
                                    f'{xml_file} не найден')

                for sch_assert in rule:
                    for error_node in sch_assert:
                        error = _get_error(error_node)

                        assert_list.append({
                            'name': name,
                            'assert': sch_assert.attrib['test'],
                            'context': context,
                            'error': error
                        })

    return assert_list


async def _get_xsd_file(xml_file):
    prefix = '_'.join(xml_file.split('_')[:2])

    with open(os.path.join(CONFIG['xml_root'], xml_file), 'rb') as xml_file_handler:
        xml_content = etree.fromstring(xml_file_handler.read())

    try:
        xml_info = _get_xml_info(xml_content, xml_file)
    except Exception as ex:
        # Ошибка при получении информации (КНД, версия и т.д.)
        print(_return_error(ex))
        test_result = 'failed'
        return test_result

    try:
        xsd_file = await _get_xsd_scheme(xml_info)
        print(xsd_file)
    except Exception as ex:
        # Ошибка при получении имени xsd схемы из БД
        print(_return_error(f'Ошибка при получении имени xsd схемы из'
                                  f' БД при проверке файла {xml_file}.\u001b[0m'))
        test_result = 'failed'
        return test_result
    if not xsd_file:
        # На найдена xsd схема для проверки
        print(_return_error(f'Не найдена xsd схема для проверки '
                            f'файла {xml_file}.'))
        test_result = 'failed'
        return test_result

    print('XSD FILE:', xsd_file)
    return xml_content, xsd_file


def check_file(xml_file, xml_content, xsd_file):
    start_time = time()

    # Очищаем кэш
    _cache = dict()
    _parser = etree.XMLParser(encoding='cp1251', remove_comments=True)

    with open(os.path.join(CONFIG['xsd_root'], xsd_file),
              'r', encoding='cp1251') as xsd_file_handler:
        xsd_content = etree.parse(xsd_file_handler, _parser).getroot()
        xsd_schema = etree.XMLSchema(xsd_content)

    _xml_content = xml_content
    _xsd_content = xsd_content
    _xml_file = xml_file

    try:
        asserts = _get_asserts(xsd_content, xml_content, xml_file)
    except Exception as ex:
        print(_return_error(ex))
        test_result = 'failed'
        return test_result

    if not asserts:
        test_result = 'passed'
        return test_result

    results = []
    for assertion in asserts:
        assertion['xml_content'] = _xml_content
        assertion['xsd_content'] = _xsd_content
        assertion['xml_file'] = _xml_file
        results.append({
            'name': assertion['name'],
            'result': parse(assertion)
        })
        if results[-1]['result']:
            print(assertion['name'], ': \u001b[32mOk\u001b[0m')
        else:
            print(assertion['name'], ': \u001b[31mError\u001b[0m', end='. ')
            print(f'\u001b[31m{_get_error_text(assertion)}\u001b[0m')

    if all(result['result'] for result in results):
        print('\u001b[32mTest passed\u001b[0m')
        test_result = 'passed'
    else:
        print('\u001b[31mTest failed\u001b[0m')
        test_result = 'failed'

    elapsed_time = time() - start_time
    print(f'Elapsed time: {round(elapsed_time, 4)} s')
    print('Cache:', _cache)

    return test_result
