import os
import re
import json
from datetime import datetime
from tests.database import db
from pprint import pprint
from lxml import etree


class TestResolver(object):
    def __init__(self, xsd_content, xml_content, xml_file):
        self.xsd_schema = xsd_content
        self.xml_obj = xml_content
        self.filename = xml_file


def _return_error(text):
    return f'\u001b[31mError. {text}.\u001b[0m'


def _get_xml_info(xml_file, xml_content):
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


async def _get_xsd_scheme(xml_info):
    query = '''SELECT xsd
            FROM documents
            WHERE knd = $1
            AND version = $2'''

    return await db.fetchval(query, xml_info['knd'], xml_info['version'])


async def get_xsd_file(xml_path, xsd_root):
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
            return TestResolver(None, None, None)

    try:
        xml_info = _get_xml_info(xml_file, xml_content)
    except Exception as ex:
        # Ошибка при получении информации (КНД, версия и т.д.)
        print(_return_error(ex))
        result['description'] = _return_error(ex)
        return TestResolver(None, None, None)
        # return result

    try:
        xsd_file = await _get_xsd_scheme(xml_info)
    except Exception as ex:
        # Ошибка при получении имени xsd схемы из БД
        print(_return_error(f'Ошибка при получении имени xsd схемы из'
                            f' БД при проверке файла {xml_file}: {ex}.'))
        result['description'] = f'Ошибка при получении имени xsd ' \
                                f'схемы из БД при проверке файла {xml_file}.'
        return TestResolver(None, None, None)
        # return result

    if not xsd_file:
        # На найдена xsd схема для проверки
        print('_' * 80)
        print('FILE:', xml_file)
        print(_return_error(f'Не найдена xsd схема для проверки '
                            f'файла {xml_file}.'))
        result['description'] = f'Не найдена xsd схема для проверки файла {xml_file}.'
        return TestResolver(None, None, None)
        # return result

    result['xsd_scheme'] = xsd_file

    try:
        with open(os.path.join(xsd_root, result['xsd_scheme']),
                  'r', encoding='cp1251') as xsd_file_handler:
            xsd_content = etree.parse(xsd_file_handler, parser).getroot()
        resolver = TestResolver(xsd_content, xml_content, xml_file)
        return resolver
    except FileNotFoundError as ex:
        return TestResolver(None, None, None)

if __name__ == '__main__':
    parser = etree.XMLParser(encoding='cp1251',
                             remove_comments=True)
    u_parser = etree.XMLParser(encoding='utf8',
                               remove_comments=True)

    # filename = 'NO_NDFL4_9999_9999_9649403827999901001_20190409_3B3DF42A-153B-4613-9795-3CDF9F8F61A6.xml'
    # filename = 'NO_RASCHSV_9105_9105_9105017720910501001_20190705_5A036D6A-DC48-B98B-1145-5D92E85FEC16.xml'
    # filename = 'NO_NDS_9_2367_2367_2317083000231.xml'
    # filename = 'NO_NDFL3_7714_7714_772784575277_20190520_6A141CF4-9AA7-4D7F-93CE-DB7E178D352F.xml'
    # filename = 'ON_DOCNPNO_741302078264_741302078264_2352_20190718_38d2dfe6-ad91-465c-891e-c30d6ef34bdb.xml'
    # filename = 'NO_IMUR_6686_6686_6686047787668601001_20181130_7d09e5df-424c-4a4f-9005-0283b9425157.xml'
    # filename = 'NO_IMUR_7724_7724_7724889644772401001_20190717_b220d952-2364-44dc-aff1-ff55fbe9cb10.xml'
    # filename = 'NO_NDPI_2646_2646_2635072555260501001_20190718_DCEB57AB-6848-C648-A119-FF764D9AF6E1.xml'
    # filename = 'IU_ZAPR_4827_0000_4826136806482601001_20190719_543164F1-081B-4884-B985-9CDDE3E561F8.xml'
    filename = 'NO_PRIB_1686_1686_1660299220166001001_20190715_3522482f-4bb6-499c-9d09-7bda6e9dc7e0.xml'

    file = '/home/vasily/PyProjects/FLK/fns/' + filename
    with open(file, 'rb') as handler:
        content = handler.read()
        handler.seek(0)
        xml_obj = etree.parse(handler, u_parser).getroot()

    period_codes_path = '/home/vasily/PyProjects/FLK/period_codes.info'
    with open(period_codes_path) as handler:
        period_codes = json.loads(handler.read())

    async def get_info():
        query = """SELECT xsd, alias_short, date_from, date_till
                   FROM documents 
                   WHERE knd = $1
                   AND version = $2"""

        return await db.fetchrow(query, '1151111', '5.01')


    def get_common_data_xml(_xml_tree):
        document = _xml_tree.find("Документ")

        res = dict()
        res['add_flag'] = False
        res['version'] = _xml_tree.attrib.get("ВерсФорм", None)
        res['period'] = document.attrib.get("Период")
        res['year'] = document.attrib.get("ОтчетГод")

        knd = None
        date = None
        recipient = None

        if document is not None:
            knd = document.attrib.get("КНД", None)
            if not knd:
                knd = document.attrib.get("Индекс", None)
            if not knd:
                knd = _xml_tree.xpath('//Документ/ОписПерСвед/@КНД')
                knd = knd[0] if knd else None
            date = document.attrib.get("ДатаДок", None)
            recipient = document.attrib.get("КодНО", None)

        res['knd'] = knd
        res['date'] = date
        res['recipient'] = recipient

        # Дополнительная информация для отображения на фронте
        svnp = document.find('.//СвНП')
        if svnp is not None:
            res['add_flag'] = True
            svnp_inn_jp = svnp.xpath('.//*/@ИННЮЛ')
            svnp_kpp_jp = svnp.xpath('.//*/@КПП')
            svnp_inn_pp = svnp.xpath('.//*/@ИННФЛ')
            if len(svnp_inn_jp) > 0:
                svnp_inn_jp.extend(svnp_kpp_jp)
                res['responder_inn_kpp'] = '/'.join(svnp_inn_jp)
            elif len(svnp_inn_pp) > 0:
                res['responder_inn_kpp'] = svnp_inn_pp[0]
            else:
                res['responder_inn_kpp'] = ''

            svnp_name = svnp.xpath('.//*/@Имя')
            svnp_surname = svnp.xpath('.//*/@Фамилия')
            svnp_midname = svnp.xpath('.//*/@Отчество')
            svnp_surname.extend(svnp_name)
            svnp_surname.extend(svnp_midname)
            res['responder_fio'] = ' '.join(svnp_surname)

            svnp_org = svnp.xpath('.//*/@НаимОрг')
            if len(svnp_org) > 0:
                res['responder_org'] = svnp_org[0]

        var = document.attrib.get("НомКорр", "None")
        if var == '0':
            res['var'] = "Первичный"
        elif var != "None":
            res['var'] = "Уточнённый №" + var
        else:
            res['var'] = var

        podp_root = document.find('.//Подписант')
        if podp_root is not None:
            podp = podp_root.xpath('./ФИО/@*')
            if podp is not None:
                res['podp_fio'] = ' '.join(podp)
                res['sender_fio'] = res['podp_fio']

            pred = podp_root.find('./СвПред')
            if pred is not None:
                res['sender_org'] = pred.attrib.get('НаимОрг', 'None')
            # Нет доверителя, отчитывающийся = отправитель
            else:
                res['sender_inn_kpp'] = res.get('responder_inn_kpp')
                res['sender_org'] = res.get('responder_org')

        pprint(res)

        return res

    def _period_verification(year, period, date_from, date_till):
        """
        Метод проверки соответствия формата и периода.
        дата_начала <= отчёт_год + период <= дата_конца
        Возвращает True, если период сдачи отчёта соответствует компендиуму.
        """
        if period is None:
            return True

        period_date = datetime.strptime(year, '%Y')
        _date_from = datetime.combine(date_from, datetime.min.time())
        _date_till = datetime.combine(date_till, datetime.min.time())

        min_date = period_date.replace(month=period_date.month
                                             + period_codes[period][0])
        max_date = period_date.replace(month=period_date.month
                                             + period_codes[period][1])

        if _date_from <= min_date and max_date <= _date_till:
            return True

        return False

    def _kpp_verification(inn_kpp):
        """
        Метод для сверки ИНН/КПП внутри файла и в имени файла.
        """
        # Проверка на доверенность
        if xml_obj.xpath('//Подписант/СвПред') is None:
            inn_kpp_root = xml_obj.find('.//НПЮЛ')
            inn = inn_kpp_root.attrib.get('ИННЮЛ')
            kpp = inn_kpp_root.attrib.get('КПП')

            if len(inn_kpp) == 19:
                try:
                    if inn + kpp != inn_kpp:
                        return False
                except Exception:
                    return False

            elif len(inn_kpp) == 12:
                try:
                    if all((inn_kpp != inn, inn_kpp != '0'*12)):
                        return False
                except Exception:
                    return False

        return True

    def _mask_verification():
        """
        Метод проверки соответствия названия файла маске.
        Возвращает ИНН/КПП, если название соответствует маске.
        """
        _filename = filename.lower()
        common_xml_mask = re.compile(
            '[a-z]+_[a-z0-9.]+_\d{4}_(\d{4})_(\d{12}|\d{19})_\d{8}_[-a-z0-9]{1,36}\.xml'
        )
        # Маска для счетов-фактур
        fakt_xml_mask = re.compile(
            '(1115104)_(\d{12}|\d{19})_(\d{4})_[-a-z0-9]{36}_\d{8}_[-a-z0-9]{36}\.xml'
        )
        jur_txt_mask = re.compile('o(\d{10})_(\d{9})_\d{8}_\d{2}_\d{8}\.txt')
        ind_txt_mask = re.compile('o(\d{12})_\d{8}_\d{2}_\d{8}\.txt')

        if type == 'XML':
            common_match = common_xml_mask.match(_filename)
            if common_match:
                return common_match.groups()
            else:
                fakt_match = fakt_xml_mask.match(_filename)
                if fakt_match:
                    return fakt_match.groups()
            return False

        elif type == 'TXT':
            jur_match = jur_txt_mask.match(_filename)
            if jur_match:
                return ''.join(jur_match.groups())
            else:
                ind_match = ind_txt_mask.match(_filename)
                if ind_match:
                    return ind_match.groups()[0]
            return False

        return False

    def _encoding_verification():
        """
        Метод проверки xml файлов ФНС на кодировку windows-1251.
        """
        if b'windows-1251' not in content[:80].lower():
            return False

        return True

    def _fns_addition_verification(common_data, date_from, date_till):
        """
        Метод выполняет дополнительные проверки файлов ФНС:
        - период отчёта;
        - маска файла;
        - кодировка файла;
        - сверка КПП внутри и в имени файла.
        """
        result = dict()
        result["add_info"] = dict()

        #TODO: через список
        if not _period_verification(common_data['year'],
                                    common_data['period'],
                                    date_from,
                                    date_till):
            print(14)

        mask_result = _mask_verification()

        if not mask_result:
            print('Mask:', 15)
            return

        if len(mask_result) > 0:
            recipient, inn_kpp = mask_result[0], mask_result[1]
        # В названии txt файла отсутствуют коды назначения
        else:
            inn_kpp = mask_result[0]

        if not inn_kpp:
            print('Not inn_kpp:', 15)
            return

        if len(inn_kpp) == 19:
            result["add_info"]['inn_filename'] = inn_kpp[:12]
            result["add_info"]['kpp_filename'] = inn_kpp[12:]
        elif len(inn_kpp) == 12:
            result["add_info"]['inn_filename'] = inn_kpp

        if not _encoding_verification():
            print(16)
            return

        if not _kpp_verification(inn_kpp):
            print(17)
            return

        return recipient


    type = 'TXT'

    txt_reps = '/home/vasily/PyProjects/FLK/txt_reports'
    files = os.listdir(txt_reps)
    for file in files[:]:
        print('-' * 80)
        with open(os.path.join(txt_reps, file), encoding='cp866') as handler:
            filename = file
            print(_mask_verification())
            content = handler.read()
            # print(content)
            fields = {
                'ВерсФорм': 'version',
                'НаимОтпрЮл': 'sender_org',
                'ФИООтпр': 'sender_fio',
                'ФИООтпрФл': 'sender_fio',
                'ДатаДок': 'date',
                'КНД': 'knd',
                'ГодПериодОтч': 'year',
                'НаимЮЛПол': 'responder_org'
            }
            res = dict()

            for line in content.split('\n'):
                matcher = re.match('ИдФайл:(\d+)[*]{2,}.*', line)
                if matcher:
                    res['responder_inn_kpp'] = matcher.groups()[0]

                for field, res_field in fields.items():
                    if 'КППЮЛ' in line:
                        res['responder_inn_kpp'] = '/'.join([res.get('responder_inn_kpp'),
                                                             line.split(':')[1]])
                        break
                    if field in line:
                        if 'ФИО' in field:
                            res[res_field] = ' '.join(line.split(':')[1].split(','))
                        else:
                            res[res_field] = line.split(':')[1]

            pprint(res)


    # loop = asyncio.get_event_loop()
    # id, xsd, alias_short, date_rom, date_till
    # _, _, date_from, date_till = loop.run_until_complete(get_info())
    # info = get_common_data_xml(xml_obj)
    # _fns_addition_verification(info, date_from, date_till)
    # print('Period verification:', _period_verification(info['year'], info['period'], date_from, date_till))
    # print('Encoding verification:', _encoding_verification())
    # print('Mask verification:', _mask_verification(filename, 'XML'))
    # inn_kpp = _mask_verification(filename, 'XML')
    # if inn_kpp:
    #     print('Kpp verification:', _kpp_verification(inn_kpp))
    # else:
    #     print('Kpp verification:', False)
