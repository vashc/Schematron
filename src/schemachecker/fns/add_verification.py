import re
from json import dumps, loads
from datetime import date
from lxml import etree


class AdditionalVerifier:
    def __init__(self, doc_type='rasch'):
        self.parser = etree.XMLParser(encoding='cp1251',
                                      remove_comments=True)
        self.doc_type = doc_type

    def process_input(self, content):
        ops_sum_root = content.xpath('/Файл/Документ/РасчетСВ/ОбязПлатСВ'
                                     '/РасчСВ_ОПС_ОМС/РасчСВ_ОПС')[0]

        sum_61 = ops_sum_root.find('НачислСВНеПрев')
        sum_2_61 = ops_sum_root.attrib.get('СумВсегоПосл3М')
        sum_3_61 = ops_sum_root.attrib.get('Сум1Посл3М')
        sum_4_61 = ops_sum_root.attrib.get('Сум2Посл3М')
        sum_5_61 = ops_su
        sum_30


        overall_sum_root = content.xpath('/Файл/Документ/РасчетСВ/ПерсСвСтрахЛиц'
                                         '/СвВыплСВОПС/СвВыпл//СвВыплМК')
        print(etree.tostring(ops_sum_root[0], encoding='utf-8').decode('utf-8'))


if __name__ == '__main__':
    file = '/home/vasily/PyProjects/FLK/' \
           'NO_RASCHSV_7719_7719_9718038663771901001_20190704_41991f74-5cd7-4b2c-b258-554a6279ed26.xml'
    ad_ver = AdditionalVerifier()
    with open(file, 'rb') as xml_handler:
        content = etree.parse(xml_handler, ad_ver.parser).getroot()
    # ad_ver.process_input(content)

    # period_codes = {
    #     '01': (0, 0),
    #     '02': (1, 1),
    #     '03': (2, 2),
    #     '04': (3, 3),
    #     '05': (4, 4),
    #     '06': (5, 5),
    #     '07': (6, 6),
    #     '08': (7, 7),
    #     '09': (8, 8),
    #     '10': (9, 9),
    #     '11': (10, 10),
    #     '12': (11, 11),
    #     '21': (0, 2),
    #     '22': (3, 5),
    #     '23': (6, 8),
    #     '24': (9, 11),
    #     '31': (0, 5),
    #     '33': (0, 8),
    #     '34': (0, 11)
    # }
    # period_codes = dumps(period_codes)
    # with open('/home/vasily/PyProjects/FLK/period_codes.info', 'w') as handler:
    #     handler.write(period_codes)
