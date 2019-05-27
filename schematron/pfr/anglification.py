import sys
import os
from lxml import etree
from .utils import Translator

comp_root = '/home/vasily/PyProjects/FLK/pfr/compendium'

# Флаг кодировки
utf_flag = True

cp_parser = etree.XMLParser(encoding='cp1251',
                            recover=True,
                            remove_comments=True)

utf_parser = etree.XMLParser(encoding='utf-8',
                             recover=True,
                             remove_comments=True)

directions = os.listdir(comp_root)
for direction in directions:
    cur_dir = os.path.join(comp_root, direction, 'Схемы')

    for root, dirs, files in os.walk(cur_dir):
        # Убираем кириллицу внутри файлов и переименовываем сами файлы
        for file in files:
            file_path = os.path.join(root, file)
            # Новое содержимое xsd схем
            tree = None

            with open(file_path, 'rb') as handler:
                content = handler.read()
                # Разные направления используют разную кодировку
                try:
                    scheme = etree.fromstring(content, utf_parser)
                    utf_flag = True
                except etree.XMLSyntaxError:
                    scheme = etree.fromstring(content, cp_parser)
                    utf_flag = False

                # Исправление кириллицы в schemaLocation URI
                nsmap = scheme.nsmap
                nsmap['empty'] = nsmap.pop(None)
                locations = scheme.xpath('.//*[@schemaLocation]', namespaces=nsmap)

                for loc in locations:
                    old_uri = loc.attrib['schemaLocation']
                    uri = Translator.translate(old_uri)
                    loc.attrib['schemaLocation'] = uri
                    if utf_flag:
                        tree = etree.tostring(scheme, encoding='utf-8')
                    else:
                        tree = etree.tostring(scheme, encoding='cp1251')

            # Содержимое файла было изменено, записываем
            if tree:
                with open(file_path, 'wb') as handler:
                    handler.write(tree)

            # Переименовываем файл
            new_file = Translator.translate(file)
            os.rename(file_path, os.path.join(root, new_file))

        # Переименовываем директории
        for idx, dir in enumerate(dirs):
            dir_path = os.path.join(root, dir)
            new_dir = Translator.translate(dir)
            os.rename(dir_path, os.path.join(root, new_dir))
            dirs[idx] = new_dir

    # Изменяем document-node()
    xquery_root = os.path.join(comp_root, direction, 'XQuery')
    pattern = 'declare variable $document as document-node() external;'
    new_pattern = 'declare variable $doc as xs:string external;\n' \
                  'declare variable $document := doc($doc);'

    for root, dirs, files in os.walk(xquery_root):
        for file in files:
            with open(os.path.join(root, file), 'r') as handler:
                text = handler.read()

            text = text.replace(pattern, new_pattern)
            with open(os.path.join(root, file), 'w') as handler:
                handler.write(text)

# if __name__ == '__main__':
#     try:
#         ROOT = sys.argv[1]
#         urls = get_files(compendium_link)
#         pprint(urls)
#         print(len(urls))
#     except IndexError as ex:
#         print(ex)
