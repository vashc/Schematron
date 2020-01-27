import os
# noinspection PyUnresolvedReferences
from lxml import etree
from collections import OrderedDict
from typing import List, Dict, Any, Tuple, Union, ClassVar
from .utils import DotDict
from .interpreter import Interpreter, PeriodInterpreter
from .tokenizer import Tokenizer
from ._dataframe import DataFrame
from .exceptions import *


# TODO: make its own class errors
class StatChecker:
    def __init__(self, *, root):
        self.root = root
        self.parser = etree.XMLParser(encoding='utf-8',
                                      recover=True,
                                      remove_comments=True)
        # Содержимое xml файла
        self.xml_report = DotDict()
        self.xml_title = DotDict()
        self.xml_sections = DotDict()
        self.filename = None
        self.content = None
        self.okud = None

        # Содержимое компендиума проверочных схем
        self.compendium = DotDict()

        # Словарь датафреймов, ключи - id секций
        self.frames = DotDict()

        # Подготовка лексера и интерпретатора
        self.interpreter = Interpreter()
        self.period_interpreter = PeriodInterpreter()
        self.tokenizer = Tokenizer()
        self.condition, self.log_expr, self.period_cond = self.tokenizer.create_tokenizer()

    @staticmethod
    def _set_error_struct(err_list: List[Tuple[str, str]], file: ClassVar[Dict[str, Any]]) -> None:
        """ Заполнение структуры ошибки для вывода. """
        for error in err_list:
            file.verify_result['asserts'].append({
                'error_code': error[0],
                'description': error[1],
                'inspection_items': []
            })

    def process_input(self, filename, content):
        self.filename = filename
        self.content = content
        # Убираем расширение
        pure_filename = filename.split('.')[0]

        add_info_split = pure_filename.split('__')
        if len(add_info_split) > 1:
            # Дополнительная информация в заголовке файла
            add_info = add_info_split[1]

        # Основная информация в заголовке файла
        file_info = add_info_split[0].split('_')

        try:
            _okud, _idf, _idp, _okpo, _year = file_info[:5]
            _period = file_info[5]
            self.period_interpreter.period = _period
            # Уникальный идентификатор формы - ОКУД + IDF
            self.okud = f'{_okud}_{int(_idf)}'
            if len(file_info) > 6:
                # Дополнительная, необязательная информация
                _extinfo = file_info[6:]
        except IndexError:
            raise InputError(self.filename,
                             'Формат названия файла не распознан')
        except ValueError:
            raise InputError(self.filename,
                             'Невалидная информация в заголовке файла')

        # Данные о статистическом отчёте
        self.xml_report = DotDict(self.content.items())

        # Данные титульной страницы отчёта
        try:
            title_items = self.content.xpath('/report/title//item')
            for item in title_items:
                self.xml_title[item.attrib['name']] = item.attrib['value']
        except KeyError as ex:
            raise InputError(self.filename,
                             f'Не найден обязательный атрибут в элементе item: {ex}')

        # Данные о разделах
        sections = self.content.xpath('/report/sections//section')
        for section in sections:
            sec_code = section.attrib['code']
            try:
                self.frames[sec_code] = \
                    DataFrame.from_file_content(
                        section, self.compendium[self.okud].sections[sec_code]
                    )
            except KeyError as ex:
                raise InputError(self.filename,
                                 f'Не найден обязательный атрибут в разделе sections: {ex}')

    @staticmethod
    def _parse_elements(rule, condition):
        """
        Вспомогательный метод для получения списка секций/строк/колонок/специфик
        для каждого правила и условия в компендиуме.
        """
        element_map = DotDict()

        # Определение секций для правила/условия
        r_sec, c_sec = None, None
        for element in rule:
            if type(element) == list:
                r_sec = element[0][0]
                break
        # Условие может отсутствовать
        if condition:
            for element in condition:
                if type(element) == list:
                    c_sec = element[0][0]
                    break

        element_map['section'] = r_sec, c_sec
        element_map['rows'] = tuple()
        element_map['cols'] = tuple()
        element_map['specs'] = tuple()

        return element_map

    @staticmethod
    def _create_df_structure(section: Dict[str, Any]) -> Dict[str, Any]:
        """
        Метод для инициализации структуры датафрейма в каждой секции
        при построении компендиума.
        """
        df_struct = DotDict()
        # Формирование структуры датафрейма по xml шаблону
        _section = int(section.code)
        # Словарь строк, ключ - номер строки, значение - массив индексов
        df_struct['rows'] = OrderedDict()
        # Словарь граф, ключ - номер графы, значение - индекс
        df_struct['cols'] = OrderedDict()
        # Двумерный массив специфик
        df_struct['specs'] = []
        # Двумерный массив данных, построчный
        df_struct['data'] = []
        # Вспомогательные массивы для быстрого поиска диапазонов
        df_struct['d_specs'] = [set(), set(), set()]

        # Добавление граф (колонок)
        idx = 0
        for col in section.columns.values():
            # Добавляем числовые колонки
            if col.type == 'Z':
                _col = int(col.code)
                df_struct['cols'][_col] = idx
                idx += 1
        # Список кодов колонок
        df_struct['d_cols'] = list(df_struct['cols'].keys())

        # Добавление строк
        for row in section.rows.values():
            # Пропускаем не предназначенные для ввода строки
            if row.type != 'C':
                _row = int(row.code)
                df_struct['rows'][_row] = list()
        df_struct['d_rows'] = list(df_struct['rows'].keys())

        return df_struct

    @staticmethod
    def _get_title_data(content: etree.ElementTree) -> Dict[str, Tuple[str, Union[str, None]]]:
        """" Метод получения данных раздела metaForm/title. """
        title_items = content.xpath('/metaForm/title//item')
        title = DotDict()

        try:
            for item in title_items:
                item_attribs = (item.attrib['name'], item.get('dic'))
                title[item.attrib['field']] = item_attribs
        except KeyError as ex:
            raise CompendiumAttributeError('item', ex)

        return title

    @staticmethod
    def _get_columns_data(section: etree.ElementTree) -> Dict[str, Dict[str, Any]]:
        """ Метод получения данных о графах в разделе section/columns. """
        columns_items = section.xpath('./columns//column[@type!="B"]')
        columns = DotDict()

        cx = 0
        for col in columns_items:
            # Коды граф, с использованием которых выполняются проверки, должен быть типа uint
            if not col.attrib['code'].isdigit():
                continue

            _col = DotDict(col.items())
            # Индекс графы в векторе col_code
            _col['index'] = cx
            default_cell = col.find('./default-cell')
            if default_cell is not None:
                _col['default-cell'] = DotDict(default_cell.items())

            columns[col.attrib['code']] = _col

            cx += 1

        return columns

    @staticmethod
    def _get_rows_data(section: etree.ElementTree,
                       section_dict: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        """ Метод получения данных о строках в разделе section/rows. Устанавливает значения по умолчанию. """
        rows_items = section.xpath('./rows//row')
        rows = DotDict()

        for row in rows_items:
            _row = DotDict(row.items())

            # cells = row.xpath('./cell')
            # for cell in cells:
            #     # Заполнение значения по умолчанию
            #     if 'default' in cell.keys():
            #         col_code = cell.attrib['column']
            #         cell_default = cell.attrib['default']
            #         section_dict['columns'][col_code]['default'] = cell_default

            if _row.type != 'C':
                rows[row.attrib['code']] = _row

        return rows

    def _get_sections_data(self, content: etree.ElementTree) -> Dict[str, Any]:
        """ Метод получения данных раздела metaForm/sections (список разделов формы). """
        sections_items = content.xpath('/metaForm/sections//section')
        sections = DotDict()

        for section in sections_items:
            _section = DotDict()
            _section['code'] = section.attrib['code']
            _section['name'] = section.attrib['name']

            _section['columns'] = self._get_columns_data(section)
            _section['rows'] = self._get_rows_data(section, _section)

            # Создание структуры для датафрейма
            _section['df_struct'] = self._create_df_structure(_section)

            sections[section.attrib['code']] = _section

        return sections

    def _get_controls_data(self, content: etree.ElementTree) -> List[Dict[str, Any]]:
        """ Метод получения контрольных проверок раздела metaForm/controls. """
        controls_items = content.xpath('/metaForm/controls//control')
        controls = []

        for control in controls_items:
            _control = DotDict(control.items())
            # Нет информации о предыдущем периоде,
            # пропускаем такие проверки
            if '{{' in _control.rule.lower():
                continue

            # Парсинг и сохранение выражения
            ex_rule = _control.rule
            try:
                _rule = self.tokenizer.tokenize_expression(
                    _control.rule.lower(), self.log_expr)
                _control.rule = _rule
            except Exception:
                raise TokenizerError(ex_rule)

            # Парсинг и сохранение условия
            # Могут попадаться условия, содержащие только пробелы
            if _control.condition and not _control.condition.isspace():
                if '{{' in _control.condition.lower():
                    continue
                ex_condition = _control.condition
                try:
                    _condition = self.tokenizer.tokenize_expression(
                        _control.condition.lower(), self.condition)
                    _control.condition = _condition
                except Exception:
                    raise TokenizerError(ex_condition)
            else:
                _control.condition = ''

            # Парсинг и сохранение условия на период
            if _control.periodClause and not _control.periodClause.isspace():
                ex_period = _control.periodClause
                try:
                    _period = self.tokenizer.tokenize_expression(
                        _control.periodClause.lower(), self.period_cond)
                    _control.period = _period
                except Exception:
                    raise TokenizerError(ex_period)
            else:
                _control.period = ''

            # Парсинг и получение списка строк/колонок/специфик
            # для правила и условия
            element_map = self._parse_elements(_control.rule,
                                               _control.condition)
            _control['section'] = element_map.section
            _control['rows'] = element_map.rows
            _control['cols'] = element_map.cols
            _control['specs'] = element_map.specs

            controls.append(_control)

        return controls

    @staticmethod
    def _get_dics_data(content: etree.ElementTree) -> Dict[str, Dict[str, Any]]:
        """ Метод получения данных проверочных словарей раздела metaForm/dics. """
        dics_items = content.xpath('/metaForm/dics//dic')
        dics = DotDict()

        for dic in dics_items:
            _dic = DotDict(dic.items())
            _dic['terms'] = DotDict()
            terms = dic.xpath('.//term')
            for term in terms:
                _dic['terms'][term.attrib['id']] = term.text

            dics[dic.attrib['id']] = _dic

        return dics

    def setup_compendium(self) -> None:
        """
        Сборка комепендиума в памяти. Для каждой проверочной формы:
            - Получение метаинформации из атрибутов корневого элемента metaForm;
            -

        Компендиум имеет следующую структуру:
        {
            "0606010": {  # ОКУД проверочной формы
                'title': {  # Данные раздела 'title'
                    "okpo": (  # Идентификатор поля, атрибут field
                        "Код предприятия": str,  # Атрибут name
                        "s_okpo": Union[str, None]  # Атрибут dic, опциональный
                    )
                },
                'sections': {  # Данные о разделах формы, 'sections'
                    'code': {
                        'code': str,
                        'name': str,
                        'columns': {  # Данные о колонках (графах) формы
                            "11": {  # Код графы
                                'index': int,
                                'type': str,
                                'name': str,
                                ...
                                'default-cell': Union[None, {
                                    'column': str,
                                    'format': str,
                                    'inputType': str
                                }],
                                'default': str  # Значение графы по умолчанию
                            }
                        },
                        'rows': {  # Данные о строках формы
                            "01": {  # Код строки
                                'code': str,
                                'type': str,
                                'name': str,
                                ...

                            }
                        },
                        'df_struct': {  # Словарь стркутуры датафрейма
                            ...
                        }
                    }
                },
                'controls': [  # Список контрольных выражений
                    {
                        'rule': List[str],
                        'condition': List[str],
                        'period': List[str],
                        'section',
                        'rows',
                        'cols',
                        'specs'
                    }
                ],
                'dics': {  # Проверочные словари
                    "s_god": {  # ID словаря
                        'name': str,
                        'id': str,
                        'terms': {  # Словарь определений
                            "2019": "за март",  # Term ID: term text
                            ...
                        }
                    }
                }
            }
        }
        """
        self.compendium = DotDict()

        comp_root = os.path.join(self.root, 'compendium')
        for root, dirs, files in os.walk(comp_root):
            for file in files:
                with open(os.path.join(root, file), 'r') as handler:
                    try:
                        content = etree.fromstring(handler.read(), parser=self.parser)
                    except etree.XMLSyntaxError as ex:
                        raise XmlParseError(file, ex)

                scheme = DotDict()
                # Данные раздела metaForm
                scheme['metaForm'] = DotDict(content.items())

                # Данные раздела title
                scheme['title'] = self._get_title_data(content)

                # Данные раздела sections
                scheme['sections'] = self._get_sections_data(content)

                # Данные раздела controls
                scheme['controls'] = self._get_controls_data(content)

                # Данные справочников
                scheme['dics'] = self._get_dics_data(content)

                _okud = content.get('OKUD')
                _idf = content.get('idf')
                if _okud:
                    self.compendium[f'{_okud}_{int(_idf)}'] = scheme
                else:
                    raise OkudError(self.filename)

    def check_file(self, file: ClassVar[Dict[str, Any]]) -> None:
        self.filename = file.filename
        self.content = file.xml_tree

        file.verify_result = dict()

        file.verify_result['result'] = 'passed'
        file.verify_result['asserts'] = []

        ret_list = []
        try:
            self.process_input(self.filename, self.content)
        except InputError as ex:
            file.verify_result['result'] = 'failed'
            ret_list.append(('', str(ex)))
            self._set_error_struct(ret_list, file)
            return

        schema = self.compendium.get(self.okud)
        if schema is None:
            raise Exception(f'Не найдена проверочная схема для ОКУД {self.okud}')
        # В схеме не содержится проверочных выражений
        if schema.controls is None:
            return

        for control in self.compendium[self.okud].controls:
            # Секция, для которой выполняется проверка
            r_sec = str(control.section[0])
            # Секция заполнена
            if self.frames[r_sec].data.size != 0:
                period_cond = True
                condition = True
                try:
                    if control.period:
                        period_cond = self.period_interpreter\
                            .evaluate_expr(control.period)
                    if control.condition:
                        condition = self.interpreter\
                            .evaluate_expr_cond(control.condition, self.frames)
                    if period_cond and condition:
                        ret = self.interpreter\
                            .evaluate_expr(control.rule, self.frames)
                        # Проверка не выполнена, формируем отчёт с ошибкой
                        if not ret:
                            ret_list.append((control.id, control.name))
                except InterpreterError as ex:
                    file.verify_result['result'] = 'failed'
                    file.verify_result['description'] = ex
                    return

        if len(ret_list):
            self._set_error_struct(ret_list, file)
            file.verify_result['result'] = 'failed'
