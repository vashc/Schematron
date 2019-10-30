import numpy as np
import operator
import warnings
# noinspection PyUnresolvedReferences
from lxml import etree
from collections import defaultdict
from pprint import pformat
from typing import Dict, List, Any, Union, Tuple, Set
from .exceptions import EmptyExtract


class DataFrame:
    """
    Класс датафрейма для хранения и обработки таблиц из .xml документов.
    Структура датафрейма:
        data: np.array((nrows, ncols)) - таблица пользовательских данных. ncols - число граф в соответствующей
            секции проверочной схемы, nrows - число строк в соответствующей секции пользовательского файла.
        specs: np.array((nrows, 3)) - таблица специфик. s1 - 0 столбец, s2 - 1 столбец, s3 - 2 столбец.
        row_codes: np.array(nrows) - вектор кодов строк в соответствии с проверочным файлом.
        col_codes: np.array(ncols) - вектор кодов граф в соответствии с проверочным файлом.
        #col_map: Dict[str, int] - отображение вида {код_графы: индекс_графы_в_col_codes}.

                    col_codes   [][][][]
        s1s2s3      row_codes
        [][][]      []          [][][][]
        [][][]      []          [ data ]
        [][][]      []          [][][][]
    """
    warnings.simplefilter(action='ignore', category=FutureWarning)
    op_map = {
        '+': operator.add,
        '-': operator.sub,
        '*': operator.mul,
        '/': operator.truediv,
        '<': operator.lt,
        '<=': operator.le,
        '=': operator.eq,
        '>': operator.gt,
        '>=': operator.ge,
        '!=': operator.ne,
    }

    def __init__(self, *,
                 data: np.array,
                 specs: np.array,
                 row_codes: np.array,
                 col_codes: np.array,
                 irows: Dict[int, Set[int]],
                 ispecs) -> None:
        self.data = data
        self.specs = specs
        self.row_codes = row_codes
        self.col_codes = col_codes
        self.irows = irows
        self.ispecs = ispecs

        # Словарь для сторокового представления внутренней структуры
        self.struct = {
            'data':         self.data,
            'specs':        self.specs,
            'row_codes':    self.row_codes,
            'col_codes':    self.col_codes,
            'irows':        self.irows,
            'ispecs':       self.ispecs
        }

    def __str__(self):
        return pformat(self.struct)

    def __add__(self, other):
        return self._op(other, '+')

    def __sub__(self, other):
        return self._op(other, '-')

    def __mul__(self, other):
        return self._op(other, '*')

    def __truediv__(self, other):
        return self._op(other, '/')

    def __gt__(self, other):
        return self._boolop(other, '>')

    def __ge__(self, other):
        return self._boolop(other, '>=')

    def __lt__(self, other):
        return self._boolop(other, '<')

    def __le__(self, other):
        return self._boolop(other, '<=')

    def __eq__(self, other):
        return self._boolop(other, '=')

    def __ne__(self, other):
        return self._boolop(other, '!=')

    def _baseop(self, other: Union['DataFrame', float], op: str) -> Union[np.array, bool]:
        """ Базовый метод для выполнения бинарных операций. """
        try:
            if type(other) == DataFrame:
                res = DataFrame.op_map[op](self.data, other.data)
            else:
                res = DataFrame.op_map[op](self.data, other)
        except Exception as ex:
            raise Exception(f'Невалидный аргумент для операции {op}: {other}. Ошибка: {ex}')

        return res

    def _op(self, other: Union['DataFrame', float], op: str) -> 'DataFrame':
        """ Вспомогательный метод для перегрузки операторов. """
        data = self._baseop(other, op)

        return DataFrame(data=data,
                         specs=self.specs,
                         row_codes=self.row_codes,
                         col_codes=self.col_codes,
                         irows=self.irows,
                         ispecs=self.ispecs)

    def _boolop(self, other: Union['DataFrame', float], op: str) -> bool:
        """ Вспомогательный метод для перегрузки булевых операций. """
        mask = self._baseop(other, op)

        if type(mask) == bool:
            return mask
        return mask.all()

    @staticmethod
    def from_file_content(xml_section: etree.ElementTree, scheme_section: Dict[str, Any]) -> 'DataFrame':
        """
        Инициализирует датафрейм по содержимому пользовательского файла.
        :param xml_section: соответствующая секция в пользовательском файле.
        :param scheme_section: соответствующая секция в объекте компендиума.
        :return: инициализированный экземпляр DataFrame.
        """
        # Вспомогательный словарь для быстрого поиска диапазонов индексов
        irows: Dict[int, Set[int]] = defaultdict(set)
        ispecs = [defaultdict(set), defaultdict(set), defaultdict(set)]

        # Отображение колонок-специфик на индексы в массиве specs
        spec_map = {'s1': 0, 's2': 1, 's3': 2}
        scheme_columns = scheme_section['columns']

        rows = xml_section.xpath('.//row')
        nrows = len(rows)
        ncols = len(scheme_columns.keys())

        # Векторы строк и колонок
        row_codes = np.zeros(nrows, dtype=np.uint)
        col_codes = np.zeros(ncols, dtype=np.uint)
        for code, item in scheme_section['columns'].items():
            index = item['index']
            col_codes[index] = code

        # Основная таблица данных для валидации
        data = np.zeros((nrows, ncols))
        # Таблица специфик
        specs = np.full((nrows, 3), np.NaN, dtype='U')

        for rx, row in enumerate(rows):
            row_code = int(row.attrib['code'])
            row_codes[rx] = row_code
            irows[row_code].add(rx)

            # Заполенение массива специфик
            for attrib, value in row.attrib.items():
                if attrib != 'code':
                    sx = spec_map.get(attrib)
                    if sx is None:
                        # TODO: internal exceptions
                        raise Exception('Неверный атрибут элемента "row"')
                    specs[rx, sx] = value.lower()
                    ispecs[sx][value.lower()].add(rx)

            # Заполнение основного массива данных data
            cols = row.xpath('.//col')
            for col in cols:
                cx = scheme_columns[col.attrib['code']]['index']
                data[rx, cx] = float(col.text) if col.text else 0

        return DataFrame(data=data,
                         specs=specs,
                         row_codes=row_codes,
                         col_codes=col_codes,
                         irows=irows,
                         ispecs=ispecs)

    def _filter_rows_indices(self, rows: List[Union[int, str]]) -> Set[int]:
        """ Метод возвращает индексы строк, удовлетворяющих условиям. """
        row_indices = set()
        while rows:
            token = rows.pop()
            # Все строки
            if token == '*':
                row_indices.update(range(len(self.row_codes)))
            # Диапазон строк
            elif token == '-':
                max_r = rows.pop()
                min_r = rows.pop()
                indices = np.where((self.row_codes > min_r) & (self.row_codes < max_r))[0]
                row_indices.update(indices.flat)
            # Отдельная строка
            elif type(token) == int:
                if token not in self.irows.keys():
                    continue
                row_indices.update(self.irows[token])

        return row_indices

    def _filter_specs_indices(self, row_indices: Set[int], *specs: List[str]) -> Set[int]:
        """ Метод фильтрует переданные индексы строк, для которых удовлетворены условия по спецификам. """
        if any(specs):
            spec_indices = [set() for _ in range(len(specs))]
            for sx, spec in enumerate(specs):
                while spec:
                    token = spec.pop()
                    if token == '*':
                        spec_indices[sx].update(row_indices)
                    elif token == '-':
                        max_s = spec.pop()
                        min_s = spec.pop()
                        indices = np.where((self.specs[sx] > min_s) & (self.specs[sx] < max_s))[0]
                        spec_indices[sx].update(indices)
                    else:
                        if token not in self.ispecs[sx].keys():
                            continue
                        spec_indices[sx].update(self.ispecs[sx][token])
        else:
            return row_indices

        return row_indices.intersection(*spec_indices)

    def _filter_cols_indices(self, cols: List[Union[int, str]]) -> Set[int]:
        """ Метод возвращает индексы граф, удовлетворяющих условиям. """
        col_indices = set()
        while cols:
            token = cols.pop()
            if token == '*':
                col_indices.update(range(len(self.col_codes)))
            elif token == '-':
                max_c = cols.pop()
                min_c = cols.pop()
                indices = np.where((self.col_codes > min_c) & (self.col_codes < max_c))[0]
                col_indices.update(indices.flat)
            elif type(token) == int:
                col_indices.update(np.where(self.col_codes == token)[0].flat)

        return col_indices

    def get(self, rows: List[str], cols: List[str], *specs: List[str]) -> 'DataFrame':
        """
        Метод для получения выборки из датафрейма по заданным спискам строк, граф и специфик.
        :param rows: список строк.
        :param cols: список граф.
        :param specs: списки специфик. Может содержать от 0 до 3 списков:
            0 - специфики отсутствуют (одиночные строки типа 'F');
            1-3 - соответствующие списки специфик s1-s3.
        :return: отфильтрованный объект DataFrame.
        """
        row_indices = self._filter_rows_indices(rows)
        row_indices = self._filter_specs_indices(row_indices, *specs)
        row_indices = list(row_indices)

        col_indices = self._filter_cols_indices(cols)
        col_indices = list(col_indices)

        # Фильтруем массив data по векторам row_codes/col_codes
        grid = np.ix_(row_indices, col_indices)

        data = self.data[grid]
        if data.size == 0:
            raise EmptyExtract()

        return DataFrame(data=data,
                         specs=self.specs[row_indices, :],
                         row_codes=self.row_codes[row_indices],
                         col_codes=self.col_codes[col_indices],
                         irows=self.irows,
                         ispecs=self.ispecs)

    def dim(self) -> Tuple[int, int]:
        """ Метод для определения размерности массива данных self.data. """
        return self.data.shape

    def get_scalar(self) -> float:
        """ Метод возвращает единственный элемент массива data. """
        return self.data[0, 0]

    def sum(self, *, axis: int=0) -> Union['DataFrame', float]:
        """
        Выполняет сложение элементов таблицы section вдоль оси axis:
            0 - по столбцам,
            1 - по строкам,
            2 - во всей таблице.
        Возвращает объект DataFrame, если сложение производится по строкам/столбцам,
        или скаляр, если сложение производится по всему датафрейму.
        """
        if axis == 2:
            return np.nansum(self.data)

        data = np.nansum(self.data, axis=axis, keepdims=True)
        return DataFrame(data=data,
                         specs=self.specs,
                         row_codes=self.row_codes,
                         col_codes=self.col_codes,
                         irows=self.irows,
                         ispecs=self.ispecs)

    def fill_none(self, *, filler: float=0.0) -> 'DataFrame':
        """ Заменяет все отсутствующие (None) элементы внутреннего массива data на filler. """
        data = np.nan_to_num(self.data)
        return DataFrame(data=data,
                         specs=self.specs,
                         row_codes=self.row_codes,
                         col_codes=self.col_codes,
                         irows=self.irows,
                         ispecs=self.ispecs)

    def is_none(self) -> bool:
        """ Определяет, содержатся ли незаполненные (None) элементы в датафрейме. """
        return np.isnan(self.data).all()

    def abs(self) -> 'DataFrame':
        """ Возвращает новый датафрейм с абсолютными значениями элементов. """
        data = np.abs(self.data)
        return DataFrame(data=data,
                         specs=self.specs,
                         row_codes=self.row_codes,
                         col_codes=self.col_codes,
                         irows=self.irows,
                         ispecs=self.ispecs)

    def round(self, precision, op_type=0):
        """
        Возвращает новый датафрейм со значениями элементов,
        округлённых до указанной длины и точности
        precision - точность округления;
        op_type - тип операции.
        """
        data = self.data.copy()
        dim = self.dim()

        for rx in range(dim[0]):
            for cx in range(dim[1]):
                if self.data[rx][cx]:
                    data[rx][cx] = round(self.data[rx][cx], precision)
        return DataFrame(data=data,
                         specs=self.specs,
                         row_codes=self.row_codes,
                         col_codes=self.col_codes,
                         irows=self.irows,
                         ispecs=self.ispecs)

    def floor(self) -> np.ndarray:
        """ Возвращает наибольшее число, меньшее или равное наименьшему элементу в датафрейме. """
        return np.nanmin(self.data)
