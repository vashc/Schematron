import operator
from typing import List, Union
from functools import wraps
from copy import deepcopy
from lxml import etree
from .exceptions import ParserError, TypeConvError, NodeAttributeError


class Interpreter:
    def __init__(self) -> None:
        # Символы, которые могут содержаться в элементе узла
        self._alphabet = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
        self._nums = '1234567890'
        self._special_symbols = '@/:._-'
        self._element = set(self._alphabet + self._alphabet.upper() +
                            self._nums + self._special_symbols)

        self._nullary_map = {
            'usch:getFileName': self._usch_file_name
        }

        self._unary_map = {
            'not': operator.not_,
            'count': self._count_func,
            'round': self._round_func,
            'sum': self._sum_func,
            'number': self._number_func
        }

        self._binary_map = {
            '<': self._op_lt,
            '<=': operator.le,
            '=': operator.eq,
            '>': operator.gt,
            '>=': operator.ge,
            '!=': operator.ne,
            '*': self._op_mul,
            '-': self._op_sub,
            '+': self._op_add,
            'mod': self._op_mod,
            'and': operator.and_,
            'or': operator.or_,
            'usch:compareDate': self._usch_compare_date
        }

        self._ternary_map = {
            'substring': self._substring_func,
            'usch:iif': self._usch_iif
        }

        self._varargs_map = {
            'concat': self._concat_func
        }

        self.stack = []

        self.xml_content = None
        self.context = None
        self.xml_file = None

        self.cache = dict()

    # TODO: LRU caching
    def _cached_node(func):
        # Кэшируется только один элемент - узел
        @wraps(func)
        def _inner(*args, **kwargs):
            self = args[0]
            element = f'//{self.context}/{args[1]}'
            if element not in self.cache:
                self.cache[element] = func(*args, **kwargs)
            return self.cache[element]

        return _inner

    def _cached_func(func):
        @wraps(func)
        def _inner(*args, **kwargs):
            self = args[0]
            # Нужен контекст, могут быть идентичные сигнатуры
            # в разных контекстах
            element = f'{func.__name__}/{self.context}/{args[1:]}'
            if element not in self.cache:
                self.cache[element] = func(*args, **kwargs)
            return self.cache[element]

        return _inner

    @_cached_node
    def _evaluate_node(self, node):
        element = f'//{self.context}/{node}'
        value = self.xml_content.xpath(element)
        if '@' in node:
            # Работаем с атрибутом, возвращаем значение
            if not value:
                # Элемент/атрибут не найден
                raise NodeAttributeError(self.context,
                                         node, self.xml_file)
            value = value[0]
            return value
        else:
            # Работаем с элементом, возвращаем наличие
            return True if value else False

    def _evaluate_stack(self) -> Union[str, int, Exception]:
        op = self.stack.pop()

        if op in self._nullary_map:
            return self._nullary_map[op]()
        elif op in self._unary_map:
            if op == 'count':
                arg = self.stack.pop()
            else:
                arg = self._evaluate_stack()
            return self._unary_map[op](arg)
        elif op in self._binary_map:
            arg2 = self._evaluate_stack()
            arg1 = self._evaluate_stack()
            return self._binary_map[op](arg1, arg2)
        elif op in self._ternary_map:
            arg3 = self._evaluate_stack()
            arg2 = self._evaluate_stack()
            if op == 'substring' and not arg2.isdigit():
                # Получили substring без опционального третьего аргумента
                return self._ternary_map[op](arg2, arg3)
            arg1 = self._evaluate_stack()
            return self._ternary_map[op](arg1, arg2, arg3)
        elif op in self._varargs_map:
            args = []
            # Смотрим на вершину стека, если он не пуст
            if len(self.stack):
                arg = self.stack[-1]
                while set(arg) <= self._element:
                    # Если элемент узла - можно вычислять значение
                    arg = self._evaluate_stack()
                    args.append(arg)
                    arg = self.stack[-1]
            # Разворачиваем аргументы, приходят из стека в обратном порядке
            return self._varargs_map[op](*args[::-1])
        elif op.isdigit():
            # Возвращаем найденное число
            return op
        elif op.startswith('\''):
            # Возвращаем строку без кавычек
            return op[1:-1]
        else:
            node = self._evaluate_node(op)
            return node

    # Обёртки для некоторых арифметических операций
    def _float_converter(func):
        @wraps(func)
        def _inner(*args, **kwargs):
            inner_args = [*args]
            try:
                inner_args[1:] = map(float, args[1:])
            except ValueError as ex:
                raise TypeConvError(inner_args[1:], ex)
            return func(*inner_args, **kwargs)

        return _inner

    @staticmethod
    def _op_lt(a, b):
        return str(a) < str(b)

    @_float_converter
    def _op_mul(self, a, b):
        val = a * b
        return val

    @_float_converter
    def _op_sub(self, a, b):
        val = a - b
        return val

    @_float_converter
    def _op_add(self, a, b):
        val = a + b
        return val

    @_float_converter
    def _op_mod(self, a, b):
        val = a % b
        return val

    @_cached_func
    def _count_func(self, node):
        return str(len(self.xml_content
                       .xpath(f'//{self.context}/{node}')))

    @_cached_func
    def _round_func(self, node):
        return round(node)

    @_cached_func
    def _sum_func(self, node):
        return node

    @_cached_func
    def _number_func(self, node):
        return node

    @_cached_func
    def _substring_func(self, node, start, length='0'):
        start, length = int(start) - 1, int(length)
        return node[start:start + int(length)] if length else node[start:]

    @staticmethod
    def _concat_func(*args):
        return ''.join(args)

    def _usch_file_name(self):
        return '.'.join(self.xml_file.split('.')[:-1])

    @staticmethod
    def _usch_iif(cond, true, false):
        return true if cond else false

    @staticmethod
    def _usch_compare_date(first_node, second_node):
        return first_node == second_node

    def evaluate_expr(self, expr: List[str],
                      xml_content: etree.ElementTree,
                      xml_file: str,
                      context: str) -> Union[Exception]:
        # Очитка кэша
        self.cache = dict()

        self.xml_content = xml_content
        self.xml_file = xml_file
        self.context = context
        self.stack = deepcopy(expr)

        try:
            return self._evaluate_stack()
        except Exception as ex:
            raise ParserError(expr, self.xml_file, ex)
