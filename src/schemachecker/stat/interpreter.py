import operator
from copy import deepcopy
from ._dataframe import DataFrame
from .exceptions import InterpreterError, EmptyExtract


class PeriodInterpreter:
    """ Интерпретатор выражений на период. """
    def __init__(self, period=None):
        self.unary_map = {
            '<':    operator.lt,
            '<=':   operator.le,
            '=':    operator.eq,
            '>':    operator.gt,
            '>=':   operator.ge,
            '<>':   operator.ne,
            'and':  operator.and_,
            'or':   operator.or_
        }

        self.stack = []
        self._period = period

    def _evaluate_stack(self):
        op = self.stack.pop()

        if op in self.unary_map:
            arg2 = self._evaluate_stack()
            arg1 = self._evaluate_stack()
            return self.unary_map[op](arg1, arg2)

        # Код периода
        elif op.isdigit():
            return op

        elif op == '&np':
            return self.period

        # Список периодов, "in"
        else:
            args = []
            arg = self.stack[-1]
            while arg != '&np':
                arg = self._evaluate_stack()
                args.append(arg)
                arg = self.stack[-1]
            return self.period in args

    @property
    def period(self):
        return self._period

    @period.setter
    def period(self, value):
        self._period = value

    def evaluate_expr(self, expr):
        self.stack = deepcopy(expr)
        try:
            return self._evaluate_stack()
        except Exception:
            raise InterpreterError(expr)


class Interpreter:
    """ Общий интерпретатор условных выражений. """
    def __init__(self):
        # self.func_map = {
        #     'sum':      self._sum,
        #     'abs':      self._abs,
        #     'floor':    self._floor,
        #     'isnull':   self._isnull,
        #     'nullif':   self._nullif,
        #     'round':    self._round,
        #     'coalesce': self._coalesce
        # }

        self.unary_map = {
            'sum':      self._sum,
            'abs':      self._abs,
            'floor':    self._floor
        }

        self.bool_map = {
            '|<|':      operator.lt,
            '|<=|':     operator.le,
            '|=|':      operator.eq,
            '|>|':      operator.gt,
            '|>=|':     operator.ge,
            '|<>|':     operator.ne,
        }

        self.binary_map = {
            'isnull':   self._isnull,
            'nullif':   self._nullif,
            '+':        operator.add,
            '-':        operator.sub,
            '*':        operator.mul,
            '/':        operator.truediv,
            'and':      operator.and_,
            'or':       operator.or_
        }

        self.ternary_map = {
            'round':    self._round
        }

        self.varargs_map = {
            'coalesce': self._coalesce
        }

        # Стек выражения
        self.stack = []
        # Флаг условного выражения
        self.condition = False
        # Флаги тернарного сравнения
        self.ternary = False
        self.first_op = False
        # Набор датафреймов
        self.frame_map = None
        # Датафрейм, из которого производятся выборки
        # при интерпретации выражений
        self.frame = None

    # Унарные функции
    # Метод для определения контекста функции SUM
    @staticmethod
    def _evaluate_context(arg1, arg2, con_type: int):
        """
        Метод вычисляет значения конекстно-зависимых функций (например, SUM).
        :param arg1:
        :param arg2:
        :param con_type: тип контекста:
            0 - первый аргумент - функция;
            1 - второй аргумент - функция;
            2 - оба аргумента - функции.
        :return:
        """
        if con_type == 2:
            return arg1(axis=2), arg2(axis=2)

        # Второй аргумент - скаляр
        if type(arg2) != DataFrame:
            return arg1(axis=2), arg2

        # Второй аргумент - датафрейм
        dim = arg2.dim()
        if dim[0] == 1 and dim[1] == 1:
            return arg1(axis=2), arg2.get_scalar()
        elif dim[0] == 1:
            return arg1(axis=0), arg2
        elif dim[1] == 1:
            return arg1(axis=1), arg2
        # Кто-то накосячил в шаблоне, такую операцию нельзя выполнить
        else:
            # TODO: raise custom exception
            print(arg1, arg2)
            raise Exception('Ошибка при работе с контекстом функции SUM')

    @staticmethod
    def _check_context(arg1, arg2):
        """ Метод проверяет, не вернулся ли promise в качестве аргумента. """
        if type(arg1).__name__ == 'method' and type(arg2).__name__ == 'method':
            con_type = 2
        elif type(arg1).__name__ == 'method':
            con_type = 0
        elif type(arg2).__name__ == 'method':
            con_type = 1
            arg1, arg2 = arg2, arg1
        else:
            return arg1, arg2

        arg1, arg2 = Interpreter._evaluate_context(arg1, arg2, con_type)
        if con_type == 1:
            return arg2, arg1

        return arg1, arg2

    @staticmethod
    def _sum(element):
        # Аргумент - скаляр
        if type(element) != DataFrame:
            return element
        return element.sum

    @staticmethod
    def _abs(element):
        return element.abs()

    @staticmethod
    def _floor(element):
        return element.floor()

    # Бинарные функции
    @staticmethod
    def _isnull(element, substitution):
        return element.fill_none(filler=substitution)

    @staticmethod
    def _nullif(element1, element2):
        #TODO: check if NoneType is multiplied by something. WTF?
        return None if element1 == element2 else True

    # Тернарные функции
    @staticmethod
    def _round(element, precision, op_type=0):
        if type(element) != DataFrame:
            return round(element, precision)
        return element.round(precision, op_type)

    # Функции с переменным числом аргументов
    @staticmethod
    def _coalesce():
        pass

    def _evaluate_stack(self):
        op = self.stack.pop()

        # Вытащили элемент
        if type(op) == list:
            # Определяем ключ в frame_map
            key = str(op[0][0])
            self.frame = self.frame_map[key]
            # Первый элемент - номер секции, пропускаем
            return self.frame.get(*op[1:])

        elif op in self.unary_map:
            arg = self._evaluate_stack()
            return self.unary_map[op](arg)

        elif op in self.bool_map:
            # Условное выражение, отсутствуют тернарные сравнения
            if self.condition:
                arg2 = self._evaluate_stack()
                arg1 = self._evaluate_stack()
                arg1, arg2 = self._check_context(arg1, arg2)
                return self.bool_map[op](arg1, arg2)
            self.first_op = ~self.first_op
            if not self.first_op:
                self.ternary = True
            arg2 = self._evaluate_stack()
            arg1 = self._evaluate_stack()
            arg1, arg2 = self._check_context(arg1, arg2)
            print(arg1, arg2)
            # Первый логический оператор, тернарное сравнение
            if self.first_op and self.ternary:
                # Восстанавливаем флаги
                self.first_op = False
                self.ternary = False
                return arg1[0] and self.bool_map[op](arg1[1], arg2)
            # Первый логический оператор, нет тернарного сравнения
            elif self.first_op and not self.ternary:
                self.first_op = False
                return self.bool_map[op](arg1, arg2)
            # Второй логический оператор тернарного сравнения
            else:
                self.first_op = ~self.first_op
                return self.bool_map[op](arg1, arg2), arg2

        elif op in self.binary_map:
            arg2 = self._evaluate_stack()
            arg1 = self._evaluate_stack()
            arg1, arg2 = self._check_context(arg1, arg2)
            return self.binary_map[op](arg1, arg2)

        elif op in self.ternary_map:
            args = []
            arg = self.stack[-1]
            while type(arg) != DataFrame:
                arg = self._evaluate_stack()
                args.append(arg)
            return self.ternary_map[op](*args[::-1])

        elif op in self.varargs_map:
            #TODO: vararg functions
            args = []

        # Просто число
        else:
            return op

    def _empty_extract_recover(self) -> None:
        """ Метод восстанавливает флаги после получения пустой выборки. """
        self.ternary = False
        self.first_op = False

    def evaluate_expr(self, expr, frame_map) -> bool:
        """ Вычисление контрольного выражения. """
        print('-'*80)
        self.frame_map = frame_map
        self.stack = deepcopy(expr)
        try:
            return self._evaluate_stack()
        except EmptyExtract:
            self._empty_extract_recover()
            return True
        except Exception:
            raise InterpreterError(expr)

    def evaluate_expr_cond(self, expr, frame_map) -> bool:
        """ Вычисление условного выражения. """
        self.condition = True
        self.frame_map = frame_map
        self.stack = deepcopy(expr)
        try:
            return self._evaluate_stack()
        except EmptyExtract:
            return False
        except Exception:
            raise InterpreterError(expr)
        finally:
            self.condition = False
