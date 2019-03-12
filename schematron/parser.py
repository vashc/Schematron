import operator
from functools import wraps

from .tokenizer import tokenize


def _cached_node(func):
    # Кэшируется только один элемент - узел
    @wraps(func)
    def _inner(*args, **kwargs):
        self = args[0]
        element = f'//{self._context}/{args[1]}'
        if element not in self._cache:
            self._cache[element] = func(*args, **kwargs)
        return self._cache[element]

    return _inner


def _cached_func(func):
    @wraps(func)
    def _inner(*args, **kwargs):
        self = args[0]
        # Нужен контекст, могут быть идентичные сигнатуры
        # в разных контекстах
        element = f'{func.__name__}/{self._context}/{args[1:]}'
        if element not in self._cache:
            self._cache[element] = func(*args, **kwargs)
        return self._cache[element]

    return _inner


# @_cached_node
def _evaluate_node(node, assertion):
    element = f'//{assertion["context"]}/{node}'
    value = assertion['xml_content'].xpath(element)
    # if not value:
        # Элемент/атрибут не найден
        # raise Exception(f'Элемент/атрибут {self._context}/{node} в '
        #                 f'файле {self._xml_file} не найден')
    if '@' in node:
        #TODO: Что делать с ненайденными атрибутами?
        if not value:
            return ''
        # Работаем с атрибутом, возвращаем значение
        value = value[0]
        return value
    else:
        # Работаем с элементом, возвращаем наличие
        return True if value else False


def _evaluate_stack(stack, assertion):
    op = stack.pop()

    if op in _nullary_map:
        return _nullary_map[op](assertion)
    elif op in _unary_map:
        if op == 'count':
            arg = stack.pop()
        else:
            arg = _evaluate_stack(stack, assertion)
        return _unary_map[op](arg, assertion)
    elif op in _binary_map:
        arg1 = _evaluate_stack(stack, assertion)
        arg2 = _evaluate_stack(stack, assertion)
        return _binary_map[op](arg2, arg1)
    elif op in _ternary_map:
        arg3 = _evaluate_stack(stack, assertion)
        arg2 = _evaluate_stack(stack, assertion)
        if op == 'substring' and not arg2.isdigit():
            # Получили substring без опционального третьего аргумента
            return _ternary_map[op](arg2, arg3)
        arg1 = _evaluate_stack(stack, assertion)
        return _ternary_map[op](arg1, arg2, arg3)
    elif op.isdigit():
        # Возвращаем найденное число
        return op
    elif op.startswith('\''):
        # Возвращаем строку без кавычек
        return op[1:-1]
    else:
        node = _evaluate_node(op, assertion)
        return node


# Обёртки для некоторых арифметических операций
def _op_not(a, assertion):
    return operator.not_(a)

def _op_mul(a, b):
    try:
        mul = float(a) * float(b)
        return mul
    #TODO: Ошибка приведения к типу
    except Exception as ex:
        raise Exception(f'Ошибка при приведении к целому типу: {ex}')


def _op_sub(a, b):
    try:
        mul = float(a) - float(b)
        return mul
    # TODO: Ошибка приведения к типу
    except Exception as ex:
        raise Exception(f'Ошибка при приведении к целому типу: {ex}')


def _count_func(node, assertion):
    # if '@' in node:
        # return str(len(self._xml_content.xpath(f'.//{self._context}/*[{node}]')))
    # return str(len(self._xml_content.findall(f'//{self._context}/{node}')))
    return str(len(assertion['xml_content']
                   .xpath(f'//{assertion["context"]}/{node}')))


# @_cached_func
def _round_func(node, assertion):
    return round(node)


# @_cached_func
def _sum_func(node, assertion):
    return node


# @_cached_func
def _number_func(node, assertion):
    return node


# @_cached_func
def _substring_func(node, start, length='0'):
    start, length = int(start) - 1, int(length)
    return node[start:start + int(length)] if length else node[start:]


def _usch_file_name(assertion):
    return ''.join(assertion['xml_file'].split('.')[:-1])


def _usch_iif(cond, true, false):
    return true if cond else false


def _usch_compare_date(first_node, second_node):
    return (first_node == second_node)


_nullary_map = {
    'usch:getFileName': _usch_file_name
}

_unary_map = {
    'not':      _op_not,
    'count':    _count_func,
    'round':    _round_func,
    'sum':      _sum_func,
    'number':   _number_func
}

_binary_map = {
    '<':                operator.lt,
    '<=':               operator.le,
    '=':                operator.eq,
    '>':                operator.gt,
    '>=':               operator.ge,
    '!=':               operator.ne,
    '*':                _op_mul,
    '-':                _op_sub,
    'and':              operator.and_,
    'or':               operator.or_,
    'usch:compareDate': _usch_compare_date
}

_ternary_map = {
    'substring': _substring_func,
    'usch:iif': _usch_iif
}


def parse(assertion):
    _stack = []
    _expression = assertion['assert']
    _context = assertion['context']
    print(_expression)
    tokenize(_expression, _stack)

    try:
        parsing_result = _evaluate_stack(_stack, assertion)
        return parsing_result
    except Exception as ex:
        print(f'\u001b[35mError. {ex}.\u001b[0m')
