from . import *


class SchematronInterpreter(object):
    def __init__(self):
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
            '<': operator.lt,
            '=': operator.eq,
            '>': operator.gt,
            '!=': operator.ne,
            '*': operator.mul,
            'usch:compareDate': self._usch_compare_date
        }

        self._ternary_map = {
            'substring': self._substring_func,
            'usch:iif': self._usch_iif
        }

        self._stack = []
        self._expr = self._create_tokenizer()

    def _push(self, toks):
        self._stack.append(toks[0])
        print('=>', self._stack)

    def _push_int(self, toks):
        self._stack.append(int(toks[0]))
        print('=>', self._stack)

    def _push_not(self, toks):
        for tok in toks:
            if tok == 'not':
                self._stack.append(tok)
        print('=>', self._stack)

    def _create_tokenizer(self):
        general_comp = oneOf('< > = !=')
        bool_and = Literal('and')
        bool_or = Literal('or')
        bool_not = Literal('not')
        lpar, rpar = map(Suppress, '()')
        comma = Suppress(',')

        alphabet = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
        element = (Word(alphabet + alphabet.upper() + nums + '@/:')
                   .setParseAction(self._push))
        integer = Word(nums).setParseAction(self._push_int)

        expr = Forward()
        node = element + ZeroOrMore(('*' + element).setParseAction(self._push))
        parenthesized_node = Group(lpar + node + rpar)
        parenthesized_expr = Group(lpar + expr + rpar)
        count_func = Literal('count') + parenthesized_node
        round_func = Literal('round') + parenthesized_node
        sum_func = Literal('sum') + parenthesized_node
        number_func = Literal('number') + parenthesized_expr
        substring_func = (Literal('substring') +
                          Group(lpar + node + comma + integer +
                                Optional(comma + integer) + rpar))
        usch_filename = Literal('usch:getFileName') + parenthesized_expr
        usch_iif = (Literal('usch:iif') +
                   Group(lpar + expr + comma + expr + comma + expr + rpar))
        usch_compare_date = (Literal('usch:compareDate') +
                             Group(lpar + node + comma + node + rpar))
        funcs = (count_func | round_func | sum_func | number_func | substring_func |
                 usch_filename | usch_iif | usch_compare_date).setParseAction(self._push)
        atom = funcs | node | (Optional(bool_not) + parenthesized_expr).setParseAction(self._push_not)
        factor = atom + ZeroOrMore((general_comp + integer).setParseAction(self._push))
        term = factor + ZeroOrMore((bool_and + factor).setParseAction(self._push))
        expr <<= term + ZeroOrMore((bool_or + term).setParseAction(self._push))
        return expr

    def _evaluate_node(self, node):
        return node

    def _evaluate_stack(self):
        op = self._stack.pop()

        if op in self._nullary_map:
            return self._nullary_map[op]()
        elif op in self._unary_map:
            arg = self._evaluate_stack()
            return self._unary_map[op](arg)
        elif op in self._binary_map:
            arg1 = self._evaluate_stack()
            arg2 = self._evaluate_stack()
            return self._binary_map[op](arg1, arg2)
        elif op in self._ternary_map:
            arg3 = self._evaluate_stack()
            arg2 = self._evaluate_stack()
            arg1 = self._evaluate_stack()
            return self._ternary_map[op](arg1, arg2, arg3)
        elif type(op) == int:
            return op
        else:
            return self._evaluate_node(op)

    # Функции

    def _count_func(self, node):
        return 1

    def _round_func(self, node):
        return round(node)

    def _sum_func(self, node):
        return node

    def _number_func(self, node):
        return node

    def _substring_func(self, node, start, length=0):
        return node[start:start + length] if length else node[start:]

    def _usch_file_name(self):
        return 'FILENAME'

    def _usch_iif(self, cond, true, false):
        return true if cond else false

    def _usch_compare_date(self, first_node, second_node):
        return (first_node == second_node)

    # Публичное API

    def tokenize(self, text):
        self._stack = []
        return self._expr.parseString(text).asList()

    def parse(self, text):
        self.tokenize(text)
        return self._evaluate_stack()
