from pyparsing import Literal, Suppress, Word, Combine, Optional, Forward,\
    ZeroOrMore, Group, nums, srange, oneOf
from typing import List


class Tokenizer:
    def __init__(self) -> None:
        self.stack = []

    def _push(self, toks: List[str]) -> None:
        self.stack.append(toks[0])

    def _push_not(self, toks: List[str]) -> None:
        for tok in toks:
            if tok == 'not':
                self.stack.append(tok)

    def create_tokenizer(self) -> Forward:
        general_comp = oneOf('< > = != <= >=')
        bool_and = Literal('and')
        bool_or = Literal('or')
        bool_not = Literal('not')
        lpar, rpar = map(Suppress, '()')
        tick = Literal("'")
        minus = Literal('-')
        plus = Literal('+')
        mul = Literal('*')
        mod = Literal('mod')
        comma = Suppress(',')

        alphabet = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
        element = (Word(alphabet + alphabet.upper() + nums + '@/:._- ')
                   .setParseAction(self._push))
        integer = Word(nums).setParseAction(self._push)
        floats = (Combine(Word(nums) + '.' + Optional(Word(nums)))
                  .setParseAction(self._push))
        string = Word(alphabet + alphabet.upper() +
                      nums + srange('[a-zA-Z]' + '.'))
        quoted_string = Combine(tick + string + tick).setParseAction(self._push)
        date = Combine(tick + Word(nums, exact=2) + '.' +
                       Word(nums, exact=2) + '.' + Word(nums, exact=4) +
                       tick).setParseAction(self._push)

        expr = Forward()
        node = element + ZeroOrMore(((mul | minus) + element)
                                    .setParseAction(self._push))
        parenthesized_node = Group(lpar + node + rpar)
        node_or_parnode = node | parenthesized_node
        # Переменное (не менее двух) количество узлов, разделённых запятыми
        variadic_node = Group(node_or_parnode + comma + node_or_parnode +
                              ZeroOrMore(comma + node_or_parnode))
        parenthesized_expr = Group(lpar + expr + rpar)

        count_func = Literal('count') + parenthesized_node
        round_func = Literal('round') + parenthesized_node
        sum_func = Literal('sum') + parenthesized_node
        number_func = Literal('number') + parenthesized_expr
        substring_func = (Literal('substring') +
                          Group(lpar + node + comma + integer +
                                Optional(comma + integer) + rpar))
        concat_func = (Literal('concat') + Group(lpar + variadic_node + rpar))
        usch_filename = Literal('usch:getFileName') + Group(lpar + rpar)
        usch_iif = (Literal('usch:iif') +
                    Group(lpar + expr + comma + expr + comma + expr + rpar))
        usch_compare_date = (Literal('usch:compareDate') +
                             Group(lpar + node + comma + node + rpar))
        funcs = (count_func | round_func | sum_func | number_func |
                 substring_func | concat_func | usch_filename |
                 usch_iif | usch_compare_date).setParseAction(self._push)

        atom = (funcs | node | (Optional(bool_not) + parenthesized_expr)
                .setParseAction(self._push_not))
        left_expr = atom + ZeroOrMore(((mul | minus | plus | mod) + atom)
                                      .setParseAction(self._push))

        factor = (left_expr +
                  ZeroOrMore((general_comp +
                              (floats | integer | atom | quoted_string | date))
                             .setParseAction(self._push)))
        term = factor + ZeroOrMore((bool_and + factor)
                                   .setParseAction(self._push))
        expr <<= term + ZeroOrMore((bool_or + term).setParseAction(self._push))
        return expr

    def tokenize_expression(self, expr: str, parser: Forward) -> List[str]:
        self.stack = []
        parser.parseString(expr)

        return self.stack
