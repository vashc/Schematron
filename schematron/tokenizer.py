from pyparsing import Literal, Suppress, Forward, Word, \
    Group, ZeroOrMore, Optional, oneOf, nums, srange, Combine


def _push(stack, tok):
    stack.append(tok)
    # print('=>', self._stack)


def _push_not(stack, toks):
    for tok in toks:
        if tok == 'not':
            stack.append(tok)
    # print('=>', self._stack)


def _create_tokenizer(stack):
    general_comp = oneOf('< > = != <= >=')
    bool_and = Literal('and')
    bool_or = Literal('or')
    bool_not = Literal('not')
    lpar, rpar = map(Suppress, '()')
    tick = Literal("'")
    minus = Literal('-')
    mul = Literal('*')
    comma = Suppress(',')

    alphabet = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
    element = (Word(alphabet + alphabet.upper() + nums + '@/:._-')
               .setParseAction(lambda toks: _push(stack, toks[0])))
    integer = Word(nums).setParseAction(lambda toks: _push(stack, toks[0]))
    string = Word(alphabet + alphabet.upper() +
                  nums + srange('[a-zA-Z]' + '.'))
    quoted_string = Combine(tick + string + tick)\
        .setParseAction(lambda toks: _push(stack, toks[0]))
    date = Combine(tick + Word(nums, exact=2) + '.' +
                   Word(nums, exact=2) + '.' + Word(nums, exact=4) +
                   tick).setParseAction(lambda toks: _push(stack, toks[0]))

    expr = Forward()
    node = element + ZeroOrMore(((mul | minus) + element)
                                .setParseAction(lambda toks: _push(stack, toks[0])))
    parenthesized_node = Group(lpar + node + rpar)
    parenthesized_expr = Group(lpar + expr + rpar)

    count_func = Literal('count') + parenthesized_node
    round_func = Literal('round') + parenthesized_node
    sum_func = Literal('sum') + parenthesized_node
    number_func = Literal('number') + parenthesized_expr
    substring_func = (Literal('substring') +
                      Group(lpar + node + comma + integer +
                            Optional(comma + integer) + rpar))
    usch_filename = Literal('usch:getFileName') + Group(lpar + rpar)
    usch_iif = (Literal('usch:iif') +
                Group(lpar + expr + comma + expr + comma + expr + rpar))
    usch_compare_date = (Literal('usch:compareDate') +
                         Group(lpar + node + comma + node + rpar))
    funcs = (count_func | round_func | sum_func |
             number_func | substring_func | usch_filename |
             usch_iif | usch_compare_date)\
        .setParseAction(lambda toks: _push(stack, toks[0]))

    atom = (funcs | node | (Optional(bool_not) + parenthesized_expr)
            .setParseAction(lambda toks: _push_not(stack, toks)))

    factor = atom + ZeroOrMore((general_comp +
                                (integer | atom | quoted_string | date))
                               .setParseAction(lambda toks: _push(stack, toks[0])))
    term = factor + ZeroOrMore((bool_and + factor)
                               .setParseAction(lambda toks: _push(stack, toks[0])))
    expr <<= term + ZeroOrMore((bool_or + term)
                               .setParseAction(lambda toks: _push(stack, toks[0])))
    return expr


def tokenize(text, stack):
    _expr = _create_tokenizer(stack)
    return _expr.parseString(text).asList()
