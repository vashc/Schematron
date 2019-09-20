import re
import operator

from time import time
from lxml import etree
from functools import wraps
from pyparsing import Word, Literal, ZeroOrMore, Forward, Keyword,\
    Combine, Group, Suppress, Optional, oneOf, srange, nums


class AdditionTokenizer:
    def __init__(self):
        self.stack = []
        self.variables = dict()
        self.tokenizer = self._create_tokenizer()
        self.valid_var_name = re.compile('[a-zA-Z0-9_]+')

    def _push_xpath(self, toks):
        chunks = toks[0].split('/')
        # Конструкция - имя переменной + xpath выражение
        for idx, chunk in enumerate(chunks):
            if len(chunk) > 0 and self.valid_var_name.match(chunk):
                chunks[idx] = self.variables[chunk]
            else:
                break
        expr = '/'.join(chunks)
        self.stack.append(expr)

    def _push_var(self, toks):
        self.variables[toks[0]] = toks[2]

    def _push(self, toks):
        self.stack.append(toks[0])

    def _create_tokenizer(self):
        alphabet_lower = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
        alphabet = alphabet_lower + alphabet_lower.upper()

        assignment = Literal(':=')
        delimiter = Literal(';')
        add = oneOf('+ -')
        multy = oneOf('* /')
        comp = oneOf('< <= = >= > <>')

        lpar, rpar = map(Suppress, '()')

        # Функции
        f_sum = Keyword('sum')

        # Имя корневой переменной, латиница
        var_name = Word(srange('[a-zA-Z]') + '_' + nums)
        node_val = Word(alphabet + '/@_' + nums)
        xpath_expr = Word(alphabet + srange('[a-zA-Z]') + '_"[]@/<=>.' + nums)
        xpath_expr_white = Word(alphabet + srange('[a-zA-Z]') + '_"[]@/<=>. ' + nums)

        var_assignment = (var_name + assignment + node_val).setParseAction(self._push_var)

        func_args = xpath_expr_white.setParseAction(self._push_xpath)
        funcs = (f_sum + Group(lpar + func_args + rpar)).setParseAction(self._push)

        term = funcs | xpath_expr

        multi_expr = term + ZeroOrMore((multy + term).setParseAction(self._push))
        add_expr = multi_expr + ZeroOrMore((add + multi_expr).setParseAction(self._push))

        expression = var_assignment | (add_expr + Optional((comp + add_expr).setParseAction(self._push)))

        return expression

    def tokenize_expression(self, expr):
        self.stack = []
        self.tokenizer.parseString(expr)

        return self.stack


class AdditionEvaluator:
    def __init__(self):
        self.unary_map = {
            'sum': self.rounder(self._sum)
        }

        self.binary_map = {
            '+':        self.rounder(operator.add),
            '-':        self.rounder(operator.sub),
            '/':        self.rounder(operator.truediv),
            '*':        self.rounder(operator.mul),
            '<':        operator.lt,
            '<=':       operator.le,
            '=':        operator.eq,
            '>':        operator.gt,
            '>=':       operator.ge,
            '<>':       operator.ne
        }

        self.stack = []
        self._content = None

    @property
    def content(self):
        return self._content

    @content.setter
    def content(self, xml_content):
        self._content = xml_content

    def rounder(self, op):
        """
        Округление результатов операций,
        требуется для вычислений с плавающей точкой
        """
        _operator = op

        @wraps(op)
        def inner(*args):
            return round(op(*args), 2)
        return inner

    def _sum(self, args):
        f_args = map(float, args)
        return sum(f_args)

    def _evaluate_stack(self):
        op = self.stack.pop()

        if op in self.unary_map:
            arg = self._evaluate_stack()
            return self.unary_map[op](arg)

        elif op in self.binary_map:
            arg2 = self._evaluate_stack()
            arg1 = self._evaluate_stack()
            return self.binary_map[op](arg1, arg2)

        else:
            return self.content.xpath(op)

    def evaluate_expr(self, expr):
        self.stack = expr.copy()

        try:
            return self._evaluate_stack()
        except Exception as ex:
            print(ex)


class AdditionInterpreter:
    def __init__(self):
        self.tokenizer = AdditionTokenizer()
        self.evaluator = AdditionEvaluator()

    @property
    def content(self):
        return self.evaluator.content

    @content.setter
    def content(self, xml_content):
        self.evaluator.content = xml_content

    def interpret(self, text):
        expr_list = list(filter(None, text.split(';')))
        stack_list = []

        for expr in expr_list:
            stack_list.append(self.tokenizer.tokenize_expression(expr.strip()))

        start_time = time()
        for stack in stack_list:
            # Пришло проверочное выражение, а не определение переменной
            if len(stack) > 0:
                result = self.evaluator.evaluate_expr(stack)
                print(result)
        print('Elapsed time:', time() - start_time)


def print_expr(at, expr):
    print('-' * 80)
    print(expr)
    print(at.tokenize_expression(expr))


if __name__ == '__main__':
    at = AdditionTokenizer()
    # print_expr(at, 'var:= /что/что;')
    # print_expr(at, 'sum(var/@кек)  =var + var/нет/пути;')
    # print_expr(at, 'sum(var/нет[@да<1.0])  =var + var/нет/пути;')
    # print_expr(at, 'sum(var/нет[@да<1.0])  =sum(var) + var/нет/пути + sum(/тут/путь@ну);')
    # print_expr(at, 'sum(var/нет[@да<1.0])  =sum(var) + var/нет/пути * var + sum(/тут/путь@ну);')
    # print_expr(at, 'root:=/Файл/Документ/РасчетСВ/;')
    # print_expr(at, 'sum(root//ПерсСвСтрахЛиц/СвВыплСВОПС/СвВыпл/СвВыплМК[@Месяц<"06"]/@НачислСВ);')

    parser = etree.XMLParser(encoding='cp1251',
                             remove_comments=True)
    file = '/home/vasily/PyProjects/FLK/NO_RASCHSV_9105_9105_9105017720910501001_20190705_5A036D6A-DC48-B98B-1145-5D92E85FEC16.xml'
    with open(file, 'rb') as handler:
        content = etree.parse(handler, parser).getroot()
    # _ = content.xpath('//ПерсСвСтрахЛиц/СвВыплСВОПС/СвВыпл/СвВыплМК[@Месяц<"06"]/@НачислСВ')
    # _ = content.xpath('//Файл/Документ/РасчетСВ/ПерсСвСтрахЛиц/СвВыплСВОПС/СвВыпл/СвВыплМК[@Месяц<"06"]/@НачислСВ')
    # print(len(_))
    # for elem in _:
    #     print(elem)
        # print(etree.tostring(elem, encoding='utf-8').decode('utf-8'))

    ai = AdditionInterpreter()
    ai.content = content
    ai.interpret('root:=//Файл/Документ/РасчетСВ;sum:=/ОбязПлатСВ/РасчСВ_ОПС_ОМС/РасчСВ_ОПС/НачислСВНеПрев;'
                 '_sum:=/ПерсСвСтрахЛиц/СвВыплСВОПС/СвВыпл;'
                 'sum(root/sum/@Сум2Посл3М) -'
                 'sum(root/_sum/СвВыплМК[@Месяц="05"]/@НачислСВ);'
                 'sum(root/sum/@СумВсегоПосл3М) -'
                 'sum(root/_sum/СвВыплМК[@Месяц="04" or @Месяц="05" or @Месяц="06"]/@НачислСВ);')
    ai.interpret('sum(//НачислСВНеПрев/@Сум2Посл3М);'
                 'sum(//СвВыпл/СвВыплМК[@Месяц="05"]/@НачислСВ);'
                 'sum(//НачислСВНеПрев/@СумВсегоПосл3М)<>'
                 'sum(//СвВыпл/СвВыплМК[@Месяц="04" or @Месяц="05" or @Месяц="06"]/@НачислСВ);')
