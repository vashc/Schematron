import io
from string import printable, whitespace
from typing import List, Dict, Tuple
from pyparsing import Literal, Suppress, Group, Word, ZeroOrMore, Optional, alphanums
from pyparsing import ParseBaseException


class Query:
    """
    Класс для создания единого xquery запроса из нескольких.
    Используется при формировании компендиума с целью уменьшить число обменов данными с сервером BaseX.
    """
    def __init__(self) -> None:
        # Пространство имён по умолчанию
        self.default_namespaces: List[str] = []
        # Дополнительные пространства имён
        self.namespaces: Dict[str, str] = {}
        # Переменные
        self.variables: Dict[str, Tuple[str, str]] = {}
        # Объявленные функции
        self.functions: Dict[str, List[str]] = {}
        # Блоки проверок
        self.blocks = []

        self.tokenizer = self._create_tokenizer()

    def _push_default_ns(self, toks: List[List[str]]) -> None:
        """
        Метод заполняет список пространств имён по умолчанию.
        :param toks:
        :return:
        """
        self.default_namespaces.append(toks[0][0])

    def _push_ns(self, toks: List[List[str]]) -> None:
        """
        Метод заполняет словарь пространств имён {"Имя_пространства": "URI"}.
        :param toks:
        :return:
        """
        toks = toks[0]
        self.namespaces.update({toks[0]: toks[1]})

    def _push_var(self, toks: List[List[str]]) -> None:
        """
        Метод заполняет словарь переменных {"Имя_переменной": ("as" | ":=", "Значение_переменной")}.
        :param toks:
        :return:
        """
        toks = toks[0]
        self.variables.update({toks[0]: (toks[1], ' '.join(toks[2:]))})

    def _push_func(self, toks: List[List[str]]) -> None:
        """
        Метод заполняет словарь функций {"Имя_функции": ("Аргументы_и_возвращаемое_значение", "Тело_функции")}.
        toks[0]:
            0: имя функции;
            1: список аргументов и возвращаемый тип;
            2: тело функции;
        :param toks:
        :return:
        """
        toks = toks[0]
        self.functions.update({toks[0]: toks[1:]})

    def _push_block(self, toks: List[List[str]]) -> None:
        """
        Метод заполняет список проверочных блоков. Вставляются в конце результирующего запроса.
        :param toks:
        :return:
        """
        toks = toks[0]
        self.blocks.append(''.join(toks))

    def _create_tokenizer(self) -> Group:
        """
        Метод для токенизации входящего xquery скрипта.
        Использует Suppress для отбрасывания ненужных ключевых слов и информации.
        :return:
        """
        decl_l = Suppress('declare')
        default_ns_l = Suppress('default element namespace')
        ns_l = Suppress('namespace')
        var_l = Suppress('variable')
        func_l = Suppress('function')
        external_l = Literal('external')
        local_l = Suppress('local')
        as_l = Literal('as')
        assign = Literal(':=')
        quote = Suppress("'")
        d_quote = Suppress('"')
        semicolon = Suppress(';')
        colon = Suppress(':')
        equal = Suppress('=')

        any_quote = quote | d_quote

        alphabet = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
        # TODO: избавиться от лишних символов в словах в соответствии со стандартом.
        element = Word(alphabet + alphabet.upper() + alphanums + '$@/:._-()?*')

        var_name = Word(alphanums + '$')
        var_value = Word(alphabet + alphabet.upper() + alphanums + '$@/:._-()?*"\'')

        func_name = Word(alphanums + '-_')
        func_args = Word(alphanums + '$:()?*,' + whitespace)
        func_body = Word(alphabet + alphabet.upper() + ''.join(c for c in printable if c != ';'))

        block = Word(alphabet + alphabet.upper() + printable + '–№')

        default_ns_decl = Group(decl_l + default_ns_l + any_quote + element + any_quote + semicolon)\
            .setParseAction(self._push_default_ns)
        ns_decl = Group(decl_l + ns_l + element + equal + any_quote + element + any_quote + semicolon)\
            .setParseAction(self._push_ns)
        var_decl = Group(decl_l + var_l + var_name + (assign | as_l) + var_value + Optional(external_l) + semicolon)\
            .setParseAction(self._push_var)
        func_decl = Group(decl_l + func_l + local_l + colon + func_name + func_args + func_body + semicolon)\
            .setParseAction(self._push_func)
        block_decl = Group(block).setParseAction(self._push_block)
        query = (default_ns_decl + ZeroOrMore(ns_decl) + ZeroOrMore(var_decl) + ZeroOrMore(func_decl) +
                 ZeroOrMore(block_decl))
        return query

    def reset_query(self) -> None:
        """
        Метод очистки внутренних структур.
        Вызывается перед формированием xquery запроса.
        :return:
        """
        self.default_namespaces: List[str] = []
        self.namespaces: Dict[str, str] = {}
        self.variables: Dict[str, Tuple[str, str]] = {}
        self.functions: Dict[str, List[str]] = {}
        self.blocks = []

    def tokenize_query(self, query: str) -> None:
        """
        Метод токенизации входящего xquery выражения.
        Заполняет внутренние структуры для дальнейшей сборки в единый запрос.
        :param query:
        :return:
        """
        try:
            self.tokenizer.parseString(query)
        except ParseBaseException as ex:
            raise Exception(ex)

    def makeup_query(self) -> str:
        """
        Метод формирует новый xquery запрос из внутренних структур.
        :return:
        """
        with io.StringIO() as buffer:
            # Добавляем пространство по умолчанию
            buffer.write(f'declare default element namespace "{self.default_namespaces[0]}";\n')

            # Собираем пространства имён
            for ns_name, ns_descr in self.namespaces.items():
                buffer.write(f'declare namespace {ns_name} = "{ns_descr}";\n')

            # Собираем переменные
            for var_name, var_descr in self.variables.items():
                buffer.write(f'declare variable {var_name} {var_descr[0]} {var_descr[1]};\n')

            # Собираем функции
            for func_name, func_descr in self.functions.items():
                buffer.write(f'declare function local:{func_name}{func_descr[0]}{func_descr[1]};\n')

            # Добавляем блоки проверок
            for block in self.blocks:
                buffer.write(f'{block}\n')

            query = buffer.getvalue()

        return query
