from typing import List


class InternalSchemaError(Exception):
    pass


class ContextError(InternalSchemaError):
    def __init__(self, context: str, xml_file: str) -> None:
        self.message = f'Контекст {context} в файле {xml_file} не найден'


class NodeAttributeError(InternalSchemaError):
    def __init__(self, context: str, node: str, xml_file: str) -> None:
        self.message = f'Атрибут {context}/{node} в файле {xml_file} не найден'


class TokenizerError(InternalSchemaError):
    def __init__(self, expression: str, xml_file: str, ex: Exception) -> None:
        self.message = (f'Ошибка при лексическом анализе xpath выражения '
                        f'{expression} в файле {xml_file} ({ex}).')


class ParserError(InternalSchemaError):
    def __init__(self, expression: str, xml_file: str, ex: Exception) -> None:
        self.message = (f'Ошибка при интерпретации xpath выражения '
                        f'{expression} в файле {xml_file} ({ex}).')


class TypeConvError(InternalSchemaError):
    def __init__(self, args: List[str], ex: Exception) -> None:
        self.message = (f'Ошибка при приведении к вещественному типу '
                        f'аргументов {args}: {ex}')
