from typing import List


class InternalFnsError(Exception):
    def __init__(self) -> None:
        self.message = None

    def __str__(self):
        return self.message


class ContextError(InternalFnsError):
    def __init__(self, context: str, xml_file: str) -> None:
        self.message = f'Контекст {context} в файле {xml_file} не найден'


class NodeAttributeError(InternalFnsError):
    def __init__(self, context: str, node: str, xml_file: str) -> None:
        self.message = f'Атрибут {context}/{node} в файле {xml_file} не найден'


class TokenizerError(InternalFnsError):
    def __init__(self, expression: str, xml_file: str, ex: Exception) -> None:
        self.message = (f'Ошибка при лексическом анализе xpath выражения '
                        f'{expression} в файле {xml_file} ({ex})')


class ParserError(InternalFnsError):
    def __init__(self, expression: str, xml_file: str, ex: Exception) -> None:
        self.message = (f'Ошибка при интерпретации xpath выражения '
                        f'{expression} в файле {xml_file} ({ex})')


class TypeConvError(InternalFnsError):
    def __init__(self, args: List[str], ex: Exception) -> None:
        self.message = (f'Ошибка при приведении к вещественному типу '
                        f'аргументов {args}: {ex}')


class XsdParseError(InternalFnsError):
    def __init__(self, xsd_name: str, ex: Exception) -> None:
        self.message = f'Ошибка при чтении/преобразовании XSD схемы {xsd_name}: {ex}'


class CompendiumParseError(InternalFnsError):
    def __init__(self, comp_file: str, ex: Exception) -> None:
        self.message = f'Ошибка при работе с файлом компендиума {comp_file}: {ex}'


class SchemeNotFound(InternalFnsError):
    def __init__(self, knd: str, version: str) -> None:
        self.message = f'Не найдена проверочная схема для файла с КНД {knd} для версии {version}'


class FileAttributeError(InternalFnsError):
    def __init__(self) -> None:
        self.message = f'Не найдена информация о КНД и версии проверяемого файла'
