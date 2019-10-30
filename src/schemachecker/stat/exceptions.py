from traceback import format_exc


class InternalStatError(Exception):
    def __init__(self) -> None:
        self.message = None

    def __str__(self):
        return self.message


class TokenizerError(InternalStatError):
    def __init__(self, expression: str) -> None:
        self.message = f'Ошибка при лексическом анализе выражения {expression}'


class InputError(InternalStatError):
    def __init__(self, xml_file: str, descr: str) -> None:
        self.message = f'Ошибка при обработке файла {xml_file} ({format_exc()})'


class OkudError(InternalStatError):
    def __init__(self, xml_file: str) -> None:
        self.message = (f'Не найден обязательный атрибут "OKUD" '
                        f'в разделе metaForm в файле {xml_file}')


class InterpreterError(InternalStatError):
    def __init__(self, expression: str) -> None:
        self.message = f'Ошибка при интерпретации выражения {expression}: {format_exc()}'


class XmlParseError(InternalStatError):
    def __init__(self, filename: str, ex: Exception) -> None:
        self.message = f'Ошибка при разборе {filename} файла шаблона: {ex}'


class CompendiumAttributeError(InternalStatError):
    def __init__(self, element: str, ex: Exception) -> None:
        self.message = f'Не найден обязательный атрибут в элементе "{element}": {ex}'


class EmptyExtract(InternalStatError):
    def __init__(self) -> None:
        self.message = 'Получена пустая выборка'
