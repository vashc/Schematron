class InternalRarError(Exception):
    def __init__(self) -> None:
        self.message = None

    def __str__(self):
        return self.message


class XsdParseError(InternalRarError):
    def __init__(self, xsd_name: str, ex: Exception) -> None:
        self.message = f'Ошибка при чтении/преобразовании XSD схемы {xsd_name}: {ex}'


class SchemeNotFound(InternalRarError):
    def __init__(self, filename: str) -> None:
        self.message = f'Не найдена проверочная схема для файла {filename}'
