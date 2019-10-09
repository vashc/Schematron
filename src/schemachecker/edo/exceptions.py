class InternalEdoError(Exception):
    def __init__(self) -> None:
        self.message = None

    def __str__(self):
        return self.message


class XsdSchemeError(InternalEdoError):
    def __init__(self, ex):
        self.message = f'Ошибка при разборе .xsd файла шаблона: {ex}'
