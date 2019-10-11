class InternalFssError(Exception):
    def __init__(self) -> None:
        self.message = None

    def __str__(self):
        return self.message


class ChecksumError(InternalFssError):
    def __init__(self) -> None:
        self.message = 'Ошибка при проверке контрольных сумм (проксирование)'
