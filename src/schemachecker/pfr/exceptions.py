class InternalPfrError(Exception):
    def __init__(self) -> None:
        self.message = None

    def __str__(self):
        return self.message


class EmptyScenarioError(InternalPfrError):
    def __init__(self) -> None:
        self.message = 'Не найден сценарий проверки'


class DocTypeNotFound(InternalPfrError):
    def __init__(self):
        self.message = 'Не удалось определить тип документа по направлению АДВ'


class DocDefinitionNotFound(InternalPfrError):
    def __init__(self):
        self.message = 'Не найдено определение документа'


class PrefixNotFound(InternalPfrError):
    def __init__(self) -> None:
        self.message = 'Не удалось определить префикс файла'


class SchemesNotFound(InternalPfrError):
    def __init__(self, prefix: str) -> None:
        self.message = f'Не найдены проверочные схемы для файла с префиксом {prefix}'


class QueriesNotFound(InternalPfrError):
    def __init__(self, prefix: str) -> None:
        self.message = f'Не найдены проверочные xquery скрипты для файла с префиксом {prefix}'


class QueryResultError(InternalPfrError):
    def __init__(self) -> None:
        self.message = 'Ошибка при получении результатов проверки по xquery выражению'
