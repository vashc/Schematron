class InternalPfrError(Exception):
    def __init__(self) -> None:
        self.message = None

    def __str__(self):
        return self.message


class EmptyScenarioError(InternalPfrError):
    def __init__(self):
        self.message = 'Не был найден сценарий проверки'
