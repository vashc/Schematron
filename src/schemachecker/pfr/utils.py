import atexit
import errno
import fcntl
import logging
import os
import signal
import sys
from functools import wraps
from time import time, sleep
from typing import List


class Translator:
    translation_dict = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd',
        'е': 'e', 'ё': 'e', 'ж': 'j', 'з': 'z', 'и': 'i',
        'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o',
        'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'c', 'ш': 'sh', 'щ': 'sch',
        'ч': 'ch', 'ы': 'i', 'э': 'e', 'ю': 'yu', 'я': 'ya'
    }

    @staticmethod
    def translate(expr):
        return ''.join(map(lambda ch: Translator.translation_dict.get(ch) or ch,
                           expr.lower()))


class Flock:
    """ Менеджер контекста для синхронизации записи в файл между несколькими процессами. """
    def __init__(self, path, timeout=None):
        self._path = path
        self._timeout = timeout
        self.fd = None

    def __enter__(self):
        self.fd = os.open(self._path, os.O_RDWR)
        start_time = time()

        while True:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self.fd
            except (OSError, IOError) as ex:
                # Ресурс недоступен
                if ex.errno != errno.EAGAIN:
                    raise
                # Не смогли взять лок в течение заданного времени
                elif (self._timeout is not None
                      and time() > start_time + self._timeout):
                    raise

            # Ждём некоторое время и пробуем взять лок снова
            sleep(0.1)

    def __exit__(self, exc_type, exc_val, exc_tb):
        fcntl.flock(self.fd, fcntl.LOCK_UN)
        os.close(self.fd)
        self.fd = None


class Logger:
    def __init__(self, logs_path):
        self.log_format = "%(asctime)s [%(levelname)5s] %(message)s"
        self.date_format = "%d.%m.%Y %H:%M:%S"
        self.logs_path = logs_path

    def get_logger(self, name):
        __formatter = logging.Formatter(fmt=self.log_format,
                                        datefmt=self.date_format)

        logger = logging.getLogger(name)
        handler = logging.FileHandler(os.path.join(self.logs_path,
                                                   f"{name}.log"))
        handler.setFormatter(__formatter)
        logger.setLevel('DEBUG')
        logger.addHandler(handler)
        return logger


# Зарегистрированные функции очистки
_registered_cleanup_funcs = set()
# Уже выполненные функции очистки
_executed_cleanup_funcs = set()


def register_cleanup_function(func: callable=None,
                              signals: List[int]=None,
                              logfunc: callable=lambda s: print(s, file=sys.stderr)) -> callable:
    """
    Метод для регистрации функции очистки, выполняемой после завершения работы.
    Функция будет выполнена после нормального завершения или получения сигнала.
    :param func: выполняемая функция.
    :param signals: список сигналов для обработки.
    :param logfunc: функция логирования, по умолчанию выводит в stderr.
    :return: callable.
    """
    def _func_wrapper():
        if func not in _executed_cleanup_funcs:
            try:
                func()
            finally:
                _executed_cleanup_funcs.add(func)

    def _signal_wrapper(signum: int=None) -> None:
        if signum is not None:
            if logfunc is not None:
                logfunc(f'Process {os.getpid()} received signal {signum}')

        _func_wrapper()
        if signum is not None:
            if signum == signal.SIGINT:
                raise KeyboardInterrupt
            sys.exit(signum)

    def _register_func(func: callable, signals: List[int]) -> None:
        if not callable(func):
            raise TypeError('{!r} is not callable'.format(func))

        signals = set(signals)

        for sig in signals:
            # Регистрирует новый обработчик для сигнала, убирает старый.
            # SIG_IGN - игнорирует сигнал;
            # SIG_DFL - обработка сигнала по умолчанию.
            old_handler = signal.signal(sig, _signal_wrapper)
            if old_handler not in (signal.SIG_DFL, signal.SIG_IGN):
                if not callable(old_handler):
                    continue
                # Чтобы не получать KeyboardInterrupt стек трейс
                if sig == signal.SIGINT and old_handler is signal.default_int_handler:
                    continue
                if old_handler not in _registered_cleanup_funcs:
                    atexit.register(old_handler)
                    _registered_cleanup_funcs.add(old_handler)

        # Если не получен сигнал, нормальное завершение
        if func not in _registered_cleanup_funcs or not signals:
            atexit.register(_func_wrapper)
            _registered_cleanup_funcs.add(func)

    if func is None:
        @wraps
        def outer(func: callable) -> None:
            return _register_func(func, signals)
        return outer
    else:
        _register_func(func, signals)
        return func


