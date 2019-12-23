import os
import requests
import asyncio
from re import compile
from typing import List, Tuple, Dict, Any, ClassVar
# noinspection PyUnresolvedReferences
from lxml import etree
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
from .exceptions import *


class FssChecker:
    def __init__(self, *, root) -> None:
        # Корневая директория для файлов валидации
        self.xsd_root = os.path.join(root, 'compendium/fss/compendium/')
        # TODO: use config
        self.filename = None
        self.xml_content = None  # Bytes
        self.xml_obj = None  # etree.ElementTree
        self.xsd_content = None
        self.xsd_scheme = None

        self.url = 'http://portal.fss.ru/'
        # ФСС использует sessionid в куках для доступа к проверке загруженного файла
        self.cookie = None

        # HTML парсер
        self.parser = etree.HTMLParser()

        # Регулярное выражение для ошибки, возвращаемой ФСС
        self.reg = compile(r'(\[.*]): (.*)')

        # Пул для распараллеливания requests
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.loop = asyncio.new_event_loop()

    @staticmethod
    def _set_error_struct(err_list: List[Tuple[str, str]], file: ClassVar[Dict[str, Any]]) -> None:
        """ Заполнение структуры ошибки для вывода.  """
        if len(err_list):
            file.verify_result['result'] = 'failed_sum'

        for error in err_list:
            file.verify_result['asserts'].append({
                'error_code': error[0],
                'description': error[1],
                'inspection_items': []
            })

    def _parse_fss_response(self, response: str) -> List[Tuple[str, str]]:
        """ Метод для извлечения ошибок из возвращаемого ФСС ответа. """
        tree = etree.parse(StringIO(response), self.parser)
        err_list = tree.findall('.//li')

        ret_list = []
        for err in err_list:
            # Разбиваем результат на код ошибки и описание
            err_match = self.reg.match(err.text)
            if err_match:
                err_code = err_match.group(1)
                err_descr = err_match.group(2)
            # Ошибка вернулась в нестандартном формате, присвоим свой код
            else:
                err_code = '[F4ERR_STD]'
                err_descr = err.text

            ret_list.append((err_code, err_descr))

        return ret_list

    def _validate_xsd(self, file: ClassVar[Dict[str, Any]]) -> bool:
        ret_list = []
        try:
            self.xsd_scheme.assertValid(self.xml_obj)
            return True
        except etree.DocumentInvalid as ex:
            for error in self.xsd_scheme.error_log:
                ret_list.append((str(error.line), error.message))
                self._set_error_struct(ret_list, file)

            file.verify_result['result'] = 'failed_xsd'
            file.verify_result['description'] = (
                f'Ошибка при валидации по xsd схеме файла '
                f'{self.filename}: {ex}.')
            return False

    @staticmethod
    def _post_dict(kwargs: Dict[str, Any]) -> requests.Response:
        """ Вспомогательный метод для запуска requests.post в executor с ключевыми аргументами. """
        # TODO: добавить внутренние коды ошибок
        try:
            response = requests.post(**kwargs)
            return response
        except requests.exceptions.RequestException:
            raise ChecksumError()

    def _validate_sum(self, file: ClassVar[Dict[str, Any]]) -> None:
        """ Метод для проверки контрольных соотношений на портале ФСС. """
        post_kwargs = dict(
            url=f'{self.url}f4upload?auth=false&type=F4_INPUT&current_org=&current_period=&rstate=',
            files={self.filename: self.xml_content},
            timeout=10
        )

        response = self.loop.run_until_complete(
            self.loop.run_in_executor(self.executor, self._post_dict, post_kwargs)
        )
        self.cookie = response.cookies  # .get('JSESSIONID')

        if self.cookie:
            check_url = f'{self.url}fss/f4validation?type=f4_input'
            response = requests.get(check_url, cookies=self.cookie)

            err_list = self._parse_fss_response(response.text)
            self._set_error_struct(err_list, file)

        # Не получили sessionid, выполнить проверку не удастся
        else:
            raise ChecksumError()

    def check_file(self, file: ClassVar[Dict[str, Any]], validate_sum: bool) -> None:
        """
        Метод для валидации файла по xsd и контрольным суммам.
            validate_sum: флаг, показывающий, нужно ли проверять контрольные суммы.
        Проверка выполняется только для поднаправлений, производных от 4ФСС.
        :return:
        """
        self.filename = file.filename
        self.xml_content = file.content
        self.xml_obj = file.xml_tree
        self.xsd_content = file.xsd_scheme
        self.xsd_scheme = etree.XMLSchema(self.xsd_content)

        file.verify_result = dict()
        file.verify_result['result'] = 'passed'
        file.verify_result['asserts'] = []

        # Проверка по xsd, если не прошла - возвращаем
        if not self._validate_xsd(file):
            return

        # Првоерка контрольных сумм (прокси на портал ФСС)
        # для форм, производных от 4ФСС
        if validate_sum:
            self._validate_sum(file)
