import os
import requests
from re import compile, match
from typing import List, Tuple, Dict, Any, ClassVar
from lxml import etree
from io import StringIO


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

    def _parse_fss_response(self, response: str) -> List[Tuple[str, str]]:
        """
        Метод для извлечения ошибок из возвращаемого ФСС ответа
        :param response:
        :return:
        """
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

    def _validate_xsd(self, input: ClassVar[Dict[str, Any]]) -> bool:
        try:
            self.xsd_scheme.assertValid(self.xml_obj)
            return True
        except etree.DocumentInvalid as ex:
            for error in self.xsd_scheme.error_log:
                input.verify_result['xsd_asserts'] \
                    .append(f'{error.message} (строка {error.line})')

            input.verify_result['result'] = 'failed_xsd'
            input.verify_result['description'] = (
                f'Ошибка при валидации по xsd схеме файла '
                f'{self.filename}: {ex}.')
            return False

    def _validate_sum(self, input: ClassVar[Dict[str, Any]]) -> None:
        """
        Метод для проверки контрольных соотношений на портале ФСС
        :param input:
        :return:
        """
        upload_url = f'{self.url}f4upload?auth=false&type=F4_INPUT&current_org=&current_period=&rstate='
        files = {self.filename: self.xml_content}

        response = requests.post(upload_url, files=files)
        self.cookie = response.cookies  # .get('JSESSIONID')

        if self.cookie:
            check_url = f'{self.url}fss/f4validation?type=f4_input'
            response = requests.get(check_url, cookies=self.cookie)

            err_list = self._parse_fss_response(response.text)
            if err_list:
                input.verify_result['result'] = 'failed_sum'
                input.verify_result['sum_asserts'].extend(err_list)

        # Не получили sessionid, выполнить проверку не удастся
        else:
            # TODO: возвращать что-нибудь полезное пользователю
            pass

    def check_file(self, input: ClassVar[Dict[str, Any]]) -> None:
        self.filename = input.filename
        self.xml_content = input.content
        self.xml_obj = input.xml_obj
        self.xsd_content = input.xsd_schema
        self.xsd_scheme = etree.XMLSchema(self.xsd_content)

        input.verify_result = dict()
        input.verify_result['result'] = 'passed'
        input.verify_result['xsd_asserts'] = []
        input.verify_result['sum_asserts'] = []

        # Проверка по xsd, если не прошла - возвращаем
        if not self._validate_xsd(input):
            return

        # Првоерка контрольных сумм (прокси на портал ФСС)
        self._validate_sum(input)
