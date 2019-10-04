import os
from typing import List, Dict, Any, ClassVar, Union
from lxml import etree
from .interpreter import Interpreter
from .tokenizer import Tokenizer
from .exceptions import ContextError, ParserError

_root = os.path.dirname(os.path.abspath(__file__))


class FnsChecker:
    def __init__(self) -> None:
        self.xml_file = None
        self.xml_content = None
        self.xsd_content = None
        self.xsd_scheme = None

        # Подготовка лексера и интерпретатора
        self.tokenizer = Tokenizer().create_tokenizer()
        self.interpreter = Interpreter()

    @staticmethod
    async def _get_error(node: etree.Element) -> Dict[str, Any]:
        error = {}
        replacing = []
        error['code'] = node.get('code')

        text = ''
        if node.text:
            text = node.text

        for child in node:
            select = child.get('select', None)
            replacing.append(select)
            text += select

            if child.tail is not None:
                # Конец текста ошибки перед закрывающим тегом
                text += child.tail

        # Убираем спецсимволы
        text = ' '.join((text.replace('\n', '').replace('\t', '')).split())
        error['text'] = text
        error['replacing'] = replacing

        return error

    @staticmethod
    async def _return_error(text: Union[str, Exception]) -> str:
        return f'Error. {text}.'

    async def _get_error_text(self, assertion: Dict[str, Any]) -> str:
        error = assertion['error']
        error_text = error['text']
        for replacement in error['replacing']:
            error_text = error_text.replace(
                replacement, str(
                    self.interpreter.evaluate_expr(replacement,
                                                   self.xml_content,
                                                   self.xml_file,
                                                   assertion['context'])
                )
            )
        return error_text

    async def _get_asserts(self, content: etree.ElementTree) -> List[Dict[str, Any]]:
        """
        Получение списка проверок Schematron-выражений
        """
        assertions = content.findall('.//xs:appinfo', namespaces=content.nsmap)
        assert_list = []

        for assertion in assertions:
            for pattern in assertion:
                name = pattern.attrib.get('name', None)
                if not name:
                    continue

                for rule in pattern:
                    context = rule.attrib['context']

                    # Пропуск проверок, родительский элемент
                    # которых может не встречаться, minOccurs=0
                    occurs_elements = assertion.xpath(
                        f'ancestor::*[@minOccurs=0]')
                    if len(occurs_elements):
                        continue

                    # Проверка, присутствует ли контекст в xml файле
                    if len(self.xml_content.xpath(f'//{context}')) == 0:
                        # Не найден контекст в xml файле

                        # Пропуск опциональных проверок, choice
                        choice_elements = assertion.xpath(f'ancestor::xs:choice',
                                                          namespaces=content.nsmap)
                        if len(choice_elements):
                            # Опциональная проверка, пропускаем
                            continue
                        # Ошибка, проверка обязательна, контекст не найден
                        raise ContextError(context, self.xml_file)

                    for sch_assert in rule:
                        for error_node in sch_assert:
                            error = await self._get_error(error_node)

                            assert_list.append({
                                'name':     name,
                                'assert':   sch_assert.attrib['test'],
                                'context':  context,
                                'error': error
                            })

        return assert_list

    async def _validate_xsd(self, input: ClassVar[Dict[str, Any]]) -> bool:
        try:
            self.xsd_scheme.assertValid(self.xml_content)
            return True
        except etree.DocumentInvalid as ex:
            for error in self.xsd_scheme.error_log:
                input.verify_result['xsd_asserts'] \
                    .append(f'{error.message} (строка {error.line})')

            input.verify_result['result'] = 'failed_xsd'
            input.verify_result['description'] = (
                f'Ошибка при валидации по xsd схеме файла '
                f'{self.xml_file}: {ex}.')
            return False

    async def _validate_schematron(self, input: ClassVar[Dict[str, Any]]) -> None:
        try:
            asserts = await self._get_asserts(self.xsd_content)
        except Exception as ex:
            input.verify_result['result'] = 'failed_sch'
            input.verify_result['description'] = await self._return_error(ex)
            return

            # Нет выражений для проверки
        if not asserts:
            return

        for assertion in asserts:
            try:
                assertion_result = self.interpreter.evaluate_expr(assertion['assert'],
                                                                  self.xml_content,
                                                                  self.xml_file,
                                                                  assertion['context'])
                if not assertion_result:
                    input.verify_result['sch_asserts'] \
                        .append((assertion['name'],
                                 assertion['error']['code'],
                                 await self._get_error_text(assertion)))
            except ParserError:
                # FIXME (ParserError)
                pass
            except Exception as ex:
                input.verify_result['result'] = 'failed_sch'
                input.verify_result['description'] = ex
                return

        if input.verify_result['sch_asserts']:
            input.verify_result['result'] = 'failed_sch'
            input.verify_result['description'] = 'Ошибки при проверке fns'

    async def check_file(self, input: ClassVar[Dict[str, Any]]) -> None:
        self.xml_file = input.filename
        self.xml_content = input.xml_tree
        self.xsd_content = input.xsd_schema
        self.xsd_scheme = etree.XMLSchema(self.xsd_content)

        input.verify_result = dict()

        input.verify_result['result'] = 'passed'
        input.verify_result['xsd_asserts'] = []
        input.verify_result['sch_asserts'] = []

        # Проверка по xsd
        if not await self._validate_xsd(input):
            return

        # Проверка выражений fns
        await self._validate_schematron(input)
