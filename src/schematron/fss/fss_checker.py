from base64 import b64encode
from suds import WebFault
from suds.client import Client
from suds.bindings import binding
from suds.sax.element import Element
from suds.sax.attribute import Attribute
from typing import Dict, Any, ClassVar
from .utils import FssPlugin


class FssChecker:
    def __init__(self) -> None:
        # TODO: use config
        # TODO: use suds caching
        binding.envns = ('s', 'http://www.w3.org/2003/05/soap-envelope')
        self.url = 'http://docs-test.fss.ru/ExtService/GatewayService.svc'
        self.action = 'http://asystems.fss/IGatewayService/'
        self.wsdl = f'{self.url}?wsdl'
        self.client = None

        self.filename = None
        self.data = None

    def check_file(self, input: ClassVar[Dict[str, Any]]) -> Any:
        self.filename = input.filename
        self.data = b64encode(input.content).decode()

        self.client = self.create_client(action='SendFile')
        # self.client = self.create_client(action='UploadsGet')

        try:
            result = self.client.service.SendFile(data=self.data,
                                                  fileName=self.filename)

            # result = self.client.service.UploadsGet(regNum='1111111111',
            #                                         filter='Day')
            print(result)
        except WebFault as ex:
            print(ex.fault)

    def create_client(self, *, action: str) -> Client:
        action_text = f'{self.action}{action}'
        client = Client(self.wsdl,
                        headers={'Content-type': 'application/soap+xml; charset=utf-8',
                                 # 'SOAPAction': f'http://tempuri.org/IExtService/{action}'},
                                 'SOAPAction': action_text},
                        plugins=[FssPlugin()])  # TODO ()

        to = Element('a:To').setText(self.url)
        action = Element('a:Action').setText(action_text)
        attribute = Attribute('s:mustUnderstand', '1')
        to.append(attribute)
        action.append(attribute)
        client.set_options(soapheaders=(to, action))

        return client
