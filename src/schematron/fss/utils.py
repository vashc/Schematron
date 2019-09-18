from suds.plugin import MessagePlugin
from suds.sax.attribute import Attribute


class FssPlugin(MessagePlugin):
    def __init__(self):
        # self.xmlns = 'http://tempuri.org/'
        self.xmlns = 'http://asystems.fss'

    def marshalled(self, context):
        context.envelope.nsprefixes = {'s': 'http://www.w3.org/2003/05/soap-envelope',
                                       'a': 'http://www.w3.org/2005/08/addressing'}
        context.envelope[1].setPrefix('s')
        context.envelope[1][0].setPrefix(None)
        context.envelope[1][0].append(Attribute('xmlns', self.xmlns))
        context.envelope[1][0][0].setPrefix(None)
        context.envelope[1][0][1].setPrefix(None)



