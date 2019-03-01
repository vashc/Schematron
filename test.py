from schematron_checker import SchematronChecker

s = ''
sch = SchematronChecker()

while s != '~':
    s = input('Enter sch/usch string: ')
    print(sch.tokenize(s))
