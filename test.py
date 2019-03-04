from schematron_checker import SchematronChecker

s = ''
sch = SchematronChecker()

while s != '~':
    s = input('Enter sch/usch string: ')
    if s != '~':
        # print(sch.tokenize(s))
        sch.check_file(s)

