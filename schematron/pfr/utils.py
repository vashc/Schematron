translation_dict = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd',
    'е': 'e', 'ё': 'e', 'ж': 'j', 'з': 'z', 'и': 'i',
    'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o',
    'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
    'ф': 'f', 'х': 'h', 'ц': 'c', 'ш': 'sh', 'щ': 'sch',
    'ч': 'ch', 'ы': 'i', 'э': 'e', 'ю': 'yu', 'я': 'ya'
}


def translate(expr):
    return ''.join(map(lambda ch: translation_dict.get(ch) or ch, expr.lower()))
