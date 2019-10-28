import os
import sys


def uppercase_schemes_names(scheme_root: str):
    """ Метод приводит имена всех схем в директории root к верхнему регистру. """
    for root, dirs, files in os.walk(scheme_root):
        for file in files:
            old_path = os.path.join(root, file)
            file = file.upper()
            new_path = os.path.join(root, file)
            os.rename(old_path, new_path)


if __name__ == '__main__':
    uppercase_schemes_names(scheme_root=sys.argv[1])
