import os
from typing import List


def assert_list_equality(first: List[str],
                         second: List[str]) -> bool:
    return all(f == s for f, s in zip(first, second))


def get_file_list(path: str) -> List[str]:
    file_list = []
    for dirpath, dirnames, filenames in os.walk(path):
        file_list.extend(filenames)
        break

    return file_list
