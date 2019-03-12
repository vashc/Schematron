import os
import asyncio
from time import time
from glob import glob
from schematron.schematron_checker import SchematronChecker
from config import CONFIG

xsd_root = CONFIG['xsd_root']
xml_root = CONFIG['xml_root']
sch = SchematronChecker(xml_root=xml_root, xsd_root=xsd_root)


def sync_test():
    os.chdir(xml_root)

    loop = asyncio.get_event_loop()

    time_list = []
    results = []

    for xml_file in glob('*')[:]:
        print('_' * 80)
        print('FILE:', xml_file)
        start_time = time()
        result = loop.run_until_complete(sch.check_file(xml_file))
        results.append(result)
        time_list.append(time() - start_time)
    print(f'\nElapsed time: {round(sum(time_list), 4)}; '
          f'average time: {round(sum(time_list) / len(time_list), 4)}')
    return results


results = sync_test()

test_results = {'passed': [], 'failed': [], 'missed_attribute': []}

for result in results:
    if result['result'][0] == 'failed':
        if result['result'][1] == 'some attributes are missed':
            test_results['missed_attribute'].append(result)
        else:
            test_results['failed'].append(result)
    else:
        test_results['passed'].append(result)

print(f'\n\u001b[31mFailed: {len(test_results["failed"])};\u001b[0m')
print(f'\u001b[32mPassed: {len(test_results["passed"])};\u001b[0m')
print(f'\u001b[35mMissed attributes: {len(test_results["missed_attribute"])};\u001b[0m')
# print(f'Ratio: {round(test_results["passed"] / (test_results["passed"] + test_results["failed"]), 4)}')
