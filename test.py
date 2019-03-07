import os
import asyncio
import concurrent.futures
from time import time
from glob import glob
from schematron_checker import SchematronChecker

xsd_root = '/home/vasily/PyProjects/FLK/Schematron/xsd'
xml_root = '/home/vasily/PyProjects/FLK/Schematron/xml'
sch = SchematronChecker(xsd_root=xsd_root, xml_root=xml_root)
# sch = SchematronChecker()

# s = ''
# while s != '~':
#     s = input('Enter sch/usch string: ')
#     if s != '~':
#         print(sch.tokenize(s))
#         # sch.check_file(s)

test_xml = ['NO_NDPI_9999_9999_9624840906999901001_20170309_CB8F959F-C631-4C37-AEC9-EC54B55112A3.xml',
            'IU_ZAPR_9999_9999_9624840906999901001_20170309_D37C7B47-516E-4A7F-AF4D-C9E728CCADF1.xml',
            'NO_PRIB_9999_9999_9624840906999901001_20170317_4E099F57-74F0-4B55-9A0B-B730A0AB9C28.xml',
            'NO_SRCHIS_9999_9999_9624840906999901001_20170309_707448F7-6513-4E9A-87C4-B9727C5AA922.xml',
            'ON_ZVLRPOK_9999_9999_9624840906999901001_20170213_01081515-A109-3618-01E5-6F1311A49718.xml',
            'UT_SBROR_9999_9999_9624840906999901001_20170310_01C11518-A1C0-B714-01B0-F61731565E1F.xml',

            'NO_RASCHSV_7743_7743_7743715629774301001_20181130_722b8393-8781-4dbd-bf3a-13cc9d815b17.xml',
            'NO_NDS_7729_7729_7729668350772901001_20181129_2fd490be-f3c4-11e8-c88f-fa163e54d461.xml',
            'NO_USN_3801_3801_380102242368_20181130_05449beb-a6a8-4811-b0d2-21fa0908ddae.xml']

n_runs = 1


def time_test(n):
    def test_func_deco(func):
        def run_func(*args, **kwargs):
            time_list = []
            for _ in range(n):
                start_time = time()
                func(*args, **kwargs)
                time_list.append(time() - start_time)
            return sum(time_list), sum(time_list) / n
        return run_func
    return test_func_deco


@time_test(n_runs)
def sync_test():
    os.chdir(xml_root)

    loop = asyncio.get_event_loop()

    time_list = []
    test_results = {'passed': 0, 'failed': 0}

    for xml_file in glob('*'):
        print('FILE:', xml_file)
        start_time = time()
        result = loop.run_until_complete(sch.check_file(xml_file))
        if result == 'passed':
            test_results['passed'] += 1
        else:
            test_results['failed'] += 1
        time_list.append(time() - start_time)
    print(f'\nElapsed time: {round(sum(time_list), 4)}; '
          f'average time: {round(sum(time_list) / len(time_list), 4)}')
    print(f'\u001b[31mFailed: {test_results["failed"]};\u001b[0m\t'
          f'\u001b[32mPassed: {test_results["passed"]};\u001b[0m\t'
          f'Ratio: {round(test_results["passed"] / (test_results["passed"] + test_results["failed"]), 4)}')


async def run_task(executor, xml):
    print(xml)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, sch.check_file, xml)


async def run_tasks(executor):
    time_list = []
    loop = asyncio.get_event_loop()
    for _ in range(n_runs):
        start_time = time()
        tasks = [run_task(executor, xml) for xml in test_xml]
        # tasks = [run_task(executor, xml) for xml in test_xml]
        await asyncio.gather(*tasks)
        time_list.append(time() - start_time)
    return sum(time_list), sum(time_list) / n_runs


def async_test():
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        event_loop = asyncio.get_event_loop()
        el_t, avg_t = event_loop.run_until_complete(run_tasks(executor))
        # event_loop.run_until_complete(run_task(executor, test_xml[3]))
        event_loop.close()
    return el_t, avg_t


sync_test()
# print(f'Elapsed time: {round(el_t, 4)}; average time per sync run: {round(avg_t, 4)}')

# el_t, avg_t = async_test()
# print(f'Elapsed time: {round(el_t, 4)}; average time per async run: {round(avg_t, 4)}')
# async_test()
