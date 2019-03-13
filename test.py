import os
import asyncio
import concurrent.futures
from time import time
from glob import glob
from schematron.schematron_checker import SchematronChecker
from schematron.checker import check_file, _get_xsd_file
from config import CONFIG, ROOT

xsd_root = CONFIG['xsd_root']
xml_root = CONFIG['xml_root']
# sch = SchematronChecker(xsd_root=xsd_root, xml_root=xml_root)
sch = SchematronChecker(xml_root=xml_root, xsd_root=xsd_root)

# s = ''
# while s != '~':
#     s = input('Enter sch/usch string: ')
#     if s != '~':
#         print(sch.tokenize(s))
#         # sch.check_file(s)


def tokenize_asserts(file):
    with open(os.path.join(*os.path.split(ROOT)[:-1], file), 'r') as handler:
        for line in handler.readlines():
            # assertion = handler.readline()
            try:
                sch.tokenize(line)
            except Exception as ex:
                print(line)
                print('Error:', ex)


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
            print(f'\nSummary on {n} runs. Total time: '
                  f'{round(sum(time_list), 4)}; '
                  f'average time: {round(sum(time_list) / n, 4)}')
            return
        return run_func
    return test_func_deco


@time_test(n_runs)
def sync_test():
    os.chdir(xml_root)

    loop = asyncio.get_event_loop()

    time_list = []
    test_results = {'passed': 0, 'failed': 0}

    for xml_file in glob('*')[:]:
        print('_' * 80)
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
    return await loop.run_in_executor(executor, check_file, xml)


async def run_tasks(executor):
    time_list = []
    test_results = {'passed': 0, 'failed': 0}

    loop = asyncio.get_event_loop()
    for _ in range(n_runs):
        start_time = time()
        tasks = [loop.run_in_executor(executor, check_file, xml)
                 for xml in test_xml]
        # tasks = [run_task(executor, xml) for xml in test_xml]
        completed, pending = await asyncio.wait(tasks)
        results = [t.result() for t in completed]
        for result in results:
            if result == 'passed':
                test_results['passed'] += 1
            else:
                test_results['failed'] += 1
        print(f'\u001b[31mFailed: {test_results["failed"]};\u001b[0m\t'
              f'\u001b[32mPassed: {test_results["passed"]};\u001b[0m\t'
              f'Ratio: {round(test_results["passed"] / (test_results["passed"] + test_results["failed"]), 4)}')
        time_list.append(time() - start_time)
    return sum(time_list), sum(time_list) / n_runs


async def async_check_one(executor, xml_file):
    loop = asyncio.get_event_loop()
    result = await _get_xsd_file(xml_file)
    if result != 'failed':
        xml_content, xsd_file = result
        start_time = time()
        result = await loop.run_in_executor(executor, check_file,
                                            xml_file, xml_content, xsd_file)
        print('Executor time:', time() - start_time)
        return result
    return 'failed'


async def check_one(xml_file):
    # result = await _get_xsd_file(xml_file)
    # if result != 'failed':
    #     xml_content, xsd_file = result
    #     return check_file(xml_file, xml_content, xsd_file)
    # return 'failed'
    sch = SchematronChecker(xml_root=xml_root, xsd_root=xsd_root)
    result = await sch.check_file(xml_file)
    return result


async def run_checker_tasks(executor):
    loop = asyncio.get_event_loop()
    # tasks = [async_check_one(executor, xml)
    #          for xml in glob('*')[:2]]
    tasks = [check_one(xml) for xml in glob('*')[:10]]
    results = await asyncio.gather(*tasks)
    return results


def async_test():
    start_time = time()
    os.chdir(xml_root)
    test_results = {'passed': 0, 'failed': 0}

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        event_loop = asyncio.get_event_loop()
        try:
            # el_t, avg_t = event_loop.run_until_complete(run_tasks(executor))
            # result = event_loop.run_until_complete(run_task(executor, test_xml[3]))
            results = event_loop.run_until_complete(run_checker_tasks(executor))
        finally:
            event_loop.close()
        # results = executor.map(check_one, glob('*')[:2])
    print('\nTotal time:', time() - start_time)
    for result in results:
        if result == 'passed':
            test_results['passed'] += 1
        else:
            test_results['failed'] += 1
    print(f'\u001b[31mFailed: {test_results["failed"]};\u001b[0m\t'
          f'\u001b[32mPassed: {test_results["passed"]};\u001b[0m\t'
          f'Ratio: {round(test_results["passed"] / (test_results["passed"] + test_results["failed"]), 4)}')
    # return el_t, avg_t


# sync_test()
# print(f'Elapsed time: {round(el_t, 4)}; average time per sync run: {round(avg_t, 4)}')

# el_t, avg_t = async_test()
# print(f'Elapsed time: {round(el_t, 4)}; average time per async run: {round(avg_t, 4)}')
# async_test()
tokenize_asserts('_asserts.txt')
