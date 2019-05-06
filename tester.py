import os
import asyncio
import concurrent.futures
from time import time
from glob import glob
from multiprocessing import cpu_count
from fns import SchemaChecker
from config import CONFIG
from fns.xsd_resolver import get_xsd_file


async def sync_run():
    # xsd_root = '/home/vasily/PyProjects/FLK/Schematron/xsd'
    time_list = []
    results = []
    file_list = []

    loop = asyncio.get_event_loop()

    root_dir = '/home/vasily/PyProjects/FLK/4'
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if not file.startswith('UO'):
                file_list.append(os.path.join(root, file))

    print(len(file_list))

    xsd_tasks = [asyncio.ensure_future(get_xsd_file(file, xsd_root), loop=loop) for file in file_list]

    xsd_schemes = await asyncio.gather(*xsd_tasks)
    schemes = [scheme for scheme in xsd_schemes if scheme.filename is not None]

    for xml_file in schemes:
        start_time = time()
        sch.check_file(xml_file)
        results.append(xml_file)
        time_list.append(time() - start_time)
    print(f'\nElapsed time: {round(sum(time_list), 4)}; '
          f'average time: {round(sum(time_list) / len(time_list), 4)}')
    return results


def sync_test():
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(sync_run())
    return results
########################################################################################################################


async def run_checker_tasks(executor):
    loop = asyncio.get_event_loop()

    xsd_tasks = [asyncio.ensure_future(get_xsd_file(xml_file), loop=loop)
                 for xml_file in glob('*')[:18]]
    xsd_schemes = await asyncio.gather(*xsd_tasks)

    check_tasks = [loop.run_in_executor(executor, sch.check_file, result, xml_content)
                   for result, xml_content in xsd_schemes]
    return await asyncio.gather(*check_tasks)


def async_test():
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        start_time = time()

        event_loop = asyncio.get_event_loop()
        try:
            results = event_loop.run_until_complete(run_checker_tasks(executor))
        finally:
            event_loop.close()
        print('\nElapsed time:', round(time() - start_time, 4))
        return results
########################################################################################################################


async def multiproc_get_xsd():
    xsd_root = '/home/vasily/PyProjects/FLK/Schematron/xsd'
    loop = asyncio.get_event_loop()

    xsd_tasks = [asyncio.ensure_future(get_xsd_file(xml_file, xsd_root), loop=loop)
                 for xml_file in glob('*')[:]]
    return await asyncio.gather(*xsd_tasks)


def multiproc_task(xsd_scheme):
    return sch.check_file(xsd_scheme)


def multiproc_test():
    with concurrent.futures.ProcessPoolExecutor(max_workers=cpu_count()) as executor:
        start_time = time()

        event_loop = asyncio.get_event_loop()
        try:
            xsd_schemes = event_loop.run_until_complete(multiproc_get_xsd())
            schemes = [scheme for scheme in xsd_schemes if scheme.filename is not None]
            results = list(executor.map(multiproc_task, schemes))
        finally:
            event_loop.close()

        print('\nElapsed time:', round(time() - start_time, 4))
        return results


xsd_root = CONFIG['xsd_root']
xml_root = CONFIG['xml_root']
sch = SchemaChecker(xml_root=xml_root, xsd_root=xsd_root, verbose=True)

if __name__ == '__main__':
    # os.chdir(xml_root)

    results = sync_test()

    test_results = {'passed': [], 'failed_xsd': [], 'failed_sch': []}

    for result in results:
        if result.verify_result['result'] == 'failed_xsd':
            test_results['failed_xsd'].append(result)
        elif result.verify_result['result'] == 'failed_sch':
            test_results['failed_sch'].append(result)
        else:
            test_results['passed'].append(result)

    print(f'\n\u001b[31mFailed xsd: {len(test_results["failed_xsd"])}; '
          f'Failed sch: {len(test_results["failed_sch"])};\u001b[0m')
    print(f'\u001b[32mPassed: {len(test_results["passed"])};\u001b[0m')
    # print(f'Ratio: {round(len(test_results["passed"]) / len(test_results["passed"] + test_results["failed"]), 4)}')

    # print(test_results['failed_sch'][-1].verify_result)
    for assertion in test_results["failed_xsd"][-1].verify_result["xsd_asserts"]:
        print(assertion)
