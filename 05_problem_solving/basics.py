from dataclasses import dataclass, field
import time
import functools


def log_timer(func):
    @functools.wraps(func)
    def inner_function(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f'Executing time of {func.__name__} : {end-start}')
        return result
    return inner_function


def uppercase01(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs).upper()
        return result
    return wrapper


def lowercase(func):
    @functools.wraps(func)
    def innerFunction(*args, **kwargs):
        result = func(*args, **kwargs).lower()
        return result
    return innerFunction


@log_timer
def test_02():
    time.sleep(2)
    return


@uppercase01
def say_hello(a: str):
    return a


@lowercase
def say_hello_lc(a: str):
    return a


def generator_initiation(nums: list):
    for i in range(len(nums)):
        yield nums[i]**2


def read_chunks(path):
    import pandas as pd
    for chunk in pd.read_csv(path, chunksize=100_000):
        yield chunk


"""
Shallow copy (copy()) copies the container but not nested objects — they share the same references. 
Deep copy (deepcopy()) recursively copies everything. 

In Pandas, this matters with df.copy() vs slicing: a slice is a view — modifying it can modify the original and raise SettingWithCopyWarning.
"""


"""
A context manager guarantees setup and teardown — even if an exception occurs. Implemented via __enter__ / __exit__ or @contextmanager from contextlib.
"""
# from contextlib import contextmanager
# import snowflake.connector

# @contextmanager
# def snowflake_conn(creds):
# conn = snowflake.connector.connect(**creds)
# try:
# yield conn
# finally: conn.close()

# always runs, even on exception
# with snowflake_conn(creds) as conn:
# cursor = conn.cursor()
# cursor.execute("SELECT ...")


"""
Dataclasses (@dataclass) auto-generate __init__, __repr__, __eq__ from field annotations. Cleaner than plain dicts for structured data.
"""


@dataclass
class RunMetrics:
    run_id: str = field(default_factory=lambda: str(time.time()))
    rows_read: int = 0
    rows_clean: int = 0
    errors: int = 0
    start: float = field(default_factory=time.perf_counter)

    def throughput(self):
        return self.rows_read / (time.perf_counter() - self.start)


def execution_context_manager():
    data = {'run_id': '123', 'rows_read': 1000,
            'rows_clean': 900, 'errors': 10}
    data_class_check = RunMetrics(**data)
    print(data_class_check.throughput())


"""
== checks value equality. is checks identity (same object in memory). Common bug: if x == None works but if x is None is correct and faster — None is a singleton. Another bug: for integers outside -5 to 256, a is b can be False even if a == b because Python doesn't intern large integers.
"""


def identity_vs_equality():
    a = 2
    b = 2
    print(a == b)  # True (value equality)
    print(a is b)  # True (small integers are interned by Python)

    a = 257
    b = 257
    print(a == b)  # True (value equality)
    print(a is b)  # False (different objects in memory)


def retry_decorator(max_retries=3, delay=1):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(
                        f"Error: {e}. Retrying {retries + 1}/{max_retries}...")
                    retries += 1
                    time.sleep(delay)
            raise Exception(f"Failed after {max_retries} retries.")
        return wrapper
    return decorator


def test_retry():
    import random

    @retry_decorator(max_retries=5, delay=0.5)
    def unstable_function():
        if random.random() < 0.7:  # 70% chance to fail
            raise ValueError("Random failure")
        return "Success!"

    print(unstable_function())


"""
Threading: multiple threads in one process, share memory, limited by GIL for CPU-bound work. Best for I/O-bound tasks (file reads, DB calls). Multiprocessing: separate processes, each with own GIL and memory, true parallelism for CPU-bound work. Cost: higher memory, serialization overhead (pickle).
"""


def multiprocessing_worker(num):
    print(f"Worker {num} is running")
    return num * num


def multiprocessing_example():
    import multiprocessing

    # Pool workers must be defined at module scope so they can be pickled.
    with multiprocessing.Pool(processes=4) as pool:
        results = pool.map(multiprocessing_worker, range(10))
    print(results)
    return results


def multithreading_worker(num, results):
    print(f"Worker {num} is running")
    time.sleep(1)
    results[num] = num * num
    print(f"Worker {num} is done")


def multithreading_example():
    import threading

    results = [None] * 5
    threads = []
    for i in range(5):
        t = threading.Thread(target=multithreading_worker, args=(i, results))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print(results)
    return results


if __name__ == "__main__":
    # multiprocessing_example()
    multithreading_example()
