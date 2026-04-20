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
    def innerFunction(*args,**kwargs):
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

if __name__ == "__main__":
    # print(say_hello('hello'))
    s = generator_initiation([10,9,8,1,1,21,21,21,44])
    print(next(s))
    print(next(s))
    print(next(s))
        # print(i)

