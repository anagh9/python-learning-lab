
import pandas

import os
import sys
# Using Third Party Library

# On Console
# sys.builtin_module_names : List all builtin functions

import time
# dir(time)

print("Hello")
time.sleep(1)
print("World")  # It will Printed after 1 seconds

"""
while True:
    if os.path.exists("Files/fruit.txt"):
        with open("Files/fruit.txt") as file:
            print(file.read())
    else:
        print("File does not exits")
        time.sleep(1)
"""
if __name__ == "__main__":

    while True:
        if os.path.exists("Files/temps_today.csv"):
            data = pandas.read_csv("Files/temp_today.csv")
            print(data.mean()["str1"])
        else:
            print("File does not exists")
