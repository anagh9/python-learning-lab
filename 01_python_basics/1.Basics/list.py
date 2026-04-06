student_grades = [9.1, 8.8, 7.5]

mylist = ["apple", "banana", "cherry"]

thislist = ["apple", "banana", "cherry"]
print(thislist)
print(len(thislist))


list1 = ["apple", "banana", "cherry"]
list2 = [1, 5, 7, 9, 3]
list3 = [True, False, False]

list3.append(False)

# A list with strings, integers and boolean values:
list1 = ["abc", 34, True, 40, "male"]


print(type(mylist))  # <class 'list'>

# list() Constructor

thislist = list(("apple", "banana", "cherry"))
# note the double round-brackets
print(thislist)

# Taking Input In List

# creating an empty list
lst = []

# number of elemetns as input
n = int(input("Enter number of elements : "))

# iterating till the range
for i in range(0, n):
    ele = int(input())

    lst.append(ele)  # adding the element

print(lst)

# On console It will give all the operations of list
# dir(list)
# dir(str)


# List Slicing
# Lst[Initial: End: IndexJump]

# Initialize list
Lst = [50, 70, 30, 20, 90, 10, 50]

# Display list
print(Lst[::])


# Negative Indexing -7 -6 -5 -4 -3 -2 -1
# Initialize list
Lst = [50, 70, 30, 20, 90, 10, 50]

# Display list
print(Lst[-7::1])


# Slicing
# Initialize list
Lst = [50, 70, 30, 20, 90, 10, 50]

# Display list
print(Lst[1:5])

# TO find what any identifier does
# help(str.upper)
