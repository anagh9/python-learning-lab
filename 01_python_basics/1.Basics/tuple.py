mytuple = ("apple", "banana", "cherry")
print(mytuple)

thistuple = ("apple", "banana", "cherry", "apple", "cherry")
# Tuple items are ordered, unchangeable, and allow duplicate values.
print(len(thistuple))
print(type(thistuple))


# NOT a tuple
thistuple = ("apple")
print(type(thistuple))  # < class 'tuple' >


tuple1 = ("apple", "banana", "cherry")
tuple2 = (1, 5, 7, 9, 3)
tuple3 = (True, False, False)
tuplex = ("abc", 34, True, 40, "male")

# The tuple() Constructor

# note the double round-brackets
thistuple = tuple(("apple", "banana", "cherry"))
print(thistuple)
