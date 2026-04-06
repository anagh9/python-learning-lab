thisdict = {
    "brand": "Ford",
    "model": "Mustang",
    "year": 1964
}
print(thisdict)

# dictionary items
print(thisdict["brand"])


# Dictionaries cannot have two items with the same key:
thisdict = {
    "brand": "Ford",
    "model": "Mustang",
    "year": 1964,
    # "year": 2020 #error
}
print(thisdict)


# dictionary Length
print(len(thisdict))
print(type(thisdict))  # <class 'dict'>
