# If file Exits It will overwrite the content

# r => read
# w => write
# a => append

with open("Files/vegetable.txt", "w") as myfile:
    myfile.write("Tomato\nCucumber\nOnion")


# Appending In txt Files
with open("Files/test.txt", "a+") as myfile:
    for i in range(10):
        myfile.write("\nTest1\nTest2\nTest3")
