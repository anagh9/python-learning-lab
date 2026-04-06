# opening a file
# Reading a File

myfile = open(
    "Files/fruit.txt")

content = myfile.read()
print(content)
# Closing a File
myfile.close()


print(content)  # Give Error

# Read a file Line by line
''' 
---------------------
Mode	Description
----------------------
'r'	    This is the default mode. It Opens file for reading.
'w'	    This Mode Opens file for writing.If file does not exist, it creates a new file.If file exists it truncates the file.
'x'	    Creates a new file. If file already exists, the operation fails.
'a'	    Open file in append mode. If file does not exist, it creates a new file.
't'	    This is the default mode. It opens in text mode.
'b'	    This opens in binary mode.
'+' 	This will open a file for reading and writing(updating)
'''
