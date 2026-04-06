# Converting a String to list

def split_string(string):
    listString = string.split(" ")
    return listString


def join_String(listString):
    string = "-".join(listString)
    return string


# Driver Function
if __name__ == "__main__":
    string = input("Enter any String with Space in between : ")
    listString = split_string(string)
    print(listString)

    new_string = join_String(listString)
    print(new_string)

# Output
# Enter any String with Space in between : anagh is good boy

# ['anagh', 'is', 'good', 'boy']
# anagh-is-good-boy
