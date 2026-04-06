import datetime

# print date and time Now
print(datetime.datetime.now())


# variables
mynow = datetime.datetime.now()
print(mynow)

x = 10  # treat it as Int
y = "10"  # treat it as string
z = 10.1        # float

sum1 = x+x
sum2 = y+y

print(sum1)  # 20
print(sum2)  # 1010
print(z)  # 10.1

# For Taking Input from User

x = input("Enter any No")  # It will take anything as string
# print(x+10) # It will Give ERROR


# For taking input a int value

y = int(input("Enter int value"))
print(y+10)
