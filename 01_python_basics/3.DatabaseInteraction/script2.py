import sqlite3

# create connection in memory and delete after disconnection
# conn.sqlite.connect(':memory:')

# Connect to database
conn = sqlite3.connect('customer.db')

# Create a Cursor
c = conn.cursor()

# Create a Table


def CreateTable():
    c.execute("""CREATE TABLE customers (
        first_name text,
        last_name text,
        email text
        )""")
# c.execute("CREATE TABLE customers ( first_name DATATYPE, last_name DATABASE, email DATATYPE)")
# NUll
# Integer
# Real
# Text
# Blob Image/Mp3/Video

# Insert One Record In Table


def InsertRecord(fname, lname, email):
    c.execute("INSERT INTO customers VALUES (?,?,?)", (fname, lname, email))


def View():
    c.execute("SELECT * FROM customers")
    rows = c.fetchall()
    return rows


InsertRecord("Saurabh", "Kumar", "anagh9931@gmail.com")
print(View())
# Commit our command
conn.commit()

# Close our connections

conn.close()
