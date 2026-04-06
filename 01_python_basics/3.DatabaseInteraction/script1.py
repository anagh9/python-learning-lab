import mysql.connector
word = input("Enter a word in English and press Enter: ")
con = mysql.connector.connect(
    user="anagh",
    password="anagh123",
    host="localhost",
    database="anagh"
)
cursor = con.cursor()
query = cursor.execute(
    "SELECT * FROM student WHERE name = '%s'" % word)
results = cursor.fetchall()
if results:
    for result in results:
        print(result[1])
else:
    print("We couldn't find any results about that.")
