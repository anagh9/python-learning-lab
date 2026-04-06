from bs4 import BeautifulSoup

# with open('home.html','r') as html_file:
# content = html_file.read()
# print(content)  # It will print whole content

'''
with open('home.html', 'r') as html_file:
    content = html_file.read()

    soup = BeautifulSoup(content, 'lxml') #Doing same thong using library
    print(soup.prettify())

'''

# It will grab first tags with h5 heading
# It will grab all tags with h5 heading

'''
with open('home.html', 'r') as html_file:
    content = html_file.read()

    soup = BeautifulSoup(content, 'lxml')
    tags = soup.find('h5')
    tags = soup.find_all('h5')
    print(tags)
'''

'''
# Grabbing all courses on page

with open('home.html', 'r') as html_file:
    content = html_file.read()

    soup = BeautifulSoup(content, 'lxml')
    courses_html_tags = soup.find_all('h5')

    for course in courses_html_tags:
        print(course.text)

### Output
# Python for beginners
# Python Web Development
# Python Machine Learning
'''