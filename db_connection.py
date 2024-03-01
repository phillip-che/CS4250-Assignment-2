#-------------------------------------------------------------------------
# AUTHOR: Phillip Che
# FILENAME: index.py
# SPECIFICATION: 
# FOR: CS 4250-Assignment #2
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import psycopg2

def connectDataBase():

    # Create a database connection object using psycopg2
    conn = psycopg2.connect(database = "datacamp_courses", 
                    user = "datacamp", 
                    host= 'localhost',
                    password = "postgresql_tutorial",
                    port = 5432)
    
    return conn

def createCategory(cur, catId, catName):
    # Insert a category in the database
    query = "INSERT INTO Categories (id, name) VALUES (%s, %s)", (catId, catName)
    print(query)
    cur.execute(query)
    print("Category created successfully.")

def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    catId = cur.execute("SELECT id FROM Categories WHERE name = %s", docCat)

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    cur.execute("INSERT INTO Documents (id, text, title, date) VALUES (%s, %s, %s, %s, %s, %s)", docId, docText, docTitle, docDate)

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    text = cur.execute("SELECT docText FROM Document WHERE docId=%s", docId)
    text = text.lower()
    terms = text.split(" ")

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    # --> add your Python code here

def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    # --> add your Python code here

    # 2 Delete the document from the database
    # --> add your Python code here

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    # --> add your Python code here

    # 2 Create the document with the same id
    # --> add your Python code here

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here