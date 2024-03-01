#-------------------------------------------------------------------------
# AUTHOR: Phillip Che
# FILENAME: db_connection.py
# SPECIFICATION: 
# FOR: CS 4250-Assignment #2
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import psycopg2
import string

def connectDataBase():

    # Create a database connection object using psycopg2
    conn = psycopg2.connect(database = "CPP", 
                    user = "postgres", 
                    host= 'localhost',
                    password = "123",
                    port = 5432)
    
    return conn

def createTables(cur):
    # Create Categories table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Categories (
            id_cat SERIAL PRIMARY KEY,
            name VARCHAR(255)
        );
    """)

    # Create Documents table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Documents (
            doc_number SERIAL PRIMARY KEY,
            id_cat INT,
            title VARCHAR(255),
            text TEXT,
            num_chars INT,
            date DATE,
            FOREIGN KEY (id_cat) REFERENCES Categories(id_cat)
        );
    """)

    # Create Document_Term table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Document_Term (
            doc_number INT,
            term VARCHAR(255),
            term_count INT,
            PRIMARY KEY (doc_number, term),
            FOREIGN KEY (doc_number) REFERENCES Documents(doc_number)
        );
    """)

    # Create Terms table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Terms (
            term VARCHAR(255) PRIMARY KEY,
            num_chars INT
        );
    """)

def createCategory(cur, catId, catName):
    # Insert a category in the database
    cur.execute("INSERT INTO Categories (id_cat, name) VALUES (%s, %s)", (catId, catName,))
    print("Category created successfully.")

def createDocument(cur, docId, docText, docTitle, docDate, docCat):
    
    # 1 Get the category id based on the informed category name
    cur.execute("SELECT id_cat FROM Categories WHERE name = %s", (docCat,))
    catId = cur.fetchone()

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    text = docText.lower()
    text = text.translate (str.maketrans('', '', string.punctuation))

    textLen = len(text.replace(" ", ""))
    cur.execute("INSERT INTO Documents (doc_number, id_cat, text, title, num_chars, date) VALUES (%s, %s, %s, %s, %s, %s)", (docId, catId, docText, docTitle, textLen, docDate,))

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database


    terms = text.split(" ")
    for term in terms:
        cur.execute("SELECT EXISTS(SELECT 1 FROM Terms WHERE term=%s)", (term,))
        exists = cur.fetchone()
        if not exists:
            cur.execute("INSERT INTO Terms (term, num_chars) VALUES (%s, %s)", (term, len(term),))    

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    termCount = {}
    for term in terms:
        if term not in termCount:
            termCount[term] = 0
        termCount[term] = termCount[term]+1

    for term in termCount:
        cur.execute("SELECT 1 FROM Document_Term WHERE doc_number = %s AND term = %s", (docId, term,))
        term_exists = cur.fetchone()

        if term_exists:
            cur.execute("UPDATE Document_Term SET term_count = term_count + %s WHERE doc_number = %s AND term = %s",
                        (termCount[term], docId, term,))
        else:
            cur.execute("INSERT INTO Document_Term (doc_number, term, term_count) VALUES (%s, %s, %s)",
                        (docId, term, termCount[term],))
    
def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    cur.execute("SELECT term FROM Document_Term WHERE doc_number = %s", docId)
    terms = cur.fetchall()

    print(terms)
    for term in terms:
        cur.execute("DELETE FROM Document_Term WHERE doc_number = %s AND term = %s", (docId, term,))

        cur.execute("SELECT SUM(term_count) as term_occurrences FROM Document_Term WHERE term = %s", (term,))
        term_occurrences = cur.fetchone()

        if term_occurrences == 0:
            cur.execute("DELETE FROM Terms WHERE term = %s", (term,))

    # 2 Delete the document from the database
    cur.execute("DELETE FROM Documents WHERE id_doc = %s", (docId,))

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    print('test')