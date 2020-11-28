import mysql.connector
from dbconfig import DB
from sys import argv

sql_client = mysql.connector.connect( **DB )
cursor = sql_client.cursor()

words = 'words'
translates = 'translates'

if len(argv) >= 3:
	words = argv[1]
	translates = argv[2]

sql_for_words = '''
    CREATE TABLE {}(
        idkey INT AUTO_INCREMENT PRIMARY KEY,
        word VARCHAR(50) NOT NULL UNIQUE,
        word1 VARCHAR(50),
        parent VARCHAR(50),
        gb TINYTEXT,
        us TINYTEXT,
        reserved VARCHAR(100)
    )
'''.format(words)

sql_for_translates = '''
    CREATE TABLE {}(
        index1 INT AUTO_INCREMENT PRIMARY KEY,
        idkey INT,
        FOREIGN KEY (idkey) REFERENCES {}(idkey),
        word VARCHAR(50),
        lang VARCHAR(30),
        gramm TINYTEXT,
        translate TEXT,
        link VARCHAR(100)
    )
'''.format(translates, words)

cursor.execute(sql_for_words)
sql_client.commit()
cursor.execute(sql_for_translates)
sql_client.commit()
sql_client.close()
