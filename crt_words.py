import mysql.connector
from dbconfig import DB

sql_client = mysql.connector.connect( **DB )

cursor = sql_client.cursor()

sql_for_words = '''
    CREATE TABLE words(
        idkey INT AUTO_INCREMENT PRIMARY KEY,
        word VARCHAR(50) NOT NULL UNIQUE,
        word1 VARCHAR(50),
        parent VARCHAR(50),
        gb TINYTEXT,
        us TINYTEXT,
        reserved VARCHAR(100)
    )
'''

sql_for_translates = '''
    CREATE TABLE translates(
        index1 INT AUTO_INCREMENT PRIMARY KEY,
        idkey INT,
        FOREIGN KEY (idkey) REFERENCES words(idkey),
        word VARCHAR(50),
        lang VARCHAR(30),
        gramm TINYTEXT,
        translate TEXT,
        link VARCHAR(100)
    )
'''

cursor.execute(sql_for_words)
sql_client.commit()
cursor.execute(sql_for_translates)
sql_client.commit()
sql_client.close()
