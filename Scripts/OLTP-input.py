import psycopg2
from psycopg2 import Error
import os

try:
    # Connection parameters with Windows-style path
    connection = psycopg2.connect(
        host="localhost",
        database="BI",
        user="postgres",
        password="root",
        port="5432"
    )

    # Create cursor with proper encoding for Windows
    cursor = connection.cursor()
    
    create_table_query = '''CREATE TABLE IF NOT EXISTS STUDENTS
          (ID SERIAL PRIMARY KEY,
          Code_apo TEXT NOT NULL,
          Nom TEXT NOT NULL,
          Prenom TEXT NOT NULL,
          Sexe TEXT NOT NULL,
          Birthdate DATE NOT NULL,
          Number_ajournement INT NOT NULL,
          Promo TEXT NOT NULL); '''

    cursor.execute(create_table_query)
    connection.commit()
    print("Table created successfully in PostgreSQL ")

except (Exception, Error) as error:
    print(f"Error: {error}")
    if 'connection' in locals():
        connection.rollback()

finally:
    if 'connection' in locals():
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")