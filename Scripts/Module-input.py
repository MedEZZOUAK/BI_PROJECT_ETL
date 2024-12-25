import pandas as pd
import psycopg2
import os
from psycopg2 import Error
from pathlib import Path

csv_dir = os.getcwd() + '/bi/modules.csv'

prod_df = pd.read_csv(csv_dir)
print(prod_df)

# Connect to an existing database
try:
    connection = psycopg2.connect(
        host="localhost",
        database="BI",
        user="postgres",
        password="root",
        port="5432"
    )
    cursor = connection.cursor()
    # Create a new record
    cursor.execute("DROP TABLE IF EXISTS modules")
    cursor.execute("""
        CREATE TABLE modules (
            id INTEGER PRIMARY KEY NOT NULL,
            code TEXT,
            name TEXT,
            filiere TEXT,
            coeff FLOAT,
            profID INTEGER,
            semester TEXT,
            CONSTRAINT fk_prof
                FOREIGN KEY(profID)
                REFERENCES prof(ID)
        )
    """)
    for index, row in prod_df.iterrows():
        cursor.execute("""
            INSERT INTO modules (id, code, name, filiere, coeff, profID, semester)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (row['id'], row['code'], row['name'], row['filiere'], row['coeff'], row['profID'], row['semester']))

    connection.commit()
    print("Data inserted successfully")
    print("Table created successfully in PostgreSQL")
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
