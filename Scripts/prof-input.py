import pandas as pd
import psycopg2
import os
from psycopg2 import Error
from pathlib import Path

csv_dir=os.getcwd()+'/bi/prof.csv'

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
  cursor.execute("DROP TABLE IF EXISTS prof")
  cursor.execute("""
        CREATE TABLE prof (
            ID INTEGER PRIMARY KEY NOT NULL,
            name TEXT,
            department TEXT,
            email TEXT)
            """)
  for index, row in prod_df.iterrows():
    cursor.execute("""
            INSERT INTO prof (ID, name, department, email)
            VALUES (%s, %s, %s, %s)
        """, tuple(row))

  connection.commit()
  print("Data inserted successfully")
  connection.commit()
  print("Table created successfully in PostgreSQL ")
except (Exception, Error) as error:
  print("Error while connecting to PostgreSQL", error)
finally:
  if connection:
    cursor.close()
    connection.close()
    print("PostgreSQL connection is closed")
