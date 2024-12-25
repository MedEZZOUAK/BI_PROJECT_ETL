import psycopg2
from psycopg2 import Error
import os
import pandas as pd
from pathlib import Path

base_dir = os.getcwd()
base_dir=base_dir+'/bi'
print("Base directory:", base_dir)

dfs = []
promo_folders = [f for f in os.listdir(base_dir) if f.startswith('promo_')]
print("Promo folders found:", promo_folders)

for folder in promo_folders:
    promo_year = folder.split('_')[1]
    student_file = Path(base_dir + '/' + folder + '/students.csv')
    print(f"Checking for file: {student_file}"+' promo year: '+promo_year)
    if student_file.exists():
        df = pd.read_csv(student_file)
        df['promo'] = promo_year
        dfs.append(df)
    else:
        print(f"File {student_file} does not exist")

if dfs:
    final_df = pd.concat(dfs)
    final_df.reset_index(drop=True, inplace=True)
    final_df = final_df.drop(columns=['ID','absence_bias'])
    #save as csv
    final_df.to_csv('students.csv', index=False)
    print(final_df)
else:
    print("No DataFrames to concatenate")

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
  cursor.execute("DROP TABLE IF EXISTS students")
  cursor.execute("""
        CREATE TABLE students (
            ID SERIAL PRIMARY KEY,
            APO INT UNIQUE NOT NULL,
            Nom TEXT,
            Prénom TEXT,
            Sexe CHAR(1),
            Birthdate DATE,
            Number_of_Ajournements INT,
            promo VARCHAR(4)
        )
    """)
  for index, row in final_df.iterrows():
    cursor.execute("""
            INSERT INTO students (APO, Nom, Prénom, Sexe, Birthdate, Number_of_Ajournements, promo)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
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

