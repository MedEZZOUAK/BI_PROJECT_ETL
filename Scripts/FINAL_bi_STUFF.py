#get all promos folder names
import logging
import os
import pandas as pd
import psycopg2
# Configure logging to write to a file
logging.basicConfig(filename='app.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
# base_dir='/opt/airflow/bi_data'
base_dir = r'C:\Users\ezzou\OneDrive\Desktop\Bi data\bi'
logging.info('base_dir: %s', base_dir)
promo_folders = [f for f in os.listdir(base_dir) if f.startswith('promo_')]
logging.info('promo_folders: %s', promo_folders)

promo_module_dfs = {}
for promo in promo_folders:
  promo_path = os.path.join(base_dir, promo, 'absence_files')
  if os.path.exists(promo_path):
    logging.info(f"Processing promo: {promo}")
    # Dictionary to store DataFrames for each module in the current promo
    module_dfs = {}
    # Iterate through each CSV file in the absences files folder
    for file in os.listdir(promo_path):
      if file.endswith('.csv'):
        file_path = os.path.join(promo_path, file)
        logging.info(f"Processing file: {file_path}")
        df = pd.read_csv(file_path)
        # Sum the absences for each student
        df['total_absences'] = df.iloc[:, 3:].sum(axis=1)
        # Store the DataFrame in the module_dfs dictionary
        module_name = os.path.splitext(file)[0]
        module_dfs[module_name] = df[['nom', 'prenom', 'apogee', 'total_absences']]

    # Store the module DataFrames in the promo_module_dfs dictionary
    promo_module_dfs[promo] = module_dfs
  else:
    logging.warning(f"Absences files folder not found for promo: {promo}")
#load the prof csv
prof_df = pd.read_csv(os.path.join(base_dir, 'prof.csv'))
#load the modules csv
module_df = pd.read_csv(os.path.join(base_dir, 'modules.csv'))
# Merge module_df and prof_df on the profID and id columns
merged_df = pd.merge(module_df, prof_df, left_on='profID', right_on='id', suffixes=('_module', '_prof'))
# Drop the redundant 'id_prof' column after the merge
merged_df.drop(columns=['id_prof'], inplace=True)
merged_df.drop(columns=['absence_bias','median_mark'], inplace=True)

#Connect to the PostgreSQL database
conn = psycopg2.connect(
  host="localhost",
  database="BI",
  user="postgres",
  password="root"
)
#Create a cursor object using the cursor() method
cursor = conn.cursor()
cursor.execute("SELECT * FROM students")
students = cursor.fetchall()
#drop the 4th element of each student tuple
students = [student[:2] + student[3:] for student in students]
#transform the students data into a pandas DataFrame
students_df = pd.DataFrame(students, columns=['id', 'apo', 'nom', 'prenom', 'sexe', 'birthdate', 'number_of_ajournements', 'promo'])
#get the marks data from the database
marks = cursor.fetchall()
#get the columns names
cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'marks'")
columns = cursor.fetchall()
columns = [column[0] for column in columns]
#transform the marks data into a pandas DataFrame
marks_df = pd.DataFrame(marks, columns=columns)
promo_module_keys=promo_module_dfs.keys()
for promo in promo_module_keys:
  logging.info(f"Processing promo: {promo}")
  module_keys = promo_module_dfs[promo].keys()
  absence_Langues_et_Communication_I1 = promo_module_dfs[promo]['absence_Langues et Communication I1']
  absence_Langues_et_Communication_I2 = promo_module_dfs[promo]['absence_Langues et Communication I2']
  absence_Langues_et_Communication_I1['total_absences'] = absence_Langues_et_Communication_I1.iloc[:, 3:].sum(axis=1)
  absence_Langues_et_Communication_I2['total_absences'] = absence_Langues_et_Communication_I2.iloc[:, 3:].sum(axis=1)
  absence_Langues_et_Communication_I = pd.concat([absence_Langues_et_Communication_I1, absence_Langues_et_Communication_I2])
  absence_Langues_et_Communication_I.drop(columns=['total_absences'], inplace=True)
  promo_module_dfs[promo]['absence_Langues et Communication I'] = absence_Langues_et_Communication_I
  del promo_module_dfs[promo]['absence_Langues et Communication I1']
  del promo_module_dfs[promo]['absence_Langues et Communication I2']
for promo in promo_module_keys:
  logging.info(f"Processing promo: {promo}")
  module_keys = promo_module_dfs[promo].keys()
  absence_Langues_et_Communication_I1 = promo_module_dfs[promo]['absence_Modélisation et Programmation Objet1']
  absence_Langues_et_Communication_I2 = promo_module_dfs[promo]['absence_Modélisation et Programmation Objet2']
  absence_Langues_et_Communication_I1['total_absences'] = absence_Langues_et_Communication_I1.iloc[:, 3:].sum(axis=1)
  absence_Langues_et_Communication_I2['total_absences'] = absence_Langues_et_Communication_I2.iloc[:, 3:].sum(axis=1)
  absence_Langues_et_Communication_I['total_absences'] = absence_Langues_et_Communication_I1['total_absences'] + absence_Langues_et_Communication_I2['total_absences']
  promo_module_dfs[promo]['absence_Modélisation et Programmation Objet'] = absence_Langues_et_Communication_I
  del promo_module_dfs[promo]['absence_Modélisation et Programmation Objet1']
  del promo_module_dfs[promo]['absence_Modélisation et Programmation Objet2']
for promo in promo_module_keys:
  module_keys = promo_module_dfs[promo].keys()
  absence_Langues_et_Communication_I1 = promo_module_dfs[promo]['absence_Technologies DotNet et JEE1']
  absence_Langues_et_Communication_I2 = promo_module_dfs[promo]['absence_Technologies DotNet et JEE2']
  absence_Langues_et_Communication_I1['total_absences'] = absence_Langues_et_Communication_I1.iloc[:, 3:].sum(axis=1)
  absence_Langues_et_Communication_I2['total_absences'] = absence_Langues_et_Communication_I2.iloc[:, 3:].sum(axis=1)
  absence_Langues_et_Communication_I ['total_absences'] = absence_Langues_et_Communication_I1['total_absences'] + absence_Langues_et_Communication_I2['total_absences']
  promo_module_dfs[promo]['absence_Technologies DotNet et JEE'] = absence_Langues_et_Communication_I
  del promo_module_dfs[promo]['absence_Technologies DotNet et JEE1']
  del promo_module_dfs[promo]['absence_Technologies DotNet et JEE2']
for promo in promo_module_keys:
  logging.info(f"Processing promo: {promo}")
  module_keys = promo_module_dfs[promo].keys()
  absence_Langues_et_Communication_I1 = promo_module_dfs[promo]['absence_Système d’Intégration et Progiciel1']
  absence_Langues_et_Communication_I2 = promo_module_dfs[promo]['absence_Système d’Intégration et Progiciel2']
  absence_Langues_et_Communication_I1['total_absences'] = absence_Langues_et_Communication_I1.iloc[:, 3:].sum(axis=1)
  absence_Langues_et_Communication_I2['total_absences'] = absence_Langues_et_Communication_I2.iloc[:, 3:].sum(axis=1)
  #sum the total_absences of absence_Système d’Intégration et Progiciel1 and absence_Système d’Intégration et Progiciel2
  absence_Langues_et_Communication_I ['total_absences'] = absence_Langues_et_Communication_I1['total_absences'] + absence_Langues_et_Communication_I2['total_absences']
  promo_module_dfs[promo]['absence_Système d’Intégration et Progiciel'] = absence_Langues_et_Communication_I
  del promo_module_dfs[promo]['absence_Système d’Intégration et Progiciel1']
  del promo_module_dfs[promo]['absence_Système d’Intégration et Progiciel2']
result_dfs = []
for promo in promo_module_dfs.keys():
  for module in promo_module_dfs[promo].keys():
    df = promo_module_dfs[promo][module].copy()
    df['promo'] = promo
    df['module'] = module
    result_dfs.append(df)
# Concatenate all the DataFrames into a single DataFrame
final_result_df = pd.concat(result_dfs, ignore_index=True)
final_result_df['module'] = final_result_df['module'].str.replace('absence_', '')
final_result_df.dropna(inplace=True)
final_result_df=final_result_df.drop(columns=['nom','prenom'])
#get the modules data from the database
cursor.execute("SELECT * FROM modules")
modules = cursor.fetchall()
columns = [column[0] for column in cursor.description]
modules_df = pd.DataFrame(modules, columns=columns)
# Check if all the modules in the final_result_df are in the modules_df
modules_in_final_result = final_result_df['module'].unique()
modules_in_modules_df = modules_df['name'].unique()
# Find modules that are not in the modules_df
missing_modules = [module for module in modules_in_final_result if module not in modules_in_modules_df]
# Print the missing modules
logging.info(f"Missing modules: {missing_modules}")
final_result_df = final_result_df.merge(modules_df, left_on='module', right_on='name', suffixes=('_absence', '_module'))
#get the students data from the database
cursor.execute("SELECT * FROM students")
students = cursor.fetchall()
columns = [column[0] for column in cursor.description]
students_df = pd.DataFrame(students, columns=columns)
#replace the apogee column with the id column from teh students_df
final_result_df = final_result_df.merge(students_df, left_on='apogee', right_on='apogee', suffixes=('_absence', '_student'))
final_result_df.drop(columns=['code','filiere','semester','code_apo','nom','prenom','sexe','birthdate','number_of_ajournements','promo'], inplace=True)
final_result_df.drop(columns=['module','name','coeff'], inplace=True)
#reanme id_absence to id_module
final_result_df.rename(columns={'id_absence': 'module_id'}, inplace=True)
final_result_df.drop(columns=['apogee'], inplace=True)
final_result_df.to_csv('absence_fact.csv', index=False)
# get marks data from the database
cursor.execute("SELECT * FROM marks")
marks = cursor.fetchall()
columns = [column[0] for column in cursor.description]
marks_df = pd.DataFrame(marks, columns=columns)
#add the apoge column to the final_result_df based on the student_id column
final_result_df = final_result_df.merge(students_df, left_on='id_student', right_on='id', suffixes=('_absence', '_student'))
final_result_df.drop(columns=['id_student','id','sexe','birthdate','number_of_ajournements','promo_year_student'], inplace=True)
#drop the nom prenom columns
final_result_df.drop(columns=['nom','prenom'], inplace=True)
#rename promo column to promo_year
final_result_df.rename(columns={'promo_year_absence': 'promo'}, inplace=True)
final_result_df.drop(columns=['code_apo'], inplace=True)
#print the columns of the marks_df and final_result_df
logging.info(f"Columns of marks_df: {marks_df.columns}")
logging.info(f"Columns of final_result_df: {final_result_df.columns}")
# Merge the two DataFrames on 'apogee' and 'module_id'
merged_df = pd.merge(final_result_df, marks_df, left_on=['apogee', 'module_id'], right_on=['student_apogee', 'module_id'])
# Drop the redundant 'student_apogee' column after the merge
merged_df.drop(columns=['student_apogee'], inplace=True)
merged_df.drop(columns=['is_split2','mark_submodule1','mark_submodule2'], inplace=True)
merged_df.to_csv('fact_student_performance.csv', index=False)
#load data to the DATAWAREHOUSE
cursor.close()
conn.close()
conn = psycopg2.connect(
  host="localhost",
  database="BI_DW",
  user="postgres",
  password="root"
)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_student (
    student_id SERIAL PRIMARY KEY,
    apogee VARCHAR(50) NOT NULL,
    nom VARCHAR(50) NOT NULL,
    prenom VARCHAR(50) NOT NULL,
    promo VARCHAR(50) NOT NULL,
    sexe CHAR(1) NOT NULL,
    birthdate DATE NOT NULL,
    number_of_ajournements INTEGER NOT NULL
);
""")
# Ensure the DataFrame columns have the correct data types
students_df['apogee'] = students_df['apogee'].astype(int)
students_df['nom'] = students_df['nom'].astype(str)
students_df['prenom'] = students_df['prenom'].astype(str)
students_df['sexe'] = students_df['sexe'].astype(str)
students_df['birthdate'] = pd.to_datetime(students_df['birthdate'], format='%Y-%m-%d')
students_df['number_of_ajournements'] = students_df['number_of_ajournements'].astype(int)
students_df['promo_year'] = students_df['promo_year'].astype(int)
# Insert the data into the dim_student table
for index, row in students_df.iterrows():
  cursor.execute("""
    INSERT INTO dim_student (apogee, nom, prenom, promo, sexe, birthdate, number_of_ajournements)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (row['apogee'], row['nom'], row['prenom'], row['promo_year'], row['sexe'], row['birthdate'], row['number_of_ajournements']))

# Commit the transaction
conn.commit()
# Ensure the DataFrame columns have the correct data types
modules_df['id'] = modules_df['id'].astype(int)
modules_df['code'] = modules_df['code'].astype(str)
modules_df['name'] = modules_df['name'].astype(str)
modules_df['filiere'] = modules_df['filiere'].astype(str)
modules_df['coeff'] = modules_df['coeff'].astype(float)
modules_df['prof_id'] = modules_df['prof_id'].astype(int)
modules_df['semester'] = modules_df['semester'].astype(str)
module_df.drop(columns=['absence_bias','median_mark'], inplace=True)
cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_module (
    module_id SERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL,
    name VARCHAR(100) NOT NULL,
    filiere VARCHAR(100) NOT NULL,
    coeff NUMERIC(5,2) NOT NULL,
    semester VARCHAR(50) NOT NULL,
    prof_id INTEGER NOT NULL
);
""")

# Insert the data into the dim_module table
for index, row in modules_df.iterrows():
  cursor.execute("""
    INSERT INTO dim_module (code, name, filiere, coeff, semester, prof_id)
    VALUES (%s, %s, %s, %s, %s, %s)
    """, (row['code'], row['name'], row['filiere'], row['coeff'], row['semester'], row['prof_id']))

# Commit the transaction
conn.commit()
# Ensure the DataFrame columns have the correct data types
prof_df['id'] = prof_df['id'].astype(int)
prof_df['name'] = prof_df['name'].astype(str)
prof_df['departement'] = prof_df['departement'].astype(str)
prof_df['email'] = prof_df['email'].astype(str)
# Create the dim_professor table
cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_professor (
    prof_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    departement VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL
);
""")

# Insert the data into the dim_professor table
for index, row in prof_df.iterrows():
  cursor.execute("""
    INSERT INTO dim_professor (name, departement, email)
    VALUES (%s, %s, %s)
    """, (row['name'], row['departement'], row['email']))

# Commit the transaction
conn.commit()
# Get unique values in the year column of the marks_df
unique_years = marks_df['year'].unique()

# Get unique values in the semester column of the module_df
unique_semesters = module_df['semester'].unique()

# Create a DataFrame for the time dimension table
time_df = pd.DataFrame([(year, semester) for year in unique_years for semester in unique_semesters], columns=['year', 'semester'])

# Convert the year column to string type
time_df['year'] = time_df['year'].astype(str)
# Create the dim_time table
cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    year VARCHAR(50) NOT NULL,
    semester VARCHAR(50) NOT NULL
);
""")

# Insert the data into the dim_time table
for index, row in time_df.iterrows():
  cursor.execute("""
    INSERT INTO dim_time (year, semester)
    VALUES (%s, %s)
    """, (row['year'], row['semester']))

# Commit the transaction
conn.commit()
# Ensure the DataFrame columns have the correct data types
final_result_df['total_absences'] = final_result_df['total_absences'].astype(float)
final_result_df['module_id'] = final_result_df['module_id'].astype(int)
final_result_df['prof_id'] = final_result_df['prof_id'].astype(int)
final_result_df['promo'] = final_result_df['promo'].astype(int)
final_result_df['apogee'] = final_result_df['apogee'].astype(int)
# Create the fact_student_performance table
cursor.execute("""
CREATE TABLE IF NOT EXISTS fact_student_performance (
    fact_id SERIAL PRIMARY KEY,
    total_absences FLOAT NOT NULL,
    module_id INTEGER NOT NULL,
    prof_id INTEGER NOT NULL,
    promo INTEGER NOT NULL,
    apogee INTEGER NOT NULL
);
""")

# Insert the data into the fact_student_performance table
for index, row in final_result_df.iterrows():
  cursor.execute("""
    INSERT INTO fact_student_performance (total_absences, module_id, prof_id, promo, apogee)
    VALUES (%s, %s, %s, %s, %s)
    """, (float(row['total_absences']), int(row['module_id']), int(row['prof_id']), int(row['promo']), int(row['apogee'])))

# Commit the transaction
conn.commit()

