import os
import pandas as pd
import psycopg2

# Base directory
base_dir = r'C:\Users\ezzou\OneDrive\Desktop\Bi data\bi'

# Function to get promo folders
def get_promo_folders(base_dir):
    return [f for f in os.listdir(base_dir) if f.startswith('promo_')]

# Function to process absence files in a promo folder
def process_absence_files(promo_path):
    module_dfs = {}
    for file in os.listdir(promo_path):
        if file.endswith('.csv'):
            file_path = os.path.join(promo_path, file)
            df = pd.read_csv(file_path)
            df['total_absences'] = df.iloc[:, 3:].sum(axis=1)
            module_name = os.path.splitext(file)[0]
            module_dfs[module_name] = df[['nom', 'prenom', 'apogee', 'total_absences']]
    return module_dfs

# Function to process all promo folders
def process_promo_folders(base_dir):
    promo_folders = get_promo_folders(base_dir)
    promo_module_dfs = {}
    for promo in promo_folders:
        promo_path = os.path.join(base_dir, promo, 'absence_files')
        if os.path.exists(promo_path):
            promo_module_dfs[promo] = process_absence_files(promo_path)
        else:
            print(f"Directory does not exist: {promo_path}")
    return promo_module_dfs

# Function to merge absence data for specific modules
def merge_absence_data(promo_module_dfs, module_name):
    for promo in promo_module_dfs.keys():
        absence_1 = promo_module_dfs[promo][f'absence_{module_name}1']
        absence_2 = promo_module_dfs[promo][f'absence_{module_name}2']
        absence_1['total_absences'] = absence_1.iloc[:, 3:].sum(axis=1)
        absence_2['total_absences'] = absence_2.iloc[:, 3:].sum(axis=1)
        absence_combined = pd.concat([absence_1, absence_2])
        absence_combined.drop(columns=['total_absences'], inplace=True)
        promo_module_dfs[promo][f'absence_{module_name}'] = absence_combined
        del promo_module_dfs[promo][f'absence_{module_name}1']
        del promo_module_dfs[promo][f'absence_{module_name}2']

# Function to create a final result DataFrame
def create_final_result_df(promo_module_dfs):
    result_dfs = []
    for promo in promo_module_dfs.keys():
        for module in promo_module_dfs[promo].keys():
            df = promo_module_dfs[promo][module].copy()
            df['promo'] = promo
            df['module'] = module
            result_dfs.append(df)
    final_result_df = pd.concat(result_dfs, ignore_index=True)
    final_result_df['module'] = final_result_df['module'].str.replace('absence_', '')
    return final_result_df

# Function to load data from CSV files
def load_csv_data():
    prof_df = pd.read_csv('./bi/prof.csv')
    module_df = pd.read_csv('./bi/modules.csv')
    return prof_df, module_df

# Function to connect to PostgreSQL database
def connect_to_db(db_name):
    return psycopg2.connect(
        host="localhost",
        database=db_name,
        user="postgres",
        password="root"
    )

# Function to execute a query and fetch all results
def fetch_all(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()

# Function to create and insert data into a table
def create_and_insert_table(cursor, create_query, insert_query, data):
    cursor.execute(create_query)
    for row in data:
        cursor.execute(insert_query, row)
    conn.commit()

# Main ETL process
def main():
    promo_module_dfs = process_promo_folders(base_dir)
    
    # Merge absence data for specific modules
    merge_absence_data(promo_module_dfs, 'Langues et Communication I')
    merge_absence_data(promo_module_dfs, 'Modélisation et Programmation Objet')
    merge_absence_data(promo_module_dfs, 'Technologies DotNet et JEE')
    merge_absence_data(promo_module_dfs, 'Système d’Intégration et Progiciel')

    final_result_df = create_final_result_df(promo_module_dfs)
    prof_df, module_df = load_csv_data()

    # Connect to the PostgreSQL database
    conn = connect_to_db("BI")
    cursor = conn.cursor()

    # Fetch students and marks data
    students = fetch_all(cursor, "SELECT * FROM students")
    marks = fetch_all(cursor, "SELECT * FROM marks")

    # Transform students data into a DataFrame
    students_df = pd.DataFrame(students, columns=['id', 'apo', 'nom', 'prenom', 'sexe', 'birthdate', 'number_of_ajournements', 'promo'])

    # Transform marks data into a DataFrame
    columns = [column[0] for column in cursor.description]
    marks_df = pd.DataFrame(marks, columns=columns)

    # Merge and clean data
    final_result_df = final_result_df.merge(module_df, left_on='module', right_on='name', suffixes=('_absence', '_module'))
    final_result_df = final_result_df.merge(students_df, left_on='apogee', right_on='apogee', suffixes=('_absence', '_student'))
    final_result_df.drop(columns=['code', 'filiere', 'semester', 'code_apo', 'nom', 'prenom', 'sexe', 'birthdate', 'number_of_ajournements', 'promo'], inplace=True)
    final_result_df.rename(columns={'id_absence': 'module_id'}, inplace=True)
    final_result_df.drop(columns=['apogee'], inplace=True)
    final_result_df.to_csv('absence_fact.csv', index=False)

    # Merge final result with marks data
    merged_df = pd.merge(final_result_df, marks_df, left_on=['apogee', 'module_id'], right_on=['student_apogee', 'module_id'])
    merged_df.drop(columns=['student_apogee', 'is_split2', 'mark_submodule1', 'mark_submodule2'], inplace=True)
    merged_df.to_csv('fact_student_performance.csv', index=False)

    # Load data to the data warehouse
    conn_dw = connect_to_db("BI_DW")
    cursor_dw = conn_dw.cursor()

    # Create and insert data into dim_student table
    create_and_insert_table(cursor_dw, """
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
    """, """
        INSERT INTO dim_student (apogee, nom, prenom, promo, sexe, birthdate, number_of_ajournements)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, students_df.values)

    # Create and insert data into dim_module table
    create_and_insert_table(cursor_dw, """
        CREATE TABLE IF NOT EXISTS dim_module (
            module_id SERIAL PRIMARY KEY,
            code VARCHAR(50) NOT NULL,
            name VARCHAR(100) NOT NULL,
            filiere VARCHAR(100) NOT NULL,
            coeff NUMERIC(5,2) NOT NULL,
            semester VARCHAR(50) NOT NULL,
            prof_id INTEGER NOT NULL
        );
    """, """
        INSERT INTO dim_module (code, name, filiere, coeff, semester, prof_id)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, module_df.values)

    # Create and insert data into dim_professor table
    create_and_insert_table(cursor_dw, """
        CREATE TABLE IF NOT EXISTS dim_professor (
            prof_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            departement VARCHAR(100) NOT NULL,
            email VARCHAR(100) NOT NULL
        );
    """, """
        INSERT INTO dim_professor (name, departement, email)
        VALUES (%s, %s, %s)
    """, prof_df.values)

    # Create and insert data into dim_time table
    unique_years = marks_df['year'].unique()
    unique_semesters = module_df['semester'].unique()
    time_df = pd.DataFrame([(year, semester) for year in unique_years for semester in unique_semesters], columns=['year', 'semester'])
    create_and_insert_table(cursor_dw, """
        CREATE TABLE IF NOT EXISTS dim_time (
            time_id SERIAL PRIMARY KEY,
            year VARCHAR(50) NOT NULL,
            semester VARCHAR(50) NOT NULL
        );
    """, """
        INSERT INTO dim_time (year, semester)
        VALUES (%s, %s)
    """, time_df.values)

    # Create and insert data into fact_student_performance table
    create_and_insert_table(cursor_dw, """
        CREATE TABLE IF NOT EXISTS fact_student_performance (
            fact_id SERIAL PRIMARY KEY,
            total_absences FLOAT NOT NULL,
            module_id INTEGER NOT NULL,
            prof_id INTEGER NOT NULL,
            promo INTEGER NOT NULL,
            apogee INTEGER NOT NULL
        );
    """, """
        INSERT INTO fact_student_performance (total_absences, module_id, prof_id, promo, apogee)
        VALUES (%s, %s, %s, %s, %s)
    """, final_result_df.values)

if __name__ == "__main__":
    main()