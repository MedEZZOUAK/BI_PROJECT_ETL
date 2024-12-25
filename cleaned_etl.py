import os
import pandas as pd
import psycopg2
from typing import Dict, List

def load_promo_absence_data(base_dir: str) -> Dict[str, Dict[str, pd.DataFrame]]:
    promo_folders = [f for f in os.listdir(base_dir) if f.startswith('promo_')]
    print("Promo folders found:", promo_folders)

    promo_module_dfs = {}

    for promo in promo_folders:
        promo_path = os.path.join(base_dir, promo, 'absence_files')

        if not os.path.exists(promo_path):
            print(f"Directory does not exist: {promo_path}")
            continue

        print("Processing promo folder:", promo)
        module_dfs = {}

        for file in os.listdir(promo_path):
            if file.endswith('.csv'):
                file_path = os.path.join(promo_path, file)
                print("Processing file:", file_path)

                df = pd.read_csv(file_path)
                print(f"DataFrame shape: {df.shape}")
                print(f"DataFrame columns: {df.columns.tolist()}")

                df['total_absences'] = df.iloc[:, 3:].sum(axis=1)
                module_name = os.path.splitext(file)[0]
                module_dfs[module_name] = df[['nom', 'prenom', 'apogee', 'total_absences']]

        promo_module_dfs[promo] = module_dfs

    return promo_module_dfs

def consolidate_module_absences(promo_module_dfs: Dict[str, Dict[str, pd.DataFrame]]) -> Dict[str, Dict[str, pd.DataFrame]]:
    module_consolidation_map = {
        'Langues et Communication I': ['Langues et Communication I1', 'Langues et Communication I2'],
        'Modélisation et Programmation Objet': ['Modélisation et Programmation Objet1', 'Modélisation et Programmation Objet2'],
        'Technologies DotNet et JEE': ['Technologies DotNet et JEE1', 'Technologies DotNet et JEE2'],
        'Système d\'Intégration et Progiciel': ['Système d\'Intégration et Progiciel1', 'Système d\'Intégration et Progiciel2']
    }

    for promo in promo_module_dfs.keys():
        for consolidated_name, split_modules in module_consolidation_map.items():
            if all(f'absence_{module}' in promo_module_dfs[promo] for module in split_modules):
                consolidated_df = pd.concat([
                    promo_module_dfs[promo][f'absence_{module}']
                    for module in split_modules
                ])

                promo_module_dfs[promo][f'absence_{consolidated_name}'] = consolidated_df
                for module in split_modules:
                    del promo_module_dfs[promo][f'absence_{module}']

    return promo_module_dfs

def prepare_final_absences_dataframe(promo_module_dfs: Dict[str, Dict[str, pd.DataFrame]]) -> pd.DataFrame:
    result_dfs = []

    for promo, modules in promo_module_dfs.items():
        for module, df in modules.items():
            module_df = df.copy()
            module_df['promo'] = promo
            module_df['module'] = module.replace('absence_', '')
            result_dfs.append(module_df)

    final_result_df = pd.concat(result_dfs, ignore_index=True)
    return final_result_df

def main():
    BASE_DIR = r'C:\Users\ezzou\OneDrive\Desktop\Bi data\bi'

    try:
        promo_module_dfs = load_promo_absence_data(BASE_DIR)
        promo_module_dfs = consolidate_module_absences(promo_module_dfs)
        final_result_df = prepare_final_absences_dataframe(promo_module_dfs)
        print(f"Final result DataFrame shape: {final_result_df.shape}")
        print(f"Final result DataFrame columns: {final_result_df.columns.tolist()}")

        op_conn = psycopg2.connect(
            host="localhost",
            database="BI",
            user="postgres",
            password="root"
        )
        op_cursor = op_conn.cursor()

        prof_df = pd.read_csv('./bi/prof.csv')
        module_df = pd.read_csv('./bi/modules.csv')

        dw_conn = psycopg2.connect(
            host="localhost",
            database="BI_DW",
            user="postgres",
            password="root"
        )
        dw_cursor = dw_conn.cursor()

        dw_cursor.execute("""
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

            CREATE TABLE IF NOT EXISTS dim_module (
                module_id SERIAL PRIMARY KEY,
                code VARCHAR(50) NOT NULL,
                name VARCHAR(100) NOT NULL,
                filiere VARCHAR(100) NOT NULL,
                coeff NUMERIC(5,2) NOT NULL,
                semester VARCHAR(50) NOT NULL,
                prof_id INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_professor (
                prof_id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                departement VARCHAR(100) NOT NULL,
                email VARCHAR(100) NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_time (
                time_id SERIAL PRIMARY KEY,
                year VARCHAR(50) NOT NULL,
                semester VARCHAR(50) NOT NULL
            );

            CREATE TABLE IF NOT EXISTS fact_student_performance (
                fact_id SERIAL PRIMARY KEY,
                total_absences FLOAT NOT NULL,
                module_id INTEGER NOT NULL,
                prof_id INTEGER NOT NULL,
                promo INTEGER NOT NULL,
                apogee INTEGER NOT NULL
            );
        """)

        op_cursor.execute("SELECT * FROM students")
        students = op_cursor.fetchall()

        students_df = pd.DataFrame(students, columns=['id', 'apogee', 'nom', 'prenom', 'sexe', 'birthdate', 'number_of_ajournements', 'promo'])

        for _, row in students_df.iterrows():
            dw_cursor.execute("""
                INSERT INTO dim_student (apogee, nom, prenom, promo, sexe, birthdate, number_of_ajournements)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (row['apogee'], row['nom'], row['prenom'], row['promo'], row['sexe'], row['birthdate'], row['number_of_ajournements']))

        for _, row in module_df.iterrows():
            dw_cursor.execute("""
                INSERT INTO dim_module (code, name, filiere, coeff, semester, prof_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                """, (row['code'], row['name'], row['filiere'], row['coeff'], row['semester'], row['prof_id']))

        for _, row in prof_df.iterrows():
            dw_cursor.execute("""
                INSERT INTO dim_professor (name, departement, email)
                VALUES (%s, %s, %s)
                """, (row['name'], row['departement'], row['email']))

        merged_df = final_result_df.merge(module_df, left_on='module', right_on='name')

        for _, row in merged_df.iterrows():
            dw_cursor.execute("""
                INSERT INTO fact_student_performance (total_absences, module_id, prof_id, promo, apogee)
                VALUES (%s, %s, %s, %s, %s)
                """, (row['total_absences'], row['id'], row['prof_id'],
                      int(row['promo'].replace('promo_', '')), row['apogee']))

        dw_conn.commit()

        print("ETL process completed successfully!")

    except Exception as e:
        print(f"An error occurred during ETL process: {e}")

    finally:
        if 'op_conn' in locals():
            op_conn.close()
        if 'dw_conn' in locals():
            dw_conn.close()

if __name__ == '__main__':
    main()
