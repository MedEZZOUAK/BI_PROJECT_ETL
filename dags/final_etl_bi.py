import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import os
import pandas as pd

# Default arguments for the DAG
default_args = {'owner': 'Mohammed', 'start_date': datetime(2023, 1, 1), }

# Define the DAG
dag = DAG(dag_id="BI_ETL", schedule_interval='@daily', default_args=default_args, catchup=False)

# Base directory for promo data
base_dir = os.getenv('AIRFLOW_BASE_DIR', '/opt/airflow/bi_data')


def map_to_time_id(row, time_map):
  # Calculate base year (2016 for promo 2019)
  base_year = row['promo'] - 3
  to_year = base_year + 1
  if row['semester'] in ['S3', 'S4']:
    base_year += 1
    to_year += 1
  elif row['semester'] == 'S5':
    base_year += 2
    to_year += 2
  key = (f"{base_year}/{to_year}", row['semester'])
  time_id = time_map.get(key)
  return time_id


def get_absences_module_promo(**kwargs):
  try:
    promo_folders = [f for f in os.listdir(base_dir) if f.startswith('promo_')]
    logging.info('Promo folders found: %s', promo_folders)
    promo_module_dfs = {}
    for promo in promo_folders:
      promo_path = os.path.join(base_dir, promo, 'absence_files')
      if os.path.exists(promo_path):
        logging.info(f"Processing promo: {promo}")
        module_dfs = {}
        for file in os.listdir(promo_path):
          if file.endswith('.csv'):
            file_path = os.path.join(promo_path, file)
            try:
              logging.info(f"Processing file: {file_path}")
              df = pd.read_csv(file_path)
              df['total_absences'] = df.iloc[:, 3:].sum(axis=1)
              module_name = os.path.splitext(file)[0]
              module_dfs[module_name] = df[['nom', 'prenom', 'apogee', 'total_absences']]
            except Exception as e:
              logging.error(f"Failed to process file {file_path}: {e}")
        promo_module_dfs[promo] = module_dfs
      else:
        logging.warning(f"Absence files folder not found for promo: {promo}")
    # push the data to the next task
    kwargs['ti'].xcom_push(key='promo_module_dfs', value=promo_module_dfs)
  except Exception as e:
    logging.error(f"Error in get_absences_module_promo: {e}")


def extract_prof_module(**kwargs):
  prof_df = pd.read_csv(os.path.join(base_dir, 'prof.csv'))
  module_df = pd.read_csv(os.path.join(base_dir, 'modules.csv'))
  merged_df = pd.merge(module_df, prof_df, left_on='profID', right_on='id', suffixes=('_module', '_prof'))
  merged_df.drop(columns=['id_prof'], inplace=True)
  merged_df.drop(columns=['absence_bias', 'median_mark'], inplace=True)
  kwargs['ti'].xcom_push(key='modules_df', value=merged_df)
  kwargs['ti'].xcom_push(key='prof_df', value=prof_df)
  logging.info("modules_df and prof_df pushed to XCom")


def extract_students_marks(**kwargs):
  conn = psycopg2.connect(host="host.docker.internal", database="BI", user="postgres", password="root")
  cursor = conn.cursor()
  cursor.execute("SELECT * FROM students")
  students = cursor.fetchall()
  students = [student[:2] + student[3:] for student in students]
  students_df = pd.DataFrame(students,
                             columns=['id', 'apo', 'nom', 'prenom', 'sexe', 'birthdate', 'number_of_ajournements',
                                      'promo'])
  cursor.execute("SELECT * FROM marks")
  marks = cursor.fetchall()
  cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'marks'")
  columns = cursor.fetchall()
  columns = [column[0] for column in columns]
  marks_df = pd.DataFrame(marks, columns=columns)
  promo_module_dfs = kwargs['ti'].xcom_pull(key='promo_module_dfs', task_ids='get_absences')
  promo_module_keys = list(promo_module_dfs.keys())
  logging.info(promo_module_keys)
  # Push the DataFrames to XCom
  kwargs['ti'].xcom_push(key='students_df', value=students_df)
  kwargs['ti'].xcom_push(key='marks_df', value=marks_df)
  kwargs['ti'].xcom_push(key='promo_module_keys', value=promo_module_keys)
  logging.info("students_df and marks_df pushed to XCom")


def fix_absences_data(**kwargs):
  promo_module_dfs = kwargs['ti'].xcom_pull(key='promo_module_dfs', task_ids='get_absences')
  promo_module_keys = promo_module_dfs.keys()
  for promo in promo_module_keys:
    module_keys = promo_module_dfs[promo].keys()
    absence_Langues_et_Communication_I1 = promo_module_dfs[promo]['absence_Langues et Communication I1']
    absence_Langues_et_Communication_I2 = promo_module_dfs[promo]['absence_Langues et Communication I2']
    absence_Langues_et_Communication_I1['total_absences'] = absence_Langues_et_Communication_I1.iloc[:, 3:].sum(axis=1)
    absence_Langues_et_Communication_I2['total_absences'] = absence_Langues_et_Communication_I2.iloc[:, 3:].sum(axis=1)
    absence_Langues_et_Communication_I = pd.concat(
      [absence_Langues_et_Communication_I1, absence_Langues_et_Communication_I2])
    absence_Langues_et_Communication_I.drop(columns=['total_absences'], inplace=True)
    promo_module_dfs[promo]['absence_Langues et Communication I'] = absence_Langues_et_Communication_I
    del promo_module_dfs[promo]['absence_Langues et Communication I1']
    del promo_module_dfs[promo]['absence_Langues et Communication I2']
  for promo in promo_module_keys:
    module_keys = promo_module_dfs[promo].keys()
    absence_Langues_et_Communication_I1 = promo_module_dfs[promo]['absence_Technologies DotNet et JEE1']
    absence_Langues_et_Communication_I2 = promo_module_dfs[promo]['absence_Technologies DotNet et JEE2']
    absence_Langues_et_Communication_I1['total_absences'] = absence_Langues_et_Communication_I1.iloc[:, 3:].sum(axis=1)
    absence_Langues_et_Communication_I2['total_absences'] = absence_Langues_et_Communication_I2.iloc[:, 3:].sum(axis=1)
    absence_Langues_et_Communication_I['total_absences'] = absence_Langues_et_Communication_I1['total_absences'] + \
                                                           absence_Langues_et_Communication_I2['total_absences']
    promo_module_dfs[promo]['absence_Technologies DotNet et JEE'] = absence_Langues_et_Communication_I
    del promo_module_dfs[promo]['absence_Technologies DotNet et JEE1']
    del promo_module_dfs[promo]['absence_Technologies DotNet et JEE2']
  for promo in promo_module_keys:
    module_keys = promo_module_dfs[promo].keys()
    absence_Langues_et_Communication_I1 = promo_module_dfs[promo]['absence_Système d’Intégration et Progiciel1']
    absence_Langues_et_Communication_I2 = promo_module_dfs[promo]['absence_Système d’Intégration et Progiciel2']
    absence_Langues_et_Communication_I1['total_absences'] = absence_Langues_et_Communication_I1.iloc[:, 3:].sum(axis=1)
    absence_Langues_et_Communication_I2['total_absences'] = absence_Langues_et_Communication_I2.iloc[:, 3:].sum(axis=1)
    absence_Langues_et_Communication_I['total_absences'] = absence_Langues_et_Communication_I1['total_absences'] + \
                                                           absence_Langues_et_Communication_I2['total_absences']
    promo_module_dfs[promo]['absence_Système d’Intégration et Progiciel'] = absence_Langues_et_Communication_I
    del promo_module_dfs[promo]['absence_Système d’Intégration et Progiciel1']
    del promo_module_dfs[promo]['absence_Système d’Intégration et Progiciel2']
  kwargs['ti'].xcom_push(key='promo_module_dfs', value=promo_module_dfs)
  result_dfs = []
  for promo in promo_module_dfs.keys():
    for module in promo_module_dfs[promo].keys():
      df = promo_module_dfs[promo][module].copy()
      df['promo'] = promo
      df['module'] = module
      result_dfs.append(df)

  final_result_df = pd.concat(result_dfs, ignore_index=True)
  final_result_df['module'] = final_result_df['module'].str.replace('absence_', '')
  final_result_df.dropna(inplace=True)
  final_result_df = final_result_df.drop(columns=['nom', 'prenom'])
  conn = psycopg2.connect(host="host.docker.internal", database="BI", user="postgres", password="root")
  cursor = conn.cursor()
  cursor.execute("SELECT * FROM modules")
  modules = cursor.fetchall()
  columns = [column[0] for column in cursor.description]
  modules_df = pd.DataFrame(modules, columns=columns)
  if modules_df is not None:
    final_result_df = final_result_df.merge(modules_df, left_on='module', right_on='name',
                                            suffixes=('_absence', '_module'))
    kwargs['ti'].xcom_push(key='final_result_df', value=final_result_df)
  else:
    logging.error("modules_df is None, cannot merge")


def transform_data(**kwargs):
  conn = psycopg2.connect(host="host.docker.internal", database="BI", user="postgres", password="root")
  cursor = conn.cursor()
  cursor.execute("SELECT * FROM students")
  students = cursor.fetchall()
  columns = [column[0] for column in cursor.description]
  students_df = pd.DataFrame(students, columns=columns)
  final_result_df = kwargs['ti'].xcom_pull(key='final_result_df', task_ids='fix_absences_data')
  final_result_df = final_result_df.merge(students_df, left_on='apogee', right_on='apogee',
                                          suffixes=('_absence', '_student'))
  final_result_df.drop(
    columns=['code', 'filiere', 'semester', 'code_apo', 'nom', 'prenom', 'sexe', 'birthdate', 'number_of_ajournements',
             'promo'], inplace=True)
  final_result_df.drop(columns=['module', 'name', 'coeff'], inplace=True)
  final_result_df.rename(columns={'id_absence': 'module_id'}, inplace=True)
  final_result_df.drop(columns=['apogee'], inplace=True)
  cursor.execute("SELECT * FROM marks")
  marks = cursor.fetchall()
  columns = [column[0] for column in cursor.description]
  marks_df = pd.DataFrame(marks, columns=columns)
  final_result_df = final_result_df.merge(students_df, left_on='id_student', right_on='id',
                                          suffixes=('_absence', '_student'))
  final_result_df.drop(
    columns=['id_student', 'id', 'sexe', 'birthdate', 'number_of_ajournements', 'promo_year_student'], inplace=True)
  final_result_df.drop(columns=['nom', 'prenom'], inplace=True)
  final_result_df.rename(columns={'promo_year_absence': 'promo'}, inplace=True)
  final_result_df.drop(columns=['code_apo'], inplace=True)
  merged_df = pd.merge(final_result_df, marks_df, left_on=['apogee', 'module_id'],
                       right_on=['student_apogee', 'module_id'])
  merged_df.drop(columns=['student_apogee'], inplace=True)
  merged_df.drop(columns=['is_split2', 'mark_submodule1', 'mark_submodule2'], inplace=True)
  # Save the DataFrame to CSV
  merged_df.to_csv('final_data.csv', index=False)
  kwargs['ti'].xcom_push(key='final_result_df', value=merged_df)
  logging.info("final_result_df pushed to XCom")


# load data to dw
def prepare_dw():
  conn = psycopg2.connect(host="host.docker.internal", database="BI_DW", user="postgres", password="root")
  cursor = conn.cursor()
  # clean the DW (dim_student,dim_module,dim_professor,dim_time,fact_student_performance)
  # if the tables doesn't exist create them
  cursor.execute("DROP TABLE IF EXISTS dim_student CASCADE")
  cursor.execute("DROP TABLE IF EXISTS dim_module CASCADE")
  cursor.execute("DROP TABLE IF EXISTS dim_professor CASCADE")
  cursor.execute("DROP TABLE IF EXISTS dim_time CASCADE")
  cursor.execute("DROP TABLE IF EXISTS fact_student_performance CASCADE")
  cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_student (
        student_id SERIAL PRIMARY KEY,
        apogee VARCHAR(50) NOT NULL UNIQUE,
        nom VARCHAR(50) NOT NULL,
        prenom VARCHAR(50) NOT NULL,
        promo VARCHAR(50) NOT NULL,
        sexe CHAR(1) NOT NULL,
        birthdate DATE NOT NULL,
        number_of_ajournements INTEGER NOT NULL
    );
    """)
  cursor.execute("""
  CREATE TABLE IF NOT EXISTS dim_module (
      module_id SERIAL PRIMARY KEY,
      code VARCHAR(50) NOT NULL,
      name VARCHAR(100) NOT NULL,
      filiere VARCHAR(100) NOT NULL,
      coeff NUMERIC(5,2) NOT NULL,
      semester VARCHAR(50) NOT NULL
  );
  """)
  cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_professor (
        prof_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        departement VARCHAR(100) NOT NULL,
        email VARCHAR(100) NOT NULL
    );
    """)
  cursor.execute("""
  CREATE TABLE IF NOT EXISTS dim_time (
      time_id SERIAL PRIMARY KEY,
      year VARCHAR(50) NOT NULL,
      semester VARCHAR(50) NOT NULL
  );
  """)
  cursor.execute("""
  CREATE TABLE IF NOT EXISTS fact_student_performance (
      fact_id SERIAL PRIMARY KEY,
      total_absences FLOAT NOT NULL,
      module_id INTEGER NOT NULL,
      prof_id INTEGER NOT NULL,
      apogee TEXT NOT NULL,
      time_id INTEGER NOT NULL,
      mark FLOAT NOT NULL
  );
  """)
  conn.commit()
  cursor.close()
  conn.close()


def load_data_dw(**kwargs):
  conn = psycopg2.connect(host="host.docker.internal", database="BI_DW", user="postgres", password="root")
  cursor = conn.cursor()

  students_df = kwargs['ti'].xcom_pull(key='students_df', task_ids='get_marks_and_students')
  if students_df is not None:
    students_df['apo'] = students_df['apo'].astype(int)
    students_df['nom'] = students_df['nom'].astype(str)
    students_df['prenom'] = students_df['prenom'].astype(str)
    students_df['sexe'] = students_df['sexe'].astype(str)
    students_df['birthdate'] = pd.to_datetime(students_df['birthdate'], format='%Y-%m-%d')
    students_df['number_of_ajournements'] = students_df['number_of_ajournements'].astype(int)
    students_df['promo_year'] = students_df['promo'].astype(int)
    students_df.drop_duplicates(subset=['apo'], inplace=True)
    for index, row in students_df.iterrows():
      cursor.execute("""
            INSERT INTO dim_student (apogee, nom, prenom, promo, sexe, birthdate, number_of_ajournements)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (apogee) DO NOTHING
            """, (row['apo'], row['nom'], row['prenom'], row['promo_year'], row['sexe'], row['birthdate'],
                  row['number_of_ajournements']))
    conn.commit()
    logging.info("Data loaded to dim_student")
  else:
    logging.error("students_df is None, cannot load data to dim_student")

  modules_df = kwargs['ti'].xcom_pull(key='modules_df', task_ids='load_prof_module')
  if modules_df is not None:
    modules_df['id_module'] = modules_df['id_module'].astype(int)
    modules_df['code'] = modules_df['code'].astype(str)
    modules_df['name_module'] = modules_df['name_module'].astype(str)
    modules_df['filiere'] = modules_df['filiere'].astype(str)
    modules_df['coeff'] = modules_df['coeff'].astype(float)
    modules_df['semester'] = modules_df['semester'].astype(str)
    modules_df.drop(columns=['profID'], inplace=True)
    for index, row in modules_df.iterrows():
      cursor.execute("""
            INSERT INTO dim_module (code, name, filiere, coeff, semester)
            VALUES (%s, %s, %s, %s, %s)
            """, (row['code'], row['name_module'], row['filiere'], row['coeff'], row['semester']))
    conn.commit()
    logging.info("Data loaded to dim_module")
  else:
    logging.error("modules_df is None, cannot load data to dim_module")
  prof_df = kwargs['ti'].xcom_pull(key='prof_df', task_ids='load_prof_module')
  if prof_df is not None:
    prof_df['id'] = prof_df['id'].astype(int)
    prof_df['name'] = prof_df['name'].astype(str)
    prof_df['departement'] = prof_df['departement'].astype(str)
    prof_df['email'] = prof_df['email'].astype(str)
    for index, row in prof_df.iterrows():
      cursor.execute("""
            INSERT INTO dim_professor (name, departement, email)
            VALUES (%s, %s, %s)
            """, (row['name'], row['departement'], row['email']))
    conn.commit()
    logging.info("Data loaded to dim_professor")
  else:
    logging.error("prof_df is None, cannot load data to dim_professor")

  marks_df = kwargs['ti'].xcom_pull(key='marks_df', task_ids='get_marks_and_students')
  logging.info("Marks_df: %s", marks_df)

  final_result_df = kwargs['ti'].xcom_pull(key='final_result_df', task_ids='transform_data')
  # Time dimension data
  data = [('2016/2017', 'S1'), ('2016/2017', 'S2'), ('2017/2018', 'S1'), ('2017/2018', 'S2'), ('2017/2018', 'S3'),
    ('2017/2018', 'S4'), ('2018/2019', 'S1'), ('2018/2019', 'S2'), ('2018/2019', 'S3'), ('2018/2019', 'S4'),
    ('2018/2019', 'S5'), ('2019/2020', 'S1'), ('2019/2020', 'S2'), ('2019/2020', 'S3'), ('2019/2020', 'S4'),
    ('2019/2020', 'S5'), ('2020/2021', 'S1'), ('2020/2021', 'S2'), ('2020/2021', 'S3'), ('2020/2021', 'S4'),
    ('2020/2021', 'S5'), ('2021/2022', 'S1'), ('2021/2022', 'S2'), ('2021/2022', 'S3'), ('2021/2022', 'S4'),
    ('2021/2022', 'S5'), ('2022/2023', 'S1'), ('2022/2023', 'S2'), ('2022/2023', 'S3'), ('2022/2023', 'S4'),
    ('2022/2023', 'S5'), ('2023/2024', 'S1'), ('2023/2024', 'S2'), ('2023/2024', 'S3'), ('2023/2024', 'S4'),
    ('2023/2024', 'S5')]

  time_df = pd.DataFrame(data, columns=['year', 'semester'])
  time_df['year'] = time_df['year'].astype(str)
  time_df['semester'] = time_df['semester'].astype(str)
  time_df['time_id'] = time_df.index + 1  # Add index column
  time_df.set_index('time_id', inplace=True)
  logging.info("Final result df Columns: %s", final_result_df.columns)
  logging.info("Modules df Columns: %s", modules_df.columns)
  final_result_df = final_result_df.merge(modules_df[['id_module', 'semester']], left_on='module_id',
                                          right_on='id_module')
  logging.info("Final_result_df: %s", final_result_df)
  time_map = {}
  for index, row in time_df.iterrows():
    time_map[(row['year'], row['semester'])] = index

  logging.info("Final_result_df columns : %s", final_result_df.columns)

  if final_result_df is not None:
    final_result_df['total_absences'] = final_result_df['total_absences'].astype(float)
    final_result_df['module_id'] = final_result_df['module_id'].astype(int)
    final_result_df['prof_id'] = final_result_df['prof_id'].astype(int)
    final_result_df['apogee'] = final_result_df['apogee'].astype(str)
    final_result_df['time_id'] = final_result_df.apply(lambda row: map_to_time_id(row, time_map), axis=1)
    final_result_df['time_id'] = final_result_df['time_id'].astype(int)
    final_result_df['mark']=final_result_df['mark'].astype(float)
    for index, row in final_result_df.iterrows():
      cursor.execute("""
            INSERT INTO fact_student_performance (total_absences, module_id, prof_id, apogee, time_id, mark)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, (row['total_absences'], row['module_id'], row['prof_id'], row['apogee'], row['time_id'], row['mark']))

    conn.commit()
    logging.info("Data loaded to fact_student_performance")
  else:
    logging.error("final_result_df is None, cannot load data to fact_student_performance")
  for index, row in time_df.iterrows():
    cursor.execute("""
        INSERT INTO dim_time (year, semester)
        VALUES (%s, %s)
        """, (row['year'], row['semester']))
  conn.commit()
  logging.info("Data loaded to dim_time")
  # adding some constraints
  cursor.execute("""ALTER TABLE public.fact_student_performance
        ADD CONSTRAINT fk_fact_student_apogee
        FOREIGN KEY (apogee)
        REFERENCES public.dim_student (apogee)
        ON UPDATE CASCADE
        ON DELETE CASCADE;
        """)
  cursor.execute("""ALTER TABLE public.fact_student_performance
        ADD CONSTRAINT fk_fact_student_module_id
        FOREIGN KEY (module_id)
        REFERENCES public.dim_module (module_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE;
        """)
  cursor.execute("""ALTER TABLE public.fact_student_performance
        ADD CONSTRAINT fk_fact_student_prof_id
        FOREIGN KEY (prof_id)
        REFERENCES public.dim_professor (prof_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE;
        """)
  cursor.execute("""ALTER TABLE public.fact_student_performance
        ADD CONSTRAINT fk_fact_student_time_id
        FOREIGN KEY (time_id)
        REFERENCES public.dim_time (time_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE;
        """)
  cursor.execute("""ALTER TABLE public.dim_student
        ADD CONSTRAINT unique_apogee UNIQUE (apogee);
        """)
  cursor.close()
  conn.commit()
  conn.close()


with dag:
  get_absences = PythonOperator(task_id='get_absences', python_callable=get_absences_module_promo)
  get_prof_modules = PythonOperator(task_id='load_prof_module', python_callable=extract_prof_module)
  get_students_marks = PythonOperator(task_id="get_marks_and_students", python_callable=extract_students_marks)
  fix_absences_data = PythonOperator(task_id='fix_absences_data', python_callable=fix_absences_data)
  transform_data = PythonOperator(task_id='transform_data', python_callable=transform_data)
  prepare_dw = PythonOperator(task_id='prepare_dw', python_callable=prepare_dw)
  load_data_dw = PythonOperator(task_id='load_data_dw', python_callable=load_data_dw)

  # task dependencies
  get_absences >> [get_prof_modules,
                   get_students_marks] >> fix_absences_data >> transform_data >> prepare_dw >> load_data_dw
