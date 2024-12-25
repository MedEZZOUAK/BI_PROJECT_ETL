import json
import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import os
import pandas as pd
import psycopg2
import logging


def promo_modules_df(base_dir='/opt/airflow/bi_data'):
  logging.info('base_dir: %s', base_dir)
  promo_folders = [f for f in os.listdir(base_dir) if f.startswith('promo_')]
  promo_module_dfs = {}
  for promo in promo_folders:
    promo_path = os.path.join(base_dir, promo, 'absence_files')
    if os.path.exists(promo_path):
      module_dfs = {}
      for file in os.listdir(promo_path):
        if file.endswith('.csv'):
          file_path = os.path.join(promo_path, file)
          df = pd.read_csv(file_path)
          df['total_absences'] = df.iloc[:, 3:].sum(axis=1)
          module_name = os.path.splitext(file)[0]
          module_dfs[module_name] = df[['nom', 'prenom', 'apogee', 'total_absences']].to_dict(orient='records')
      promo_module_dfs[promo] = module_dfs
  return promo_module_dfs  # JSON-serializable dictionary



def prof_modules_dfs(prof_file='/opt/airflow/bi_data/prof.csv', module_file='/opt/airflow/bi_data/modules.csv'):
  prof_df = pd.read_csv(prof_file)
  module_df = pd.read_csv(module_file)
  merged_df = pd.merge(module_df, prof_df, left_on='profID', right_on='id', suffixes=('_module', '_prof'))
  merged_df.drop(columns=['id_prof', 'absence_bias', 'median_mark'], inplace=True)
  return merged_df, prof_df, module_df


def student_df():
  conn = psycopg2.connect(
    host="host.docker.internal",
    database="BI",
    user="postgres",
    password="root"
  )
  cursor = conn.cursor()
  cursor.execute("SELECT * FROM students")
  students = cursor.fetchall()
  students = [student[:2] + student[3:] for student in students]
  students_df = pd.DataFrame(students, columns=['id', 'apo', 'nom', 'prenom', 'sexe', 'birthdate', 'number_of_ajournements', 'promo'])
  conn.close()
  return students_df


def marks_df():
  conn = psycopg2.connect(
    host="host.docker.internal",
    database="BI",
    user="postgres",
    password="root"
  )
  cursor = conn.cursor()
  cursor.execute("SELECT * FROM marks")
  marks = cursor.fetchall()
  cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'marks'")
  columns = [column[0] for column in cursor.fetchall()]
  marks_df = pd.DataFrame(marks, columns=columns)
  conn.close()
  return marks_df


def fix_absences(promo_modules_data):
  promo_modules_dfs = json.loads(promo_modules_data)
  promo_module_keys = promo_modules_dfs.keys()
  for promo in promo_module_keys:
    module_keys = promo_modules_dfs[promo].keys()
    absence_Langues_et_Communication_I1 = promo_modules_dfs[promo]['absence_Langues et Communication I1']
    absence_Langues_et_Communication_I2 = promo_modules_dfs[promo]['absence_Langues et Communication I2']
    absence_Langues_et_Communication_I1['total_absences'] = absence_Langues_et_Communication_I1.iloc[:, 3:].sum(axis=1)
    absence_Langues_et_Communication_I2['total_absences'] = absence_Langues_et_Communication_I2.iloc[:, 3:].sum(axis=1)
    absence_Langues_et_Communication_I = pd.concat([absence_Langues_et_Communication_I1, absence_Langues_et_Communication_I2])
    absence_Langues_et_Communication_I.drop(columns=['total_absences'], inplace=True)
    promo_modules_dfs[promo]['absence_Langues et Communication I'] = absence_Langues_et_Communication_I
    del promo_modules_dfs[promo]['absence_Langues et Communication I1']
    del promo_modules_dfs[promo]['absence_Langues et Communication I2']
  for promo in promo_module_keys:
    module_keys = promo_modules_dfs[promo].keys()
    absence_Langues_et_Communication_I1 = promo_modules_dfs[promo]['absence_Modélisation et Programmation Objet1']
    absence_Langues_et_Communication_I2 = promo_modules_dfs[promo]['absence_Modélisation et Programmation Objet2']
    absence_Langues_et_Communication_I1['total_absences'] = absence_Langues_et_Communication_I1.iloc[:, 3:].sum(axis=1)
    absence_Langues_et_Communication_I2['total_absences'] = absence_Langues_et_Communication_I2.iloc[:, 3:].sum(axis=1)
    absence_Langues_et_Communication_I['total_absences'] = absence_Langues_et_Communication_I1['total_absences'] + absence_Langues_et_Communication_I2['total_absences']
    promo_modules_dfs[promo]['absence_Modélisation et Programmation Objet'] = absence_Langues_et_Communication_I
    del promo_modules_dfs[promo]['absence_Modélisation et Programmation Objet1']
    del promo_modules_dfs[promo]['absence_Modélisation et Programmation Objet2']
  for promo in promo_module_keys:
    module_keys = promo_modules_dfs[promo].keys()
    absence_Langues_et_Communication_I1 = promo_modules_dfs[promo]['absence_Technologies DotNet et JEE1']
    absence_Langues_et_Communication_I2 = promo_modules_dfs[promo]['absence_Technologies DotNet et JEE2']
    absence_Langues_et_Communication_I1['total_absences'] = absence_Langues_et_Communication_I1.iloc[:, 3:].sum(axis=1)
    absence_Langues_et_Communication_I2['total_absences'] = absence_Langues_et_Communication_I2.iloc[:, 3:].sum(axis=1)
    absence_Langues_et_Communication_I ['total_absences'] = absence_Langues_et_Communication_I1['total_absences'] + absence_Langues_et_Communication_I2['total_absences']
    promo_modules_dfs[promo]['absence_Technologies DotNet et JEE'] = absence_Langues_et_Communication_I
    del promo_modules_dfs[promo]['absence_Technologies DotNet et JEE1']
    del promo_modules_dfs[promo]['absence_Technologies DotNet et JEE2']
  for promo in promo_module_keys:
    module_keys = promo_modules_dfs[promo].keys()
    absence_Langues_et_Communication_I1 = promo_modules_dfs[promo]['absence_Système d’Intégration et Progiciel1']
    absence_Langues_et_Communication_I2 = promo_modules_dfs[promo]['absence_Système d’Intégration et Progiciel2']
    absence_Langues_et_Communication_I1['total_absences'] = absence_Langues_et_Communication_I1.iloc[:, 3:].sum(axis=1)
    absence_Langues_et_Communication_I2['total_absences'] = absence_Langues_et_Communication_I2.iloc[:, 3:].sum(axis=1)
    absence_Langues_et_Communication_I ['total_absences'] = absence_Langues_et_Communication_I1['total_absences'] + absence_Langues_et_Communication_I2['total_absences']
    promo_modules_dfs[promo]['absence_Système d’Intégration et Progiciel'] = absence_Langues_et_Communication_I
    del promo_modules_dfs[promo]['absence_Système d’Intégration et Progiciel1']
    del promo_modules_dfs[promo]['absence_Système d’Intégration et Progiciel2']
  return promo_modules_dfs  # Return modified data


def result_dfs(promo_module_dfs):
  result_dfs = []
  for promo, modules in promo_module_dfs.items():
    for module, df in modules.items():
      df_copy = df.copy()
      df_copy['promo'] = promo
      df_copy['module'] = module
      result_dfs.append(df_copy)
  final_result_df = pd.concat(result_dfs, ignore_index=True)
  final_result_df['module'] = final_result_df['module'].str.replace('absence_', '')
  final_result_df.dropna(inplace=True)
  final_result_df.drop(columns=['nom', 'prenom'], inplace=True)
  return final_result_df


def merge_modules(final_result_df, modules_file='/opt/airflow/bi_data/modules.csv'):
  modules_df = pd.read_csv(modules_file)
  return final_result_df.merge(modules_df, left_on='module', right_on='name', suffixes=('_absence', '_module'))


def process_students(final_result_df, students_df):
  merged_df = final_result_df.merge(students_df, left_on='apogee', right_on='apogee', suffixes=('_absence', '_student'))
  merged_df.drop(columns=['code', 'filiere', 'semester', 'code_apo', 'nom', 'prenom', 'sexe', 'birthdate', 'number_of_ajournements', 'promo'], inplace=True)
  merged_df.rename(columns={'id_absence': 'module_id'}, inplace=True)
  merged_df.drop(columns=['apogee'], inplace=True)
  return merged_df


def export_to_csv(df, file_name):
  df.to_csv(file_name, index=False)

# Default arguments for the DAG
default_args = {
  'owner': 'Mohammed',
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'start_date': datetime(2024, 12, 23),
}

# Define the DAG
with DAG(
  'student_performance_dag',
  default_args=default_args,
  description='A DAG to process student performance data',
  schedule_interval=None,
  catchup=False,
) as dag:

  # Task 1: Generate the promo modules data
  promo_modules_task = PythonOperator(
    task_id='promo_modules_df',
    python_callable=promo_modules_df,
    op_args=['/opt/airflow/bi_data'],
    provide_context=True,
    dag=dag,
  )

  # Task 2: Generate the professor modules data
  prof_modules_task = PythonOperator(
    task_id='prof_modules_dfs',
    python_callable=prof_modules_dfs,
    op_args=['/opt/airflow/bi_data/prof.csv', '/opt/airflow/bi_data/modules.csv'],  # Modify paths if needed
    dag=dag,
  )

  # Task 3: Fetch student data from database
  student_task = PythonOperator(
    task_id='student_df',
    python_callable=student_df,
    dag=dag,
  )

  # Task 4: Fetch marks data from database
  marks_task = PythonOperator(
    task_id='marks_df',
    python_callable=marks_df,
    dag=dag,
  )

  # Task 5: Fix absences in the promo module data
  fix_absences_task = PythonOperator(
    task_id='fix_absences',
    python_callable=fix_absences,
    op_args=['{{ ti.xcom_pull(task_ids="promo_modules_df") }}'],
    dag=dag,
  )

  # Task 6: Create the final result DataFrame from the promo data
  result_dfs_task = PythonOperator(
    task_id='result_dfs',
    python_callable=result_dfs,
    op_args=[promo_modules_task],  # Get the output from promo_modules_df task
    dag=dag,
  )

  # Task 7: Merge the final result with modules data
  merge_modules_task = PythonOperator(
    task_id='merge_modules',
    python_callable=merge_modules,
    op_args=[result_dfs_task, '/opt/airflow/bi_data/modules.csv'],  # Modify path if necessary
    dag=dag,
  )

  # Task 8: Process students with the final result data
  process_students_task = PythonOperator(
    task_id='process_students',
    python_callable=process_students,
    op_args=[result_dfs_task, student_task],  # Get output from result_dfs and student_df tasks
    dag=dag,
  )

  # Task 9: Export the final DataFrame to a CSV file
  export_to_csv_task = PythonOperator(
    task_id='export_to_csv',
    python_callable=export_to_csv,
    op_args=[process_students_task, 'final_student_performance.csv'],
    dag=dag,
  )

  # Task dependencies
  promo_modules_task >> prof_modules_task
  promo_modules_task >> student_task
  promo_modules_task >> marks_task
  promo_modules_task >> fix_absences_task
  promo_modules_task >> result_dfs_task
  result_dfs_task >> merge_modules_task
  result_dfs_task >> process_students_task
  process_students_task >> export_to_csv_task
