import os
import pandas as pd
import psycopg2
from typing import Dict, List

class DataProcessor:
  def __init__(self, base_dir: str, db_config: Dict[str, str]):
    """
    Initialize the data processor with base directory and database configuration.

    :param base_dir: Base directory containing promo folders
    :param db_config: Dictionary with database connection parameters
    """
    self.base_dir = base_dir
    self.db_config = db_config
    self.promo_module_dfs: Dict[str, Dict[str, pd.DataFrame]] = {}

  def _connect_to_database(self) -> psycopg2.extensions.connection:
    """
    Establish a connection to the PostgreSQL database.

    :return: Database connection object
    """
    try:
      return psycopg2.connect(**self.db_config)
    except psycopg2.Error as e:
      print(f"Error connecting to the database: {e}")
      raise

  def _load_promo_absence_data(self):
    """
    Load absence data from CSV files in promo folders.
    """
    # Find promo folders
    promo_folders = [f for f in os.listdir(self.base_dir)
                     if f.startswith('promo_')]

    for promo in promo_folders:
      promo_path = os.path.join(self.base_dir, promo, 'absence_files')

      if not os.path.exists(promo_path):
        print(f"Directory does not exist: {promo_path}")
        continue

      print(f"Processing promo folder: {promo}")
      module_dfs = {}

      # Process CSV files in the absence folder
      for file in os.listdir(promo_path):
        if file.endswith('.csv'):
          file_path = os.path.join(promo_path, file)
          df = pd.read_csv(file_path)

          # Calculate total absences
          df['total_absences'] = df.iloc[:, 3:].sum(axis=1)

          # Store DataFrame with selected columns
          module_name = os.path.splitext(file)[0]
          module_dfs[module_name] = df[['nom', 'prenom', 'apogee', 'total_absences']]

      self.promo_module_dfs[promo] = module_dfs

  def _merge_similar_modules(self):
    """
    Merge similar modules across different versions.
    """
    module_merges = {
  "Langues et Communication I": ["Langues et Communication I1", "Langues et Communication I2"],
  "Modélisation et Programmation Objet": ["Modélisation et Programmation Objet1", "Modélisation et Programmation Objet2"],
  "Technologies DotNet et JEE": ["Technologies DotNet et JEE1", "Technologies DotNet et JEE2"],
  "Système d'Intégration et Progiciel": ["Système d'Intégration et Progiciel1", "Système d'Intégration et Progiciel2"]
}

    for promo, modules in self.promo_module_dfs.items():
      for merged_name, modules_to_merge in module_merges.items():
        merged_dfs = []
        for module in modules_to_merge:
          full_module_name = f'absence_{module}'
          if full_module_name in modules:
            df = modules[full_module_name].copy()
            df['total_absences'] = df.iloc[:, 3:].sum(axis=1)
            merged_dfs.append(df)
            # Remove the original modules
            del modules[full_module_name]

        # Combine merged dataframes
        if merged_dfs:
          merged_df = pd.concat(merged_dfs, ignore_index=True)
          modules[f'absence_{merged_name}'] = merged_df

  def process_data(self):
    """
    Main data processing method.
    """
    # Load absence data
    self._load_promo_absence_data()

    # Merge similar modules
    self._merge_similar_modules()

    # Prepare final dataframe
    result_dfs = []
    for promo, modules in self.promo_module_dfs.items():
      for module, df in modules.items():
        df_copy = df.copy()
        df_copy['promo'] = promo
        df_copy['module'] = module.replace('absence_', '')
        result_dfs.append(df_copy)

    final_result_df = pd.concat(result_dfs, ignore_index=True)

    # Database connection and additional processing
    with self._connect_to_database() as conn:
      cursor = conn.cursor()

      # Fetch modules and students data
      cursor.execute("SELECT * FROM modules")
      modules_df = pd.DataFrame(cursor.fetchall(),
                                columns=[desc[0] for desc in cursor.description])

      cursor.execute("SELECT * FROM students")
      students_df = pd.DataFrame(cursor.fetchall(),
                                 columns=[desc[0] for desc in cursor.description])

    # Final data preparation
    final_result_df = final_result_df.dropna()
    final_result_df = final_result_df.drop(columns=['nom', 'prenom'])

    # Merge with modules and students data
    final_result_df = final_result_df.merge(
      modules_df,
      left_on='module',
      right_on='name',
      suffixes=('_absence', '_module')
    )

    final_result_df = final_result_df.merge(
      students_df,
      left_on='apogee',
      right_on='apogee',
      suffixes=('_absence', '_student')
    )

    # Clean up columns
    columns_to_drop = [
      'code', 'filiere', 'semester', 'code_apo',
      'nom', 'prenom', 'sexe', 'birthdate',
      'number_of_ajournements', 'promo',
      'module', 'name', 'coeff', 'apogee'
    ]
    final_result_df = final_result_df.drop(columns=columns_to_drop)

    # Rename and save
    final_result_df = final_result_df.rename(columns={'id_absence': 'module_id'})
    final_result_df.to_csv('absence_fact.csv', index=False)

    print("Data processing completed successfully.")

def main():
  # Database configuration
  db_config = {
    'host': 'localhost',
    'database': 'BI',
    'user': 'postgres',
    'password': 'root'
  }

  # Base directory containing promo folders
  base_dir = r'C:\Users\ezzou\OneDrive\Desktop\Bi data\bi'

  # Create and run data processor
  processor = DataProcessor(base_dir, db_config)
  processor.process_data()

if __name__ == '__main__':
  main()
