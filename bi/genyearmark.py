import os
import pandas as pd

def load_modules(modules_file):
    """Loads the module data and categorizes them by academic year based on module codes."""
    modules = pd.read_csv(modules_file)

    # Ensure that the column names are stripped of any leading or trailing spaces
    modules.columns = modules.columns.str.strip()

    # Separate modules into three lists by year based on their codes
    year1_modules = [module for module in modules.itertuples() if module.code.startswith(('TC', 'GINF2'))]
    year2_modules = [module for module in modules.itertuples() if module.code.startswith(('GINF3', 'GINF4'))]
    year3_modules = [module for module in modules.itertuples() if module.code.startswith(('GINF5', 'GISIAD'))]

    return year1_modules, year2_modules, year3_modules

def process_student_marks(student, marks_output_dir, year1_modules, year2_modules, year3_modules):
    """Extracts and calculates marks for a student from the marks output directory."""
    # Extract values from the student Series directly
    student_apogee = student.get("Code APO")
    student_name = student.get("Nom", "")

    if student_apogee is None:
        print(f"Missing 'Code APO' for student {student_name}")
        return {}

    # Initialize lists for each year
    marks_year1 = []
    marks_year2 = []
    marks_year3 = []

    # Iterate through each file in the marks output directory
    for file_name in os.listdir(marks_output_dir):
        if file_name.startswith("note_") and file_name.endswith(".csv"):
            file_path = os.path.join(marks_output_dir, file_name)
            module_code = file_name.split('_')[1]  # Extract module code from the filename

            # Read the CSV file and strip column names
            module_df = pd.read_csv(file_path)
            module_df.columns = module_df.columns.str.strip()

            # Check if the 'Code APO' column exists in the current module's DataFrame
            if "apogee" not in module_df.columns:
                print(f"'Code APO' column not found in {file_path}")
                continue

            # Filter to find the student's mark in the module DataFrame
            student_marks = module_df[module_df["apogee"] == student_apogee]

            if not student_marks.empty:
                mark = student_marks.iloc[0]["mark"]  # Extract the mark column

                # Add the mark to the appropriate year list based on the module code
                if any(module_code == mod.code for mod in year1_modules):
                    marks_year1.append(mark)
                elif any(module_code == mod.code for mod in year2_modules):
                    marks_year2.append(mark)
                elif any(module_code == mod.code for mod in year3_modules):
                    marks_year3.append(mark)

    # Calculate pass/fail for each year
    pass_year1 = (sum(marks_year1) / len(marks_year1)) > 12 if marks_year1 else False
    pass_year2 = (sum(marks_year2) / len(marks_year2)) > 12 if marks_year2 else False
    pass_year3 = (sum(marks_year3) / len(marks_year3)) > 12 if marks_year3 else False

    # Calculate average marks for each year
    avg_year1 = sum(marks_year1) / len(marks_year1) if marks_year1 else None
    avg_year2 = sum(marks_year2) / len(marks_year2) if marks_year2 else None
    avg_year3 = sum(marks_year3) / len(marks_year3) if marks_year3 else None

    return {
        "apogee": student_apogee,
        "nom": student_name,
        "noteyear1": avg_year1,
        "pass_year1": pass_year1,
        "noteyear2": avg_year2,
        "pass_year2": pass_year2,
        "noteyear3": avg_year3,
        "pass_year3": pass_year3,
    }

def process_all_promos(modules_file):
    """Processes all promo directories and calculates yearly marks for each student."""
    current_directory = os.getcwd()
    promo_folders = [d for d in os.listdir(current_directory) if os.path.isdir(d) and d.startswith('promo_')]
    year1_modules, year2_modules, year3_modules = load_modules(modules_file)

    for folder in promo_folders:
        folder_path = os.path.join(current_directory, folder)
        marks_output_dir = os.path.join(folder_path, 'marks_output')

        if not os.path.exists(marks_output_dir):
            print(f"No 'marks_output' directory found in {folder_path}")
            continue

        # Read the student list
        student_file_path = os.path.join(folder_path, 'students.csv')
        if not os.path.exists(student_file_path):
            print(f"No student list found in {folder_path}")
            continue

        students_df = pd.read_csv(student_file_path)
        students_df.columns = students_df.columns.str.strip()  # Strip any whitespace from column names
        results = []

        # Process each student
        for _, student in students_df.iterrows():
            student_results = process_student_marks(student, marks_output_dir, year1_modules, year2_modules, year3_modules)
            if student_results:
                results.append(student_results)

        # Create DataFrame from results and save to CSV
        results_df = pd.DataFrame(results)
        output_file = os.path.join(folder_path, 'yearly_marks_summary.csv')
        results_df.to_csv(output_file, index=False, float_format="%.2f")

        print(f"Processed {folder_path}. Results saved to {output_file}.")

# Specify the path to your modules CSV file
modules_file = 'modules.csv'

# Run the processing for all promo folders
process_all_promos(modules_file)
