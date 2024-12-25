import pandas as pd
import numpy as np
import os


def load_csv(file_path):
    """Loads a CSV file and returns its content as a DataFrame."""
    return pd.read_csv(file_path)


def generate_marks(student, module):
    """Generate a mark for a student in a module based on various biases and elevate marks close to certain thresholds."""
    median = module["median_mark"]
    absence_bias = student["absence_bias"]
    ajournement = student["Number of Ajournements"]

    # Base mean around median mark
    mean = median - (ajournement * 0.5) - (absence_bias * 0.3)
    std_dev = 2.5  # Standard deviation

    # Add rare exceptions for very high or low scores
    if np.random.rand() < 0.05:  # 5% chance of rare exception
        if np.random.rand() < 0.5:
            mean += 5  # Exceptionally high
        else:
            mean -= 5  # Exceptionally low

    # Generate the mark with normal distribution
    mark = np.random.normal(mean, std_dev)

    # Clamp between 0 and 20
    mark = max(0, min(20, mark))

    # Round to 2 decimal places
    mark = round(mark, 2)

    # Elevate marks close to 8 or 12 if needed
    if mark < 8:
        mark += 1  # Slight increase for marks below 8
    elif 8 <= mark < 12:
        mark += 0  # Slight increase for marks between 8 and 12
    elif 12 <= mark < 14:
        mark += 0.25  # Slight increase for marks close to 12

    # Ensure the mark does not exceed 20 after adjustment
    mark = min(20, mark)

    # Determine if rattrapage is needed
    if mark < 12:
        rattrapage = True
        if mark == 12 and absence_bias > 0 and np.random.rand() < 0.5:
            rattrapage = True
        elif mark == 12:
            rattrapage = np.random.rand() < 0.2
    else:
        rattrapage = False

    # If rattrapage, cap mark at 12
    if rattrapage:
        mark = min(mark + np.random.uniform(0, 3), 12)
        mark = round(mark, 2)

    return mark, rattrapage


def generate_marks_files(promo_folder, modules):
    """Generates marks CSV files for each promo folder."""
    # Get the list of student files in the promo folder
    student_files = [f for f in os.listdir(promo_folder) if f.startswith('students') and f.endswith('.csv')]

    for student_file in student_files:
        student_path = os.path.join(promo_folder, student_file)
        students = load_csv(student_path)

        # Create output directory inside the promo folder
        output_dir = os.path.join(promo_folder, 'marks_output')
        os.makedirs(output_dir, exist_ok=True)

        for _, module in modules.iterrows():
            module_code = module["code"]
            module_name = module["name"]
            has_submodules = module_code.endswith("1")  # Example condition for submodules

            marks_data = []

            for _, student in students.iterrows():
                if has_submodules:
                    # Generate submodule marks
                    mark_sub1, _ = generate_marks(student, module)
                    mark_sub2, _ = generate_marks(student, module)
                    mark = round((mark_sub1 + mark_sub2) / 2, 2)
                    rattrapage = mark < 12 or (mark == 12 and np.random.rand() < 0.5)
                    marks_data.append([  # Append submodule marks
                        student["Code APO"],
                        student["Nom"],
                        student["Prénom"],
                        mark,
                        rattrapage,
                        mark_sub1,
                        mark_sub2
                    ])
                else:
                    mark, rattrapage = generate_marks(student, module)
                    marks_data.append([  # Append single module mark
                        student["Code APO"],
                        student["Nom"],
                        student["Prénom"],
                        mark,
                        rattrapage
                    ])

            # Create DataFrame for marks
            if has_submodules:
                marks_df = pd.DataFrame(marks_data, columns=[
                    "apogee", "nom", "prenom", "mark", "rattrapage", "marksub1", "marksub2"
                ])
            else:
                marks_df = pd.DataFrame(marks_data, columns=[
                    "apogee", "nom", "prenom", "mark", "rattrapage"
                ])

            # Save to CSV
            output_file = os.path.join(output_dir, f"note_{module_code}_{module_name}.csv")
            marks_df.to_csv(output_file, index=False, float_format="%.2f")


def process_all_promos(modules_file):
    current_directory = os.getcwd()
    promo_folders = [d for d in os.listdir(current_directory) if os.path.isdir(d) and d.startswith('promo_')]

    for folder in promo_folders:
        folder_path = os.path.join(current_directory, folder)
        print(f"Processing folder: {folder_path}")
        modules = load_csv(modules_file)
        generate_marks_files(folder_path, modules)


# File path for the module data
modules_file = 'modules.csv'

# Run the processing for all promo folders
process_all_promos(modules_file)
