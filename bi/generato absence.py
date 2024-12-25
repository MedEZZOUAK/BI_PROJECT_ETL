import csv
import random
import os


def load_csv(file_path):
    """Loads a CSV file and returns its content as a list of dictionaries."""
    with open(file_path, mode='r', encoding='utf-8') as file:
        return list(csv.DictReader(file))


def generate_absence(cour_range, tp_range, student_bias, module_bias):
    """Generates absence data for the specified course and TP ranges."""
    base_attendance_probability = 0.95  # High baseline for attendance

    # Adjust based on student and module bias
    student_adjustment = (student_bias - 1) / 8 * 0.6  # Maps student bias from 1 to 9 to 0 to 0.4
    module_adjustment = (module_bias - 1) / 8 * 0.5  # Maps module bias from 1 to 9 to 0 to 0.3

    # Calculate the final attendance probability
    attendance_probability = base_attendance_probability - student_adjustment - module_adjustment
    attendance_probability = max(min(attendance_probability, 1.0), 0.0)  # Cap between 0 and 1

    absence_data = {}
    for i in cour_range:
        cour_attended = random.random() < attendance_probability
        absence_data[f'cour{i}'] = int(cour_attended)

        # TP attendance probability influenced by prior course attendance
        tp_attended = cour_attended or (random.random() < attendance_probability * 1.1)
        absence_data[f'tp{i}'] = int(tp_attended)
    return absence_data


def generate_absence_files(promo_folder, module_file):
    """Generates absence CSV files based on student and module data for each promo folder."""
    # Load module data
    modules = load_csv(module_file)

    # Get list of student files from the promo folder
    student_files = [f for f in os.listdir(promo_folder) if f.startswith('students') and f.endswith('.csv')]

    for student_file in student_files:
        student_path = os.path.join(promo_folder, student_file)
        students = load_csv(student_path)

        # Create an output directory inside the promo folder
        output_dir = os.path.join(promo_folder, 'absence_files')
        os.makedirs(output_dir, exist_ok=True)

        for module in modules:
            module_name = module['name']
            coeff = float(module['coeff'])
            module_bias = int(module['absence_bias'])

            # Check if the module is split into two halves
            if coeff == 0.5:
                parts = [(1, range(1, 9), range(1, 9)), (2, range(9, 17), range(9, 17))]
            else:
                parts = [("", range(1, 17), range(1, 17))]

            for part, cour_range, tp_range in parts:
                filename = f"absence_{module_name}{part}.csv"
                file_path = os.path.join(output_dir, filename)

                with open(file_path, mode='w', encoding='utf-8', newline='') as file:
                    fieldnames = ['nom', 'prenom', 'apogee'] + [f'cour{i}' for i in cour_range] + [f'tp{i}' for i in
                                                                                                   tp_range]
                    writer = csv.DictWriter(file, fieldnames=fieldnames)
                    writer.writeheader()

                    for student in students:
                        student_bias = int(student['absence_bias'])
                        absence_data = generate_absence(cour_range, tp_range, student_bias, module_bias)

                        writer.writerow({
                            'nom': student['Nom'],
                            'prenom': student['Prénom'],
                            'apogee': student['Code APO'],
                            **absence_data
                        })


# Main function to process all promo folders
def process_all_promos(module_file):
    current_directory = os.getcwd()
    promo_folders = [d for d in os.listdir(current_directory) if os.path.isdir(d) and d.startswith('promo_')]

    for folder in promo_folders:
        folder_path = os.path.join(current_directory, folder)
        print(f"Processing folder: {folder_path}")
        generate_absence_files(folder_path, module_file)


# File path for the module data
module_file = 'modules.csv'

# Run the processing for all promo folders
process_all_promos(module_file)
