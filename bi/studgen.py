import os
import csv
import random
from faker import Faker

# Custom lists for Moroccan names (shortened version)
male_first_names = [
    "Mohammed", "Ahmed", "Youssef", "Hamza", "Ali", "Omar", "Hassan", "Rachid", "Karim", "Tarek",
    "Sami", "Anas", "Younes", "Bilal", "Mustapha", "Adil", "Said", "Zaid", "Ibrahim", "Hicham",
    "Khalid", "Jawad", "Mounir", "Faysal", "Nabil", "Reda", "Amine", "Imad", "Soufiane", "Abdelaziz",
    "Aziz", "Walid", "Fouad", "Hassan", "Rachid", "Zine", "Nassim", "Yassine", "Zakaria", "Mohcine",
    "Said", "Abdelhak", "Othman", "Samir", "Rida", "Mouad", "Yahya", "Badr", "Zaid", "Khalil",
    "Mounir", "Lahcen", "Hicham", "Abdelkader", "Youssef", "Fadel", "Tariq", "Ismail", "Nasser", "Khaled",
    "Saber", "Simo", "Brahim", "Jamal", "Hamid", "Marouane", "Rachid", "Samir", "Hossam", "Riyad",
    "Majid", "Dari", "Hakim", "Ali", "Husain", "Abdallah", "Khalid", "Badr", "Oussama", "Chafik",
    "Ismail", "Zaki", "Nabil", "Abdelhamid", "Mohammed", "Yassine", "Imad", "Salim", "Walid", "Hassan",
    "Ahmed", "Younes", "Omar", "Sami", "Anas", "Youssef", "Faycal", "Khaled", "Mohcine", "Soufiane",
    "Youssef", "Mohammed", "Imad", "Aziz", "Karim", "Zine", "Tariq", "Jawad", "Adil", "Walid",
    "Adil", "Hicham", "Hassan", "Rachid", "Rida", "Majid", "Nasser", "Azzeddine", "Brahim", "Amine"
]

female_first_names = [
    "Amina", "Fatima", "Latifa", "Salma", "Khadija", "Sara", "Zineb", "Nadia", "Hanae", "Siham",
    "Leila", "Meriem", "Samira", "Imane", "Yasmina", "Hanane", "Rania", "Mouna", "Hind", "Hiba",
    "Kawtar", "Nour", "Asmaa", "Rim", "Nadia", "Yasmine", "Maha", "Sofia", "Loubna", "Fadoua",
    "Hajar", "Aicha", "Dounia", "Oumayma", "Zaynab", "Fatine", "Sanae", "Malak", "Yousra", "Imane",
    "Marwa", "Sanaa", "Hiba", "Jana", "Meryem", "Chafia", "Wiam", "Laila", "Zineb", "Oumaima",
    "Khadija", "Hana", "Layla", "Salma", "Amal", "Mouna", "Rama", "Sabrine", "Rachida", "Fatima",
    "Hanane", "Nour", "Ines", "Rania", "Meriem", "Najwa", "Sofia", "Asmae", "Samia", "Imane",
    "Rim", "Nadia", "Rania", "Mouna", "Wafaa", "Amina", "Zohra", "Loubna", "Nawel", "Siham",
    "Sana", "Aicha", "Yasmina", "Rim", "Layla", "Salma", "Sofia", "Malika", "Nadia", "Sanae",
    "Wissal", "Samira", "Hind", "Fatiha", "Maya", "Sabrina", "Mouna", "Hanane", "Najia", "Hiba",
    "Widad", "Amira", "Kenza", "Jana", "Sofia", "Nawal", "Israa", "Anissa", "Dounia", "Kawtar",
    "Asmaa", "Laila", "Rim", "Sana", "Wiam", "Yousra", "Zahra", "Rania", "Hana", "Malak",
    "Leila", "Ahlam", "Meriem", "Layla", "Fatima", "Rachida", "Hanane", "Rania", "Wafaa", "Imane"
]

last_names = [
    "El-Badri", "El-Khayat", "Amrani", "Bouzid", "Essamadi", "Alami", "Fassi", "Ouazzani", "Bennani", "Tazi",
    "Chafik", "El-Fassi", "Ouarzazi", "El-Mansouri", "Haddad", "Al-Karim", "El-Khoury", "Rifai", "Hassani", "Zine",
    "Taoufiq", "Fakir", "El-Saadi", "Chebbi", "Al-Hadri", "Jouhari", "Ait-Lahcen", "Berrada", "El-Qasimi", "El-Alaoui",
    "El-Moudden", "Rahmouni", "Mejri", "Boussaidi", "Tahiri", "El-Mekki", "Rizki", "Ouafi", "Zarrouk", "El-Solh",
    "Anissi", "Rafik", "El-Midani", "Tawfik", "Belaid", "Baddi", "Mouhammad", "Anouar", "Ezzedine", "Idrissi",
    "El-Bay", "Kebdani", "Maddouri", "El-Hariri", "Ramdani", "El-Mansouri", "El-Khayat", "Badi", "Anas", "El-Daoud",
    "Koulak", "Lahouari", "Hammani", "Al-Fadli", "Bentaib", "Zahiri", "El-Basri", "El-Ayadi", "Khiari", "Bounou",
    "Cherkaoui", "El-Benali", "El-Mekki", "Boussaid", "Hibiri", "El-Morabit", "Rkia", "Rabi", "Chouki", "El-Atmani",
    "Ibrahim", "El-Attari", "Atif", "El-Kherfi", "Dahbi", "Moutaouakkil", "Nadi", "El-Massiri", "El-Kosari", "Sboui",
    "El-Abbadi", "Haidari", "Rihani", "Moussaoui", "El-Talbi", "Houri", "Shami", "Nora", "Azzouz", "Kabbaj",
    "El-Bekkali", "Berrah", "Mohsine", "Zarouk", "Salhi", "Arfaoui", "Idriss", "Ouares", "Nasser", "Benali",
    "Yachou", "Ouchrif", "El-Farouki", "Amari", "Benbrahim", "El-Morrabit", "Ibrahim", "El-Boudj", "El-Maghribi",
    "Aziz",
    "Lahiji", "El-Rahmani", "El-Kawthar", "Saoud", "Ragheb", "El-Gassimi", "El-Boughaz", "Atfi", "El-Idrissi",
    "Lahrichi",
    "Berouane", "Akoubri", "Maha", "Goussar", "EZ-ZOUAK", "Maachi", "El-Bennani", "Kabbour", "Oukhira", "Meriem",
    "Djamel", "Al-Khiyari", "Madani", "Nissani", "Brahimi", "Bounah", "Jabari", "Derraz", "Kasmi", "El-Ayyad",
    "Tayeb", "Benbihi", "El-Mohammadi", "Tajar", "Haddouchi", "Omar", "El-Kammouni", "El-Chaoui", "Zayed", "Daoudi",
    "Akil", "Boudiaf", "El-Mansouri", "Mourad", "Mokhtar", "Saghir", "El-Soufi", "Saib", "Housni", "Hajji",
    "El-Khader", "Brouz", "Benno", "Sami", "Bensalah", "Rkik", "Toulal", "Aissat", "EL-OTMANI", "El-Ouarzazi",
    "El-Ahrach", "Maid", "Faiyaz", "Ammi", "Akla", "Ouaji", "Zanate", "Al-Mutawakkil", "Fattah", "Maki",
    "Arbi", "Djari", "Chennouf", "Hasnaoui", "Benslimane", "Barjawi", "Al-Hossaini", "Hafidi", "Messaoudi", "Hassan",
    "Benserhane", "Samkani", "Khabir", "Doukkali", "Hassani", "Hiziri", "Idrisi", "Sabri", "Zouaoui", "Marhaba",
    "El-Aissawi", "Ait-Sidi", "Benkirane", "Kachkach", "Oumahdi", "Fikri", "Didi", "Zikhri", "El-Khazzani", "Heraichi"
]

# Initialize Faker for other data
fake = Faker()


# Function to generate a student (assuming this function is already defined)
def generate_student(student_id, promo_year):
    sexe = random.choice(['M', 'F'])
    prenom = random.choice(male_first_names) if sexe == 'M' else random.choice(female_first_names)
    nom = random.choice(last_names)

    # Calculate birth year and generate a realistic birthdate
    birth_year = promo_year - 19  # Most students are around 19 when starting university
    birth_month = random.randint(1, 12)
    if birth_month == 2:  # February (consider leap years)
        if (birth_year % 4 == 0 and birth_year % 100 != 0) or (birth_year % 400 == 0):
            birth_day = random.randint(1, 29)
        else:
            birth_day = random.randint(1, 28)
    elif birth_month in [4, 6, 9, 11]:  # Months with 30 days
        birth_day = random.randint(1, 30)
    else:  # Months with 31 days
        birth_day = random.randint(1, 31)

    birthdate = f"{birth_year}-{birth_month:02d}-{birth_day:02d}"

    # Determine ajournment status with an adjusted weight (e.g., 5% chance of being ajourned)
    is_ajourned = random.choices([0, 1], weights=[95, 5])[0]

    # Adjust absence bias based on ajournment status
    if is_ajourned == 0:
        absence_bias = random.randint(1, 5)  # Lower absence bias for students not ajourned
    else:
        absence_bias = random.randint(6, 9)  # Higher absence bias for students who are ajourned

    return {
        "ID": student_id,
        "Code APO": random.randint(10000000, 99999999),
        "Nom": nom,
        "Prénom": prenom,
        "Sexe": sexe,
        "Birthdate": birthdate,
        "Number of Ajournements": is_ajourned,
        "absence_bias": absence_bias,
    }


# Function to create a CSV file and delete existing data if present
def create_csv(folder, filename, promo_year):
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)

    # Delete existing file if it exists
    if os.path.exists(filepath):
        os.remove(filepath)

    with open(filepath, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=[
            "ID", "Code APO", "Nom", "Prénom", "Sexe", "Birthdate", "Number of Ajournements", "absence_bias"
        ])
        writer.writeheader()

        for student_id in range(1, 71):  # Adjust the range as needed
            student = generate_student(student_id, promo_year)
            writer.writerow(student)


# Generate data for each promo year
def generate_promos():
    for year in range(2019, 2025):  # Adjust the range for the desired years
        folder_name = f"promo_{year}"
        create_csv(folder_name, "students.csv", year)


# Run the generation process
generate_promos()