import csv
from faker import Faker

EXISTING_USER_CSV_FILE = "data/csv/recommendations-nodes-User.csv"

def generate_users(number, seed):
    Faker.seed(seed)
    fake = Faker()

    # load existing user names
    with open(EXISTING_USER_CSV_FILE, "r") as existing_user_file:

        user_csv = csv.DictReader(
            existing_user_file,
            fieldnames=['userId', 'name']
            )
        
        existing_users = {}
        for row in user_csv:
            existing_users[row['userId']] = row['name']

    # Create users
    users = []
    for i in range(1, number + 1):

        name = existing_users[str(i)] if str(i) in existing_users.keys() else f"{fake.first_name()} {fake.last_name()}"
        
        users.append({
            'userId': i,
            'name': name,
            'address': fake.address(),
            'email': f"{name.replace(" ", ".").lower()}@{fake.domain_name()}",
            'phone': fake.phone_number()
        })

    return users

if __name__ == "__main__":
    print(generate_users(10, 486436149))