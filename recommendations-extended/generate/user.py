import csv
from faker import Faker

from constants import USER_CSV_FILE

def generate_users(number, seed, addresses):
    Faker.seed(seed)
    fake = Faker()

    # load existing user names
    with open(USER_CSV_FILE, "r") as existing_user_file:

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
            'postal_address': fake.random_element(elements=addresses)['postal_address'],
            'email': f"{name.replace(" ", ".").lower()}@{fake.domain_name()}",
            'phone': fake.phone_number()
        })

    return users

if __name__ == "__main__":
    from address import generate_addresses
    from constants import SEED
    print(generate_users(10, 486436149, generate_addresses(5, SEED)))