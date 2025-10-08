from faker import Faker
from gps import generate_coord_by_state

def generate_addresses(number, seed):

    Faker.seed(seed)
    fake = Faker()

    addresses = []
    for i in range(number):

        street = fake.street_address()
        city = fake.city()
        state = fake.state_abbr(include_territories=False, include_freely_associated_states=False)
        zip_code = fake.postcode_in_state(state)
        postal_address = f"{street}, {city}, {state} {zip_code}"
        lat, long = generate_coord_by_state(seed, state)
        
        addresses.append(
            {
            'postal_address': postal_address,
            'street': street,
            'city': city,
            'state': state,
            'zip_code': zip_code,
            'latitude': lat,
            'longitude': long
            }
        )

    return addresses

if __name__ == "__main__":
    print(generate_addresses(10, 764875463))

