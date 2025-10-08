import csv
from address import generate_addresses
from user import generate_users
from purchased import generate_purchases
from stream import generate_streams

from constants import (
    SEED,
    NUMBER_OF_ADDRESSES,
    NUMBER_OF_USERS,
    NUMBER_OF_PURCHASES,
    NUMBER_OF_STREAMS,
    EXTENDED_USER_CSV_FILE,
    ADDRESS_CSV_FILE,
    PURCHASED_CSV_FILE,
    STREAM_CSV_FILE
)

def write_csv(file_path, records):
    print(f"Writing {file_path}")
    with open(file_path, "w", newline='') as file:
        writer = csv.DictWriter(file, fieldnames=records[0].keys())
        writer.writeheader()
        for record in records:
            writer.writerow(record)

print("Generating addresses...")
addresses = generate_addresses(NUMBER_OF_ADDRESSES, SEED)
print("Generating users...")
users = generate_users(NUMBER_OF_USERS, SEED, addresses)
print("Generating purchases...")
purchases = generate_purchases(NUMBER_OF_PURCHASES, SEED, NUMBER_OF_USERS)
print("Generating streams...")
streams = generate_streams(NUMBER_OF_STREAMS, SEED, NUMBER_OF_USERS)

write_csv(ADDRESS_CSV_FILE, addresses)
write_csv(EXTENDED_USER_CSV_FILE, users)
write_csv(PURCHASED_CSV_FILE, purchases)
write_csv(STREAM_CSV_FILE, streams)
