import csv
from user import generate_users
from purchased import generate_purchases
from stream import generate_streams

SEED = 486436149
NUMBER_OF_USERS = 671
NUMBER_OF_MOVIES = 9125
NUMBER_OF_PURCHASES = 2000
NUMBER_OF_STREAMS = 25000

USER_CSV_FILE = "data/csv/extended-movies-nodes-User.csv" 
PURCHASED_CSV_FILE = "data/csv/extended-movies-relationships-PURCHASED.csv"
STREAM_CSV_FILE = "data/csv/extended-movies-nodes-Stream.csv"

def write_csv(file_path, records):
    print(f"Writing {file_path}")
    with open(file_path, "w", newline='') as file:
        writer = csv.DictWriter(file, fieldnames=records[0].keys())
        writer.writeheader()
        for record in records:
            writer.writerow(record)

users = generate_users(NUMBER_OF_USERS, SEED)
purchases = generate_purchases(NUMBER_OF_PURCHASES, SEED, NUMBER_OF_USERS)
streams = generate_streams(NUMBER_OF_STREAMS, SEED, NUMBER_OF_USERS)

write_csv(USER_CSV_FILE, users)
write_csv(PURCHASED_CSV_FILE, purchases)
write_csv(STREAM_CSV_FILE, streams)
