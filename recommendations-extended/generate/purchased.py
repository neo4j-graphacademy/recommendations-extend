import pandas as pd
from faker import Faker
from datetime import date

from constants import RATED_CSV_FILE

def generate_purchases(number, seed, number_of_users):
    Faker.seed(seed)
    fake = Faker()

    # load the ratings into a dataframe and create a new dataframe where movie ids are repeated
    # proportionally to their rating, so higher rated movies are more likely to be picked
    ratings_df = pd.read_csv(RATED_CSV_FILE)
    avg_rating_df = ratings_df.groupby("movie_movieId")["rating"].agg("mean").sort_values(ascending=False).to_frame()
    movie_ids_x_rating = avg_rating_df.loc[avg_rating_df.index.repeat(avg_rating_df.rating.astype(int))]

    orders = []
    for i in range(1, number + 1):
        orders.append({
            'userId': fake.pyint(1, number_of_users),
            'movieId': movie_ids_x_rating.iloc[fake.pyint(0, len(movie_ids_x_rating)-1)].name,
            'cost': fake.pyfloat(min_value=9, max_value=12, left_digits=2, right_digits=2, positive=True),
            'timestamp': fake.date_time_between_dates(
                date(2024, 1, 1),
                date(2024, 12, 31)
                )
        })

    return orders

if __name__ == "__main__":
    print(generate_purchases(10, 486436149, 100))
    