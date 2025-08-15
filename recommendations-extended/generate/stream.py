import pandas as pd
from faker import Faker
from datetime import date

RATED_CSV_FILE = "data/csv/recommendations-relationships-RATED.csv"


def generate_streams(number, seed, number_of_users):
    Faker.seed(seed)
    fake = Faker()

    # load the ratings into a dataframe the more likely a movie is to be rated, the more likely it is to be picked
    ratings_df = pd.read_csv(RATED_CSV_FILE)
    # create avg rating dataframe, higher rated movies are more likely to be completed
    avg_rating_df = ratings_df.groupby("movie_movieId")["rating"].agg("mean")

    def purchase_likelihood(movie_id):
        rating = avg_rating_df.get(movie_id, default=3)
        # Higher ratings increase the likelihood of being purchased
        if rating < 1:
            return 50
        elif rating < 2:
            return 75
        elif rating < 3:
            return 90
        elif rating < 4:
            return 98
        else:
            return 99

    streams = []
    for i in range(1, number + 1):
        movie_id = ratings_df.iloc[fake.pyint(0, len(ratings_df)-1)].name
        streams.append({
            'streamId': i,
            'movieId': movie_id,
            'userId': fake.pyint(1, number_of_users),
            'startTime': fake.date_time_between_dates(
                date(2024, 1, 1),
                date(2024, 12, 31)
                ),
            'completed': fake.pybool(purchase_likelihood(movie_id)),
        })
    return streams

if __name__ == "__main__":
    print(generate_streams(10, 486436149, 1000))