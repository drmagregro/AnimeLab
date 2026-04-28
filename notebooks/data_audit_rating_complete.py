import pandas as pd

# Audit rating_complete.csv

#Columns are: user_id,anime_id,rating
# user_id: User's ID
# anime_id: Anime's ID
# rating: Anime rating given par de user

rating_complete_df = pd.read_csv("data/rating_complete.csv")

print(rating_complete_df[
    pd.to_numeric(rating_complete_df["user_id"], errors="coerce").isna()
])
print(rating_complete_df["user_id"].isna().sum())
print(rating_complete_df[
    pd.to_numeric(rating_complete_df["anime_id"], errors="coerce").isna()
])
print(rating_complete_df["anime_id"].isna().sum())
print(rating_complete_df[
    pd.to_numeric(rating_complete_df["rating"], errors="coerce").isna()
])
print(rating_complete_df["rating"].isna().sum())
# No invalid value
