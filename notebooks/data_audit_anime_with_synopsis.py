import pandas as pd
import numpy as np

# Audit

# anime_with_synopsis.csv:
# Column: MAL_ID,Name,Score,Genres,sypnopsis
# MAL_ID: my_anime_list id de l'anime
# Name: string Nom de l'anime
# Score: Note des utilisateurs moyennes sur 10
# Genres: Comma separated list of genres for this anime.
# Synopsis: string with the synopsis of the anime.



anime_with_synopsis_df =  pd.read_csv("data/anime_with_synopsis.csv")
print(f"info: {anime_with_synopsis_df}")
print(f"no duplicate id {anime_with_synopsis_df.duplicated(["MAL_ID"]).value_counts().head(1)}")
print(f"Unknown score count:{anime_with_synopsis_df["Score"].value_counts().head(1)}")
print(f"No Synopsis count:{anime_with_synopsis_df["sypnopsis"].value_counts().head(2)}")
#Double are just next part or next season
print(f"duplicate Name count:{anime_with_synopsis_df["Name"].value_counts().head(3)}")

print(anime_with_synopsis_df[anime_with_synopsis_df["Name"] == "Hinamatsuri"]["sypnopsis"])
histogramme = {}
for genres in anime_with_synopsis_df["Genres"]:
    genres_list = genres.split(", ")
    for genre in genres_list:
        if genre in histogramme:
            histogramme[genre]+=1
        else:
            histogramme[genre]=0
print(f"genre repartition histogramme:{histogramme}")