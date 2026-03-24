import pandas as pd
import numpy as np





# Cleaning

#Turning invalid value to pd.NA

anime_with_synopsis_df =  pd.read_csv("data/anime_with_synopsis.csv")
anime_with_synopsis_df.loc[
    anime_with_synopsis_df["Genres"] == "Unknown",
    "Genres"
] = pd.NA

anime_with_synopsis_df.loc[
    anime_with_synopsis_df["Score"] == "Unknown",
    "Score"
] = pd.NA

anime_with_synopsis_df.loc[
    anime_with_synopsis_df["sypnopsis"] == "No synopsis has been added for this series yet. Click here to update this information.",
    "sypnopsis"
] = pd.NA

anime_with_synopsis_df.loc[
    anime_with_synopsis_df["sypnopsis"] == "No synopsis information has been added to this title. Help improve our database by adding a synopsis here .",
    "sypnopsis"
] = pd.NA

print(f"info: {anime_with_synopsis_df}")
print(f"no duplicate id {anime_with_synopsis_df.duplicated(["MAL_ID"]).value_counts().head(1)}")
print(f"Unknown score count:{anime_with_synopsis_df["Score"].value_counts().head(1)}")
print(f"No Synopsis count:{anime_with_synopsis_df["sypnopsis"].value_counts().head(2)}")
#Double are just next part or next season
print(f"duplicate Name count:{anime_with_synopsis_df["Name"].value_counts().head(3)}")

print(anime_with_synopsis_df[anime_with_synopsis_df["Name"] == "Hinamatsuri"]["sypnopsis"])