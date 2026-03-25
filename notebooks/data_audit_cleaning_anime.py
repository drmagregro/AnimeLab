import pandas as pd
from pandas import DataFrame as df
import numpy as np

# Audit

# anime.csv:
# Column: MAL_ID, Name , Scores, Genre, English Name, Japanese Name, Type, Episodes, Aired, Premiered, Producers, Licensors, Studios, Source, Duration, Rating, Score, Ranked, Popularity, Members, Favorites, Watching, Completed, On-Hold, Dropped, Plan to Watch, Score-10, Score-9, Score-8, Score-7, Score-6, Score-5, Score-4, Score-3, Score-2, Score-1
# MAL_ID: my_anime_list id de l'anime
# Name: string Nom de l'anime
# Score: Note des utilisateurs moyennes sur 10
# Genre: Comma separated list of genres for this anime.
# English Name: string Nom anglais de l'anime
# Japanese Name: string Nom japonais de l'anime
# Type: string Type de l'anime
# Episodes: string Nombre d'episode de l'anime
# Aired: string Date de sortie de l'anime
# Premiered: string Date de sortie de l'anime
# Producers: Comma separated list of producers for this anime.
# Licensors: Comma separated list of licensors for this anime.
# Studios: Comma separated list of studios for this anime.
# Source: string Source de l'anime
# Duration: string Duree de l'anime
# Rating: string Rating de l'anime
# Score: Note des utilisateurs moyennes sur 10
# Ranked: string Rank de l'anime
# Popularity: string Popularity de l'anime
# Members: string Nombre de membres de l'anime
# Favorites: string Nombre de favoris de l'anime
# Watching: string Nombre de personnes en train de regarder l'anime
# Completed: string Nombre de personnes ayant terminé l'anime
# On-Hold: string Nombre de personnes ayant mis l'anime en attente
# Dropped: string Nombre de personnes ayant abandonnées l'anime
# Plan to Watch: string Nombre de personnes ayant planifié de regarder l'anime
# Score-10: string Nombre de personnes ayant note 10 sur 10
# Score-9: string Nombre de personnes ayant note 9 sur 10   
# Score-8: string Nombre de personnes ayant note 8 sur 10
# Score-7: string Nombre de personnes ayant note 7 sur 10
# Score-6: string Nombre de personnes ayant note 6 sur 10   
# Score-5: string Nombre de personnes ayant note 5 sur 10
# Score-4: string Nombre de personnes ayant note 4 sur 10
# Score-3: string Nombre de personnes ayant note 3 sur 10
# Score-2: string Nombre de personnes ayant note 2 sur 10
# Score-1: string Nombre de personnes ayant note 1 sur 10

anime_df = pd.read_csv("./data/anime.csv")

print(f"Dataset shape: {anime_df.shape}\n")  # (17562, 35)

print("Colonnes du dataset:")
print(anime_df.columns.tolist(), "\n")

print("Résumé info():")
print(anime_df.info(), "\n")

pd.set_option('display.max_columns', None)
print("Aperçu des 5 premières lignes:")
print(anime_df.head(), "\n")

print("Nombre de valeurs uniques par colonne:")
for col in anime_df.columns:
    unique_count = anime_df[col].nunique()
    print(f"{col}: {unique_count} valeurs uniques")

print("\nDoublons sur MAL_ID:", anime_df["MAL_ID"].duplicated().sum())

print("\nValeurs manquantes par colonne:")
print(anime_df.isna().sum())


