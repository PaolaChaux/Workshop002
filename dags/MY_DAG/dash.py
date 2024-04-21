import requests
import psycopg2
import pandas as pd
import json
import logging
import os
import transformations

def extract_csv():
    df_spotify = pd.read_csv("spotify_dataset.csv")
    logging.info(f"Columns are: {df_spotify.columns}")

    return df_spotify
    
def extract_basedatos():
    df_grammy = pd.read_csv("the_grammy_awards.csv")

    return df_grammy



def transform_spotify(df_spotify):
    print("Data coming from extract:", df_spotify)
    print("Data type is: ", type(df_spotify))

    df_spotify = transformations.drop_columns(df_spotify)
    df_spotify = transformations.drop_nulls(df_spotify)
    df_spotify = transformations.drop_duplicates1(df_spotify)
    df_spotify = transformations.drop_duplicates2(df_spotify)
    

    logging.info(f"Data after transformation is: {df_spotify.head()}")
    logging.info(f"Columns after transformation are: {df_spotify.columns}")

    jdata_spotify = df_spotify.to_json()

    return df_spotify	

def transform_grammy(df_grammy):

    df_grammy = transformations.drop_null_ayw(df_grammy)
    df_grammy = transformations.workerxartist(df_grammy)
    df_grammy = transformations.artist_workers(df_grammy)
    df_grammy = transformations.lower(df_grammy)
    df_grammy = transformations.drop_grammy(df_grammy)

    jdata_grammy = df_grammy.to_json()

    return df_grammy

def merge(df_spotify, df_grammys):

    df_merged = df_spotify.merge(df_grammys, how='left', left_on=['track_name', 'artists'], right_on=['nominee', 'artist'])
    df_merged.drop(columns=['nominee', 'artist'], inplace=True)

    jdata_merged = df_merged.to_json()

    return df_merged

  
if __name__=='__main__':
    df_spotify = transform_spotify(extract_csv())
    df_grammy = transform_grammy(extract_basedatos())
    df = merge(df_spotify, df_grammy)
    df.to_csv("grammy_spotify_data.csv")