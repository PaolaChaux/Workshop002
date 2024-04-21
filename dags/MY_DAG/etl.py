import requests
import psycopg2
import pandas as pd
import json
import logging
import os
import transformations


def extract_csv():
    csv = r"data/spotify_dataset.csv"
    df_spotify = pd.read_csv(csv)
    logging.info(f"Columns are: {df_spotify.columns}")

    jdata_spotify = df_spotify.to_json(orient='records')
    return jdata_spotify
    
def extract_basedatos():
    with open(r"db_config.json") as config_json:
        config = json.load(config_json)

    conx = psycopg2.connect(**config)
    try:
        mycursor = conx.cursor()

        all_info = "SELECT * from grammy"
        mycursor.execute(all_info)
        results = mycursor.fetchall()
        df_grammy = pd.DataFrame(results, columns=['id', 'year', 'title', 'published_at', 'updated_at', 'category', 'nominee', 'artist', 'workers', 'img', 'winner'])

        logging.info(f"data is: {df_grammy.head()}")
        logging.info(f"Columns are: {df_grammy.columns}")

        jdata_grammy = df_grammy.to_json(orient='records')

        return jdata_grammy

    finally:
        mycursor.close()
        conx.close()

def transform_spotify(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="extract_csv_task"))
    print("Data coming from extract:", json_data)
    print("Data type is: ", type(json_data))

    df_spotify = pd.json_normalize(json_data)

    df_spotify = transformations.drop_columns(df_spotify)
    df_spotify = transformations.drop_nulls(df_spotify)
    df_spotify = transformations.drop_duplicates1(df_spotify)
    df_spotify = transformations.drop_duplicates2(df_spotify)
    

    logging.info(f"Data after transformation is: {df_spotify.head()}")
    logging.info(f"Columns after transformation are: {df_spotify.columns}")

    jdata_spotify = df_spotify.to_json(orient='records')

    return jdata_spotify

def transform_grammy(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="extract_bd_task"))
    print("data coming from extract:", json_data)
    print("data type is: ", type(json_data))

    df_grammy = pd.json_normalize(json_data)

    df_grammy = transformations.drop_null_ayw(df_grammy)
    df_grammy = transformations.workerxartist(df_grammy)
    df_grammy = transformations.artist_workers(df_grammy)
    df_grammy = transformations.lower(df_grammy)
    df_grammy = transformations.drop_grammy(df_grammy)

    jdata_grammy = df_grammy.to_json(orient='records')

    return jdata_grammy

def merge(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="transform_g_task"))
    df2 = pd.json_normalize(json_data)
    json_data = json.loads(ti.xcom_pull(task_ids="transform_s_task"))
    df1 = pd.json_normalize(json_data)
    
    logging.info("Data coming from Spotify extract:", df1)
    logging.info("Data type is:", type(df1))
    logging.info("Data coming from Grammy extract:", df2)
    logging.info("Data type is:", type(df2))

    df_spotify = pd.read_json(df1)
    df_grammys = pd.read_json(df2)

    df_merged = df_spotify.merge(df_grammys, how='left', left_on=['track_name', 'artists'], right_on=['nominee', 'artist'])
    df_merged.drop(columns=['nominee', 'artist'], inplace=True)

    jdata_merged = df_merged.to_json(orient='records')

    return jdata_merged

def load(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="merge_task"))
    
    logging.info("Data coming from extract:", json_data)
    logging.info("Data type is:", type(json_data))

    df_grammys = pd.json_normalize(json_data)

    with open('db_config.json', 'r') as config_json:
        config = json.load(config_json)
    conx = psycopg2.connect(**config)

    try:
        mycursor = conx.cursor()

        mycursor.execute("""
            CREATE TABLE IF NOT EXISTS grammy_spotify_data (
                track_id VARCHAR(255) PRIMARY KEY,
                artists VARCHAR(255),
                album_name VARCHAR(255),
                track_name VARCHAR(255),
                popularity INT,
                explicit BOOLEAN,
                track_genre VARCHAR(255),
                year INT,
                title VARCHAR(255),
                category VARCHAR(255),
                workers VARCHAR(255)
            )
        """)

        for index, row in df_grammys.iterrows():
            values = [row['track_id'], row['artists'], row['album_name'], row['track_name'],
                      row['popularity'], row['explicit'], row['track_genre'], row['year'],
                      row['title'], row['category'], row['workers']]
            values = [None if pd.isna(value) else value for value in values]
            query = """
                INSERT INTO grammy_spotify_data (track_id, artists, album_name, track_name, popularity, explicit, 
                                                 track_genre, year, title, category, workers) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            mycursor.execute(query, values)

        conx.commit()
        logging.info("Data has been successfully loaded to the database.")

    except Exception as e:
        logging.error("An error occurred:", e)
        conx.rollback()

    finally:
        mycursor.close()
        conx.close()

    df_grammys.to_csv("./data/grammy_spotify_data.csv")
    return df_grammys.to_json(orient='records')

def store(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="load_task"))
    df = pd.json_normalize(json_data)
    print("data coming from extract:", json_data)
    print("data type is: ", type(json_data))
    
    logging.info(f"data is {json_data}")

    #upload_csv("df_grammy.csv","1gq1Ih6mCI2_EgKDh5yV2LUKSqap9x2v6")      
    logging.info( f"completed")
  