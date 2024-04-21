import pandas as pd

#Grammy

def drop_null_ayw(df):
    mask = df['artist'].isnull() & df['workers'].isnull()
    df = df[~mask]
    
    return df

def workerxartist(df):
    mask = df['artist'].isnull() & df['workers'].notnull()
    df.loc[mask, 'artist'] = df.loc[mask, 'workers'].str.extract(r'\((.*?)\)')[0]

    return df

def artist_workers(df):
    mask1 = df['artist'].isnull() & ~df['workers'].isnull()
    df.loc[mask1, 'artist'] = df.loc[mask1, 'workers'].apply(
        lambda x: x.split(';')[0].split(',')[0].strip() if ';' in x or ',' in x else x.strip())

    return df

def lower(df):
    df['nominee'] = df['nominee'].str.lower()
    df['category'] = df['category'].str.lower()

    return df

def drop_grammy(df):
    grammy = df.drop(['published_at','updated_at','img','winner'], axis=1)
    return grammy


def filter_songs(df):
    include_mask = (df['category'].str.contains('song', case=False, na=False) |
                    df['category'].str.contains('performance', case=False, na=False) |
                    df['category'].str.contains(r'\brecord\b', case=False, na=False, regex=True))
    
    exclude_mask = (df['category'].str.contains('album', case=False) |
                    df['category'].str.contains('artist', case=False))

    combined_mask = include_mask & ~exclude_mask

    return df[combined_mask]


#Spotify

def drop_columns(df):
    columns_to_remove = ['Unnamed: 0', 'duration_ms', 'danceability', 'energy', 'key', 
                         'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 
                         'liveness', 'valence', 'tempo', 'time_signature']
    df.drop(columns=[col for col in columns_to_remove if col in df.columns], axis=1, inplace=True)

    return df
    
def drop_nulls(df):
    mask = df.isnull().any(axis=1)
    df = df[~mask]

    return df

def drop_duplicates1(df):
    df['track_name'] = df['track_name'].str.lower()
    indices_max_popularity = df.groupby(['track_id', 'track_name', 'artists'])['popularity'].idxmax()
    df = df.loc[indices_max_popularity]
    df.reset_index(drop=True, inplace=True)

    return df


def drop_duplicates2(df):
    df['track_name'] = df['track_name'].str.lower()
    spotify = df.loc[df.groupby(['track_name', 'artists'])['popularity'].idxmax()]

    return spotify

