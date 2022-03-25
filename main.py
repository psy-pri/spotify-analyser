import psycopg2
import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests 
import json 
from datetime import datetime 
import datetime

#store in different file 
#auto generate token 
DATABASE_ENGINE = "postgresql+psycopg2://postgres:priyanka123@localhost:5432/spotify_trends"
SPOTIFY_USER_ID = "21fnzbpor6sxzaox2k7lysf7q"
TOKEN = "BQAo_w1HbFxZxTsrU1RO9gZU-BtCm0j_Y-xFJPXTAl1hN0fYhwd02mL6jutaEjyqom7MNxSFVoG6tVMPUeryIsmvdCDX4cJn4uUa-BLUkcQ8KVanPISL96R0KcZiYp1KVkaK5coVSZiJ7i3_el0yJuIVQXV8ch4oS9H6"

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False 
    
    # Primary key check
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary key check failed. Terminating program")
    
    # Check if null values exist
    if df.isnull().values.any():
        raise Exception("Null values found. Terminating program")

    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    print("yesterday:", yesterday)
    
    timestamps = df["ts"].tolist()
    print("timestamps", timestamps)
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")

    return True

if __name__ == "__main__":
    
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application.json",
        "Authorization" : "Bearer {token}".format(token = TOKEN)
    }
    
    today = datetime.datetime.now()
    today_unix_ts = int(today.timestamp()) * 1000
    yesterday = today - datetime.timedelta(days = 1)
    print("yesterday ts:" , yesterday)
    yesterday_unix_ts = int(yesterday.timestamp()) * 1000
    print("yesterday ts unix ", yesterday_unix_ts)
    
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time = yesterday_unix_ts), headers = headers)
    
    data = r.json()
    
    # print('Spotify data JSON:', data)
    
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = [] 
    
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
        
    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "ts" : timestamps
    }
    
    song_df = pd.DataFrame(data = song_dict)
    print('Spotify data df format:', song_df)


# Validate 
if check_if_valid_data(song_df):
    print("Data valid, proceed to load stage.")
    
    
# Load 
conn = psycopg2.connect("dbname=spotify_trends user=postgres password=priyanka123")
db_engine = sqlalchemy.create_engine(DATABASE_ENGINE)
#print("db-eng ",db_engine)
if conn:
    cursor = conn.cursor()
    sql_query = """
    CREATE TABLE IF NOT EXISTS tracks.my_played_tracks(
        song_name VARCHAR(250) NOT NULL,
        artist_name VARCHAR(250) NOT NULL,
        played_at TIMESTAMP PRIMARY KEY NOT NULL
    );
    """
    cursor.execute(sql_query)
    conn.commit()
    print("Database opened successfully")
else:
    print("Database connection failed")
    

try:
    song_df.to_sql("my_played_tracks", con= db_engine, index=False, schema="tracks", if_exists='append')
except:
    print("Data is already present")
    
conn.close()
print("Database closed")

# Schedule