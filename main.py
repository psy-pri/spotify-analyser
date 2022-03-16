import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests 
import json 
from datetime import datetime 
import datetime


DATABASE_LOCATION = ""
SPOTIFY_USER_ID = "21fnzbpor6sxzaox2k7lysf7q"
TOKEN = "BQDHaRA6whVITYo61w-nuy0Q7b3qusG8GeqlNo_WWXvHtKrVLduIjA3pynqVd0ZCwtCmRepc5gf5NxrmsXRdB7DALFbNFz1bFfEVcgkOmLUQ7rWV8Xbcg7OGK2coQ2buYVTuEXsY0HecYb9dq_iR6_0lhxbMlTzMQYl2"

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

    timestamps = df["ts"].tolist()
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
    yesterday_unix_ts = int(yesterday.timestamp()) * 1000
    # print("time is ", yesterday_unix_ts)
    
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time = yesterday_unix_ts), headers = headers)
    
    data = r.json()
    
    #print(data)
    
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
    print(song_df)


# Validate 
if check_if_valid_data(song_df):
    print("Data valid, proceed to load stage.")
    
    
# Load 


# Schedule