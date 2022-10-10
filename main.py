"""
Author: Priyanka Shukla
This script extracts user songs played in the last 24 hours,
loads the data into a database and scheduled by Apache Airflow
"""
#pylint: disable=W0621,C0103,W0404,W0611,E0401,W0703

from datetime import datetime
import datetime
import requests
import psycopg2
import sqlalchemy
import pandas as pd
import requests as re
import base64
import six
from sqlalchemy.orm import sessionmaker

DATABASE_ENGINE = "postgresql+psycopg2://postgres:pri123@localhost:5432/spotify_trends"
SPOTIFY_USER_ID = "21fnzbpor6sxzaox2k7lysf7q"
TOKEN = "BQCltQoQ3PndEoDGEANEptmEqhubE6sYhAWAJdS0xf7zZ-0jNaqf5mb7UL4cL5m1DUMFKRMuykQuPa72zWWzkHgxyGYd34RgChr2s9csxZ3-7CygukfDQAT9lYxkAPTyZpL-w4HWpzkf0BCth29bb3ZpVUquYPAMx0B5SvxWgtiib8Po9hQOPy5DL7ZcDGa-glh5"

def check_if_valid_data(df: pd.DataFrame) -> bool:
    """
    Function to check if data is valid
    param: DataFrame
    returns: Boolean
    """
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
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    # yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    # today = datetime.datetime.now()
    # yesterday = today - datetime.timedelta(days = 1)
    # print("yesterday:", yesterday)
    # timestamps = df["ts"].tolist()
    # print("timestamps", timestamps)
    # for timestamp in timestamps:
    #     # trim_ts = datetime.datetime.strptime(str(timestamp), '%Y-%m-%d')
    #     trim_ts = str(timestamp)
    #     print("trim_ts:",trim_ts)
    #     if trim_ts < yesterday:
    #         raise Exception(
    #         "At least one of the returned songs does not have yesterday's timestamp"
    #         )

    return True

if __name__ == "__main__":
    TOKEN_URL = 'https://accounts.spotify.com/api/token'
    BASE_URL = 'https://api.spotify.com/v1/'

    #get auth code
    auth_code = re.get("https://accounts.spotify.com/authorize", 
                    {   "client_id":"535fb26b536d4ed4900a146efb235dcd",
                        'response_type': 'code',
                        'redirect_uri':'http://localhost:8080',
                        'scope':'user-read-recently-played',
                    })
    aa= auth_code.json()
    print(aa)

    #set header 
    CLIENT_ID = "535fb26b536d4ed4900a146efb235dcd"
    CLIENT_SECRET = "2ba8891fd59e41ccafe63f9ac65b4de7"
    auth_header = base64.b64encode(
                (CLIENT_ID + ":" + CLIENT_SECRET).encode("ascii")
        )
    auth_header = auth_header.decode("ascii")

    headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic %s' % auth_header
    }

    payload = {
        'grant_type':"authorization_code",
        'code': auth_code,
        'redirect_uri':'http://localhost:8080'
        }

    # Make a request to the /token endpoint to get an access token
    access_token_request = re.post(url=TOKEN_URL, data=payload, headers=headers)
    print(access_token_request)
    # convert the response to JSON
    access_token_response_data = access_token_request.json()

    access_token = (access_token_response_data["access_token"])

    headers = {
        "Authorization": "Bearer " + access_token
    }
    today = datetime.datetime.now()
    today_unix_ts = int(today.timestamp()) * 1000
    yesterday = today - datetime.timedelta(days = 1)
    #print("yesterday ts:" , yesterday)
    yesterday_unix_ts = int(yesterday.timestamp()) * 1000
    #print("yesterday ts unix ", yesterday_unix_ts)
    r = requests.get(\
        f"https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix_ts}",\
             headers = headers)
    data = r.json()
    print('Spotify data JSON:', data)
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
conn= psycopg2.connect("dbname=spotify_trends user=postgres password=pri123")
db_engine= sqlalchemy.create_engine(DATABASE_ENGINE)
#print("db-eng ",db_engine)

if conn:
    cursor = conn.cursor()
    SQL_QUERY = """
    CREATE TABLE IF NOT EXISTS tracks.my_played_tracks(
        song_name VARCHAR(250) NOT NULL,
        artist_name VARCHAR(250) NOT NULL,
        played_at TIMESTAMP PRIMARY KEY NOT NULL
    );
    """
    cursor.execute(SQL_QUERY)
    conn.commit()
    print("Database opened successfully")
else:
    print("Database connection failed")
try:
    song_df.to_sql("my_played_tracks", con=db_engine, index=False,
    schema="tracks", if_exists='append')
except Exception as e:
    print("Data is already present")
conn.close()
print("Database closed")
# Schedule
