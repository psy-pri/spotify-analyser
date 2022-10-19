"""
Author: Priyanka Shukla
This script extracts user songs played in the last 24 hours,
loads the data into a database
"""

from datetime import datetime
import datetime
import base64
import urllib
import psycopg2
import sqlalchemy
import pandas as pd
import requests as re
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from constant import DATABASE_ENGINE
from constant import TOKEN_URL
from constant import CLIENT_ID
from constant import CLIENT_SECRET
from constant import CHROME_DRIVER_PATH

#pylint: disable=W0621,W0703

def check_if_valid_data(data_frame: pd.DataFrame) -> bool:
    """
    Function to check if data is valid
    param: DataFrame
    returns: Boolean
    """
    # Check if dataframe is empty
    if data_frame.empty:
        print("No songs downloaded. Finishing execution")
        return False
    # Primary key check
    if pd.Series(data_frame["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary key check failed. Terminating program")
    # Check if null values exist
    if data_frame.isnull().values.any():
        raise Exception("Null values found. Terminating program")

    # Check that all timestamps are of yesterday's date
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    # yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    # today = datetime.datetime.now()
    # yesterday = today - datetime.timedelta(days = 1)
    # print("yesterday:", yesterday)
    # timestamps = data_frame["ts"].tolist()
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

def access_token():
    """
    This method is to generate access token
    returns: access token
    """
    auth_code = {
        'response_type': 'code',
        'client_id': CLIENT_ID,
        'scope': 'user-read-recently-played',
        'redirect_uri': 'http://localhost:8080',
    }


    driver = webdriver.Chrome(CHROME_DRIVER_PATH)
    driver.get("https://accounts.spotify.com/authorize?" + urllib.parse.urlencode(auth_code))
    wait = WebDriverWait(driver, 60)
    wait.until(EC.url_contains('http://localhost:8080'))
    get_url = driver.current_url
    print("The current url is:"+str(get_url))
    url_code = str(get_url)
    driver.quit()
    idx = (url_code.find('='))+1
    code = ((url_code[idx:-4].lstrip()).rstrip())

    print("code:",code)

    #set header
    encode_id_secret = f"{CLIENT_ID}:{CLIENT_SECRET}".encode("ascii")
    auth_header = base64.b64encode(encode_id_secret)
    auth_header = auth_header.decode("ascii")
    headers = {
            "Authorization": f"Basic  {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded"
    }

    #data
    payload = {
        "code": code,
        "redirect_uri": 'http://localhost:8080',
        "grant_type": "authorization_code"
        }

    # Make a request to the /token endpoint to get an access token
    access_token_request = re.post(TOKEN_URL, headers=headers, data=payload, timeout=180)
    # convert the response to JSON
    access_token_response_data = access_token_request.json()

    try:
        return access_token_response_data['access_token']
    except KeyError:
        err = '\x1b[0;30;41m' + 'Error ocured' + '\x1b[0m'
        print(err,'(Make sure you enter right code)')
    return None


def run_spotify_etl():

    access_token = access_token()

    print(f"access_token: {access_token}")

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    today = datetime.datetime.now()
    today_unix_ts = int(today.timestamp()) * 1000
    yesterday = today - datetime.timedelta(days = 1)
    #print("yesterday ts:" , yesterday)
    yesterday_unix_ts = int(yesterday.timestamp()) * 1000
    #print("yesterday ts unix ", yesterday_unix_ts)


    headers = {'Authorization': f'Bearer {access_token}','Content-Type': 'application/json'}
    RECENTLY_PLAYED_URL = "https://api.spotify.com/v1/me/player/recently-played"
    recently_played = re.get(f"{RECENTLY_PLAYED_URL}",headers=headers, timeout=180)

    print(recently_played)
    data = recently_played.json()

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
    song_data_frame = pd.DataFrame(data = song_dict)
    print('Spotify data data_frame format:', song_data_frame)

    # Validate
    if check_if_valid_data(song_data_frame):
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
        song_data_frame.to_sql("my_played_tracks", con=db_engine, index=False,
        schema="tracks", if_exists='append')
    except Exception as e:
        print("Data is already present")
    conn.close()
    print("Database closed")


