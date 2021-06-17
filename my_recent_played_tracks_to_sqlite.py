import pandas as pd 
import requests 
import json 
import datetime
import sqlite3
#%%
"Part One - Extract Recently Played Songs from Spotify API"
def main():
    
    global song_df
    TOKEN = "BQC3m99uUTKLD794tAJLfOE-gyMk4h2qMFQ6dHJiZgq13_0uvcD-8HfWl_hgDm8tHr1x9PcPAZP9k2yV74TvZN0SpNRaqYAlzS4cDT0PC7dPQGTVFo53vrPF3U8uBGtk-eRPkLlL_U0QWCcuyks0lxsqGwTBnigOLaaiiHqJK30CERbw5CmFG4odNgu0hV_P"
    YESTERDAY_DATE = int((datetime.datetime.now() - datetime.timedelta(days=1)).timestamp()) * 1000
    HEADERS = {"Accept": "application/json", "Content-Type":  "application/json", "Authorization": f"Bearer {TOKEN}"}
    URL = f"https://api.spotify.com/v1/me/player/recently-played?after={YESTERDAY_DATE}&limit=10"
    R = requests.get(URL, headers = HEADERS)
    DATA = R.text
    PARSED = json.loads(DATA)
    P = PARSED["items"]
    
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
    
    for number in list(range(len(P))):
        song_names.append(P[number]["track"]["name"])
        artist_names.append(P[number]["track"]["artists"][0]["name"])
        played_at_list.append(P[number]["played_at"])
        timestamps.append(P[number]["played_at"][0:10])
    
    song_dict = { 
        "song_name": song_names,
        "artist_name": artist_names, 
        "played_at_list": played_at_list,
        "timestamps": timestamps} 
    
    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at_list", "timestamps"])
    

main()

#%%
"Part Two - Transformation of the Recently Played Songs DataFrame - Validation Stage"

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False
    
    # Primary Key Check
    if pd.Series(df["played_at_list"]).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")
        
    # Check for nulls 
    if song_df.isnull().values.any():
        raise Exception("Null values found")
        
    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    timestamps = df["timestamps"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")
    return True

if check_if_valid_data(song_df):
    print("Data valid, proceed to Load stage")

#%%
"Part Three - Loading Recently Play Songs Data into Database"

#Create a sqlite database 
conn = sqlite3.connect("Evidence.db")
cursor = conn.cursor()

sql_statement = "CREATE TABLE prepare( "
sql_statement += "song_name text, "
sql_statement += "artist_name text, "
sql_statement += "played_at_list text, "
sql_statement += "timestamps text) "


cursor.execute(sql_statement)

values = list(song_df.values.tolist())
sql_statement = "INSERT INTO prepare "
sql_statement += "(song_name, artist_name, played_at_list, timestamps) "
sql_statement += "VALUES (?, ?, ?, ?) "

for v in values:
    cursor.execute(sql_statement, v)
    

conn.commit()
conn.close()
