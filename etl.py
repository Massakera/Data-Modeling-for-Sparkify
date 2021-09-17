import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file"""
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()

    cur.execute(song_table_insert, list(song_data))

    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()

    cur.execute(artist_table_insert, list(artist_data))


def process_log_file(cur, filepath):
    """This procedure is quite similar to the first one. It processes a log file whose filepath has been provided as an argument.
    It filters by the NexSong action.
    Converts the timestamp column to datetime data type in order to store it into to the time table.
    Creates a subset of the main dataframe called user_df that will the user table.
    Extracts the user information and store it into the user table.
    Then extracts the songid and artistid from song and artist table and store it into the songplay table.
    
    INPUTS:
    * cur the cursor variable
    * filepath the file path to the log file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    # filter by NextSong action
    df = df[df['page']=='NextSong']
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour.values, t.dt.day.values, t.dt.week.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values)
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(data=list(zip(*time_data)), columns=column_labels)
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            print(results)
            songid, artistid = results
            print(results)
        else:
            songid, artistid = None, None
        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    """This is the final procedure of the script. Here we processs all the data that we have.
    First we get all the files matching extension from the directory.
    Gets the number of the total files found.
    Then it iterates over the files and finally all the data is processed to our connection.
    
    INPUTS:
    * cur the cursor variable
    * conn connection to the database
    * filepath the file path to the directory
    * func the function that is being used to process the data"""
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()