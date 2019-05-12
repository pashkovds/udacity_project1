import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    ''' Function to process single song file.
    Read song and artist json data from filepath, process it and write separatly to song and artist tables.
    Two queries used during execution: song_table_insert, artist_table_insert
    
    Args:
            cur (psycopg2.cursor): Cursor to uxecute commands in database session
            filepath (str): Path to file with data
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)
    number_of_record = 0
    df_one_row = df.iloc[number_of_record,:]
    
    # insert song record
    song_required_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data_dict = dict(df_one_row[song_required_columns])
    song_data_dict['year'] = int(song_data_dict['year'])
    song_data_dict['duration'] = float(song_data_dict['duration'])
    song_data = [value for key,value in song_data_dict.items()]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_required_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_data_dict = dict(df_one_row[artist_required_columns])
    artist_data_dict['artist_latitude'] = float(artist_data_dict['artist_latitude'])
    artist_data_dict['artist_longitude'] = float(artist_data_dict['artist_longitude'])
    artist_data = [value for key,value in artist_data_dict.items()]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    ''' Function to process single log file.
    Read log data from filepath, process it and write separatly to user, time and songplay tables.
    Take in account that song_select query used to find songs and artists IDs which corresposnd particular song title and artist`s name.
    Queries used to insert data: time_table_insert, user_table_insert , songplay_table_insert
    
    Args:
            cur (psycopg2.cursor): Cursor to uxecute commands in database session
            filepath (str): Path to file with data
    '''
    # open log file
    df =  pd.read_json(filepath, lines=True)
    df['ts_prc'] = pd.to_datetime(df['ts'], unit='ms')
    
    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    print('insert time data records:' + filepath)
    time_data = [(  
        cut_dt.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S'),
        cut_dt.hour, 
        cut_dt.day,
        cut_dt.week,
        cut_dt.month,
        cut_dt.year,
        cut_dt.weekday()
    ) for cut_dt in t]
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday') 
    time_df = pd.DataFrame(data=time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    users_required_columns = ['userId', 'firstName','lastName','gender','level']
    user_df = df[users_required_columns]
    user_df.columns = ['user_id', 'first_name','last_name','gender','level']

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        #cur.execute(song_select, (row.song, row.artist))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
         row['ts_prc'].to_pydatetime().strftime('%Y-%m-%d %H:%M:%S'), 
         int(row['userId']),
         str(row['level']),
         songid, 
         artistid,
         str(row['sessionId']),
         str(row['location']),
         str(row['userAgent'])
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    ''' Function find all subdirectory leaf files from filepath.
    For each file func was applied to write internal data to database
    
    Args:
            cur (psycopg2.cursor): Cursor to uxecute commands in database session
            conn (psycopg2.connection): Postgresql connection
            filepath (str): Path to file with data
            func (function): function(cur, filepath) which takes one particular file to database
            
    '''
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