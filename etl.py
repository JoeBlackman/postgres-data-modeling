"""
This module is an ETL pipeline for extracting song metadata and user activity logs, transforming it wherever necessary, and loading it into a database for analytical queries to be run against.
This will be accomplished by crawling over each file in certain directories containing metadata and log files, reading their contents into pandas dataframes, and performing SQL insertions with their contents.
"""

import datetime
import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *


def process_song_file(cur, filepath):
    """
    A method for reading a song metadata file and inserting data from it into the song and artist tables of the sparkifydb database.

    Arguments:
    cur -- a Postgres datbase adapter cursor object to operate on the database with
    filepath -- a string representation of the path to the song metadata file to read
    """
    # open song file
    song_data_df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = song_data_df[['song_id', 'title',
                              'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = song_data_df[['artist_id', 'artist_name', 'artist_location',
                                'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    A method for reading a user activity log file and inserting data from it into the time, user, and song_play tables of the sparkifydb database.

    Arguments:
    cur -- a Postgres datbase adapter cursor object to operate on the database with
    filepath -- a string representation of the path to the user activity log file to read
    """
    # open log file
    log_data_df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    log_data_df = log_data_df[log_data_df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(log_data_df['ts'], unit='ms')
    log_data_df['ts'] = pd.to_datetime(log_data_df['ts'], unit='ms')

    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day,
                 t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('timestamp', 'hour', 'day',
                     'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    # TODO: times look wrong in pgadmin 4. clearly something is going wrong with my datetime conversions
    # FIXED: added unit='ms' on pd.to_datetime() call

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = log_data_df[['userId', 'firstName',
                           'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(build_user_table_insert(
            row.userId, row.firstName, row.lastName, row.gender, row.level))

    # insert songplay records
    for index, row in log_data_df.iterrows():

        # get songid and artistid from song and artist tables
        # string substitution not working well with the following line
        # created build_song_select_query method in sql_queries to better handle query string formatting
        #cur.execute(song_select, (row.song, row.artist, row.length))
        cur.execute(build_song_select_query(row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        # FIXED: created build_song_select_query method in sql_queries.py
        # this was more annoying to test than it should have been.
        # very sneaky that i had to use a $token$ in the query because a song name ended with a '$' character

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid,
                         artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    A method for calling a function for each file found by walking/crawling over the subdirectories of a directory path

    Arguments:
    cur -- a Postgres datbase adapter cursor object to operate on the database with. will get passed as an argument to func.
    conn --a Postgres datbase adapter connection object, the handler for all attributes of the database connection and parent object to the cur object. will get apssed as an argument to func.
    filepath -- a string representation of the path to the top level directory that contains either song metadata files or user activity log files
    func -- the name of a method in this file to be called iteratively for each file found in the filepath directory and its subdirectories
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    """
    This method serves as an entry point for calling code in this file. Calling "python etl.py" from the command line will run this code.
    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
