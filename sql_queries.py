# DROP TABLES

songplay_table_drop = """
DROP TABLE IF EXISTS song_play;
"""
user_table_drop = """
DROP TABLE IF EXISTS app_user;
"""
song_table_drop = """
DROP TABLE IF EXISTS song;
"""
artist_table_drop = """
DROP TABLE IF EXISTS artist;
"""
time_table_drop = """
DROP TABLE IF EXISTS time;
"""

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS song_play (
    songplay_id SERIAL  PRIMARY KEY, 
    start_time  bigint, 
    user_id     int, 
    level       varchar, 
    song_id     varchar, 
    artist_id   varchar, 
    session_id  int, 
    location    varchar, 
    user_agent  varchar,
    CONSTRAINT fk_user 
        FOREIGN KEY(user_id) 
            REFERENCES app_user(user_id),
    CONSTRAINT fk_song 
        FOREIGN KEY(song_id) 
            REFERENCES song(song_id),
    CONSTRAINT fk_artist 
        FOREIGN KEY(artist_id) 
            REFERENCES artist(artist_id));
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS app_user (
    user_id     INT     PRIMARY KEY, 
    first_name  varchar, 
    last_name   varchar, 
    gender      char, 
    level       varchar)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song (
    song_id     varchar     PRIMARY KEY, 
    title       varchar, 
    artist_id   varchar, 
    year        int, 
    duration    numeric)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist (
    artist_id   varchar     PRIMARY KEY, 
    name        varchar, 
    location    varchar, 
    latitude    numeric, 
    longitude   numeric)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time  bigint, 
    hour        int, 
    day         int, 
    week        int, 
    month       varchar, 
    year        int, 
    weekday     int)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO song_play (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
INSERT INTO app_user (user_id, first_name, last_name, gender, level) 
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT DO NOTHING;
""")

song_table_insert = ("""
INSERT INTO song (song_id, title, artist_id, year, duration) 
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artist (artist_id, name, location, latitude, longitude) 
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
VALUES (%s, %s, %s, %s, %s, %s, %s);
""")

# FIND SONGS

song_select = ("""
SELECT song_id, song.artist_id
FROM song 
JOIN artist ON song.artist_id=artist.artist_id
WHERE (title=$title$%s$title$ AND name=$name$%s$name$ AND duration=%s);
""")


def build_song_select_query(title, name, duration):
    return f"""
    SELECT song_id, song.artist_id
    FROM song 
    JOIN artist ON song.artist_id=artist.artist_id
    WHERE (title=$title${title}$title$ AND name=$name${name}$name$ AND duration={duration});
    """

# QUERY LISTS


create_table_queries = [user_table_create, song_table_create,
                        artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
