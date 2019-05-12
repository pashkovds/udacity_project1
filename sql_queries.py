# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time varchar, 
    user_id int, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent varchar
)
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY, 
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
)
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS song(
        song_id varchar PRIMARY KEY,
        title varchar NOT NULL, 
        artist_id varchar NOT NULL,
        year int,
        duration numeric
    );
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS artist(
        artist_id varchar PRIMARY KEY,
        name varchar,
        location varchar, 
        lattitude numeric, 
        longitude numeric
    );
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS time (
        start_time varchar PRIMARY KEY, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday int
    );
"""

# INSERT RECORDS

songplay_table_insert = """
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

user_table_insert = """
    INSERT INTO users
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
    DO NOTHING;
"""

song_table_insert = """
    INSERT INTO song (song_id, title, artist_id, year, duration)
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
    DO NOTHING;
"""


artist_table_insert = """
    INSERT INTO artist (artist_id, name, location, lattitude, longitude)
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
    DO NOTHING;
"""


time_table_insert = """
    INSERT INTO time
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time)
    DO NOTHING;
"""

# FIND SONGS

song_select = """
    SELECT 
    song_id,
    artist.artist_id
    FROM artist
    LEFT JOIN song on artist.artist_id = song.artist_id
    WHERE song.title = %s
    AND artist.name = %s
    AND song.duration = %s
"""


# TEST QUERIES
songplay_test_select = "SELECT * FROM songplays LIMIT 5;"
songplay_notnone_test_select = "SELECT * FROM songplays WHERE artist_id!='None';"
users_test_select = "SELECT * FROM users LIMIT 5;"
song_test_select = "SELECT * FROM song LIMIT 5;"
artist_test_select = "SELECT * FROM artist LIMIT 5;"
time_test_select = "SELECT * FROM time LIMIT 5;"

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
test_project_queries = [songplay_test_select, songplay_notnone_test_select, users_test_select, song_test_select, artist_test_select, time_test_select]