"""
This module is intended to be used after running etl.py
running pytest with this file should confirm that all data was loaded (correctly)
assertions will be made against data i've pulled from the metadata and log files
"""

import psycopg2
import pytest
from decimal import *


@pytest.fixture
def connection_handler():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    yield cur
    conn.close()


def test_song_play_data_quality(connection_handler):
    # verify number of song_play entities
    q_count = "SELECT COUNT(1) FROM song_play;"
    connection_handler.execute(q_count)
    actual_count = connection_handler.fetchone()[0]
    expected_count = 6820
    assert actual_count == expected_count
    # verify properties of a song_play entity (including song_id and artist_id found when available)
    q_props = "SELECT * FROM song_play WHERE user_id=15 AND start_time=1542837407796;"
    connection_handler.execute(q_props)
    actual_props = list(connection_handler.fetchone())
    expected_props = [4627, 1542837407796, 15, "paid", "SOZCTXZ12AB0182364", "AR5KOSW1187FB35FF4", 818, "Chicago-Naperville-Elgin, IL-IN-WI",
                      '\"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36\"']
    assert actual_props == expected_props


def test_app_user_data_quality(connection_handler):
    # verify number of user entities
    q_count = "SELECT COUNT(1) FROM app_user;"
    connection_handler.execute(q_count)
    actual_count = connection_handler.fetchone()[0]
    expected_count = 96
    assert actual_count == expected_count
    # verify properties of a user entity
    q_props = "SELECT * FROM app_user WHERE user_id=15;"
    connection_handler.execute(q_props)
    actual_props = connection_handler.fetchone()
    expected_props = (15, "Lily", "Koch", "F", "paid")
    assert actual_props == expected_props


def test_song_data_quality(connection_handler):
    # verify number of song entities
    q_count = "SELECT COUNT(1) FROM song;"
    connection_handler.execute(q_count)
    actual_count = connection_handler.fetchone()[0]
    expected_count = 71
    assert actual_count == expected_count
    # verify properties of a song entity
    q_props = "SELECT * FROM song WHERE song_id='SOPVXLX12A8C1402D5';"
    connection_handler.execute(q_props)
    actual_props = list(connection_handler.fetchone())
    actual_props[4] = float(actual_props[4])
    print(actual_props)
    expected_props = ["SOPVXLX12A8C1402D5", "Larger Than Life",
                      "AR3JMC51187B9AE49D", 1999, 236.25098]
    assert actual_props == expected_props


def test_artist_data_quality(connection_handler):
    # verify number of artist entities
    q_count = "SELECT COUNT(1) FROM artist;"
    connection_handler.execute(q_count)
    actual_count = connection_handler.fetchone()[0]
    expected_count = 69
    assert actual_count == expected_count
    # verify properties of an artist entity
    q_props = "SELECT * FROM artist WHERE artist_id='ARMAC4T1187FB3FA4C';"
    connection_handler.execute(q_props)
    actual_props = list(connection_handler.fetchone())
    actual_props[3] = float(actual_props[3])
    actual_props[4] = float(actual_props[4])
    expected_props = ["ARMAC4T1187FB3FA4C", "The Dillinger Escape Plan",
                      "Morris Plains, NJ",  40.82624, -74.47995]
    assert actual_props == expected_props


def test_time_data_quality(connection_handler):
    # verify number of time entities
    q_count = "SELECT COUNT(1) FROM time;"
    connection_handler.execute(q_count)
    actual_count = connection_handler.fetchone()[0]
    expected_count = 6820
    assert actual_count == expected_count
    # verify properties of a time entity
    q_props = "SELECT * FROM time WHERE start_time=1541903636796000000;"
    connection_handler.execute(q_props)
    actual_props = list(connection_handler.fetchone())
    expected_props = [1541903636796000000, 2, 11, 45, "11", 2018, 6]
    assert actual_props == expected_props
