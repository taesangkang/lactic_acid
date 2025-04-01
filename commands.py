from datetime import datetime
import mysql.connector


def create_tables(connection):
    cursor = connection.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Player (
        ID CHAR(8) PRIMARY KEY,
        Name VARCHAR(255) NOT NULL,
        Rating FLOAT NOT NULL CHECK (Rating >= 0)
    )""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Game (
        Time DATETIME NOT NULL,
        Acidic CHAR(8) NOT NULL,
        Alkaline CHAR(8) NOT NULL,
        AcScore INT CHECK (AcScore BETWEEN 0 AND 10),
        AkScore INT CHECK (AkScore BETWEEN 0 AND 10),
        AcRating FLOAT CHECK (AcRating >= 0),
        AkRating FLOAT CHECK (AkRating >= 0),
        Tournament VARCHAR(40),
        PRIMARY KEY (Time, Acidic)
    )""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Tournament (
        Name VARCHAR(40) PRIMARY KEY,
        Organizer CHAR(8) NOT NULL
    )""")
    connection.commit()
    cursor.close()


def clear_tables(connection):
    cursor = connection.cursor()
    try:
        cursor.execute("DELETE FROM Game")
        cursor.execute("DELETE FROM Tournament")
        cursor.execute("DELETE FROM Player")
        connection.commit()
    except mysql.connector.Error:
        create_tables(connection)
    finally:
        cursor.close()


def add_player(connection, player_id, name, rating):
    try:
        if float(rating) < 0:
            return False
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO Player VALUES (%s, %s, %s)",
            (player_id, name, float(rating))
        )
        connection.commit()
        cursor.close()
        return True
    except:
        connection.rollback()