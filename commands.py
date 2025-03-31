from datetime import datetime
import mysql.connector


def create_tables(connection):
    cursor = connection.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Player (
        ID CHAR(8) PRIMARY KEY,
        Name VARCHAR(255) NOT NULL,
        Rating FLOAT NOT NULL CHECK (Rating >= 0)
    )
    """)

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
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Tournament (
        Name VARCHAR(40) PRIMARY KEY,
        Organizer CHAR(8) NOT NULL
    )
    """)

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
    cursor = connection.cursor()
    try:
        if float(rating) < 0:
            return False

        cursor.execute(
            "INSERT INTO Player (ID, Name, Rating) VALUES (%s, %s, %s)",
            (player_id, name, float(rating))
        )
        connection.commit()
        return True
    except mysql.connector.Error:
        connection.rollback()
        return False
    finally:
        cursor.close()

def check_earlier_games_without_results(connection, datetime_str, acidic, alkaline):
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(*) FROM Game 
            WHERE Time < %s AND (Acidic = %s OR Acidic = %s OR Alkaline = %s OR Alkaline = %s)
            AND (AcScore IS NULL OR AkScore IS NULL)
        """, (datetime_str, acidic, alkaline, acidic, alkaline))

        count = cursor.fetchone()[0]
        return count > 0
    finally:
        cursor.close()


def add_game(connection, date, time, acidic, alkaline, ac_score=None, ak_score=None,
             ac_rating=None, ak_rating=None, tournament=None):
    cursor = connection.cursor()

    try:
        cursor.execute("SELECT ID FROM Player WHERE ID = %s", (acidic,))
        if not cursor.fetchone():
            return False
        cursor.execute("SELECT ID FROM Player WHERE ID = %s", (alkaline,))
        if not cursor.fetchone():
            return False
        if ac_score is not None and (ac_score < 0 or ac_score > 10):
            return False
        if ak_score is not None and (ak_score < 0 or ak_score > 10):
            return False
        datetime_str = f"{date[:4]}-{date[4:6]}-{date[6:]} {time[:2]}:{time[2:]}:00"
        game_datetime = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")

        if ac_score is not None and ak_score is not None:
            if check_earlier_games_without_results(connection, game_datetime, acidic, alkaline):
                return False

        cursor.execute(
            "INSERT INTO Game (Time, Acidic, Alkaline, AcScore, AkScore, AcRating, AkRating, Tournament) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (game_datetime, acidic, alkaline, ac_score, ak_score, ac_rating, ak_rating, tournament)
        )

        if ac_score is not None and ak_score is not None and ac_rating is not None and ak_rating is not None:
            cursor.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (ac_rating, acidic))
            cursor.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (ak_rating, alkaline))

        connection.commit()
        return True
    except mysql.connector.Error:
        connection.rollback()
        return False
    finally:
        cursor.close()


def enter_game_result(connection, date, time, acidic, alkaline, ac_score, ak_score, ac_rating, ak_rating):
    cursor = connection.cursor()

    datetime_str = f"{date[:4]}-{date[4:6]}-{date[6:]} {time[:2]}:{time[2:]}:00"
    game_datetime = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")

    try:
        cursor.execute("""
            SELECT COUNT(*) FROM Game 
            WHERE Time < %s AND (Acidic = %s OR Acidic = %s OR Alkaline = %s OR Alkaline = %s)
            AND (AcScore IS NULL OR AkScore IS NULL)
        """, (game_datetime, acidic, alkaline, acidic, alkaline))

        if cursor.fetchone()[0] > 0:
            return False

        cursor.execute(
            "UPDATE Game SET AcScore = %s, AkScore = %s, AcRating = %s, AkRating = %s "
            "WHERE Time = %s AND Acidic = %s AND Alkaline = %s",
            (ac_score, ak_score, ac_rating, ak_rating, game_datetime, acidic, alkaline)
        )

        cursor.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (ac_rating, acidic))
        cursor.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (ak_rating, alkaline))

        connection.commit()
        return True
    except mysql.connector.Error:
        connection.rollback()
        return False
    finally:
        cursor.close()


def add_tournament(connection, name, organizer):
    cursor = connection.cursor()
    try:
        cursor.execute(
            "INSERT INTO Tournament (Name, Organizer) VALUES (%s, %s)",
            (name, organizer)
        )
        connection.commit()
        return True
    except mysql.connector.Error:
        connection.rollback()
        return False
    finally:
        cursor.close()