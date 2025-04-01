import mysql.connector
import csv
from datetime import datetime

#function to connect to database
def db_connection():
    return mysql.connector.connect(host = "localhost", user= "cs5330", password="pw5330", database = "dbprog")
#data manipulation commands e, c, p, g, r, t
#function to create tables : e
def create_tables(connection):
    cursor = connection.cursor()
    #check if player table exists, if not create it
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Player(
    ID CHAR(8) PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    Rating FLOAT NOT NULL CHECK (Rating >= 0))""")
    #check if game table exists, if not create it
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Game(
    Time DATETIME NOT NULL,
    Acidic CHAR(8) NOT NULL,
    Alkaline CHAR(8) NOT NULL,
    AcScore INT CHECK (AcScore BETWEEN 0 AND 10),
    AkScore INT CHECK (AkScore BETWEEN 0 AND 10),
    AcRating FLOAT CHECK (AcRating >= 0),
    AkRating FLOAT CHECK (AcRating >= 0),
    Tournament VARCHAR(40),
    PRIMARY KEY (Time, Acidic))""")
    #check if tournament table exists, if not create it
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Tournament(
    NAME VARCHAR(40) PRIMARY KEY,
    Organizer CHAR(8) NOT NULL)""")
    #commit changes and close the cursor
    connection.commit()
    cursor.close()
#function to clear all tables : c
def clear_tables(connection):
    cursor = connection.cursor()
    #delete data from the tables but not the tables themselves
    cursor.execute("DELETE FROM Player")
    cursor.execute("DELETE FROM Game")
    cursor.execute("DELETE FROM Tournament")
    connection.commit()
    cursor.close()
#function to add player : p
def add_player(connection, id_of_player, name_of_player, rating_of_player):
    cursor = connection.cursor()
    cursor.execute("INSERT INTO Player VALUES (%s, %s, %s)", (id_of_player, name_of_player, rating_of_player))
    connection.commit()
    cursor.close()
#constraint for both g and r: can not enter if there is an earlier game for any of the two players that does not have a result entered
#helper function to check if player has all results
def does_player_have_result(connection, id_of_player, datetime_str):
    cursor = connection.cursor()
    cursor.execute("""
    SELECT COUNT(*)
    FROM Game
    WHERE Time < %s
    AND (Acidic = %s OR Alkaline = %s)
    AND (AcScore IS NULL OR AkScore IS NULL)
    """, (datetime_str, id_of_player, id_of_player))
    amt_of_unfinished = cursor.fetchone()[0]
    cursor.close()
    return amt_of_unfinished == 0
#function to add game  *THAT DOES NOT EXIST: g
def enter_new_gameInfo(connection, date, time, Acidic, Alkaline, AcScore=None, AkScore=None, AcRating=None, AkRating=None, Tournament=None):
    cursor = connection.cursor()
    datetime_str = f"{date[:4]}-{date[4:6]}-{date[6:]} {time[:2]}:{time[2:]}:00"
    if not does_player_have_result(connection, Alkaline, datetime_str) or not does_player_have_result(connection, Acidic, datetime_str):
        cursor.close()
        return False
    cursor.execute("INSERT INTO Game VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (datetime_str, Acidic, Alkaline, AcScore, AkScore, AcRating, AkRating, Tournament))
    if AcScore is not None and AkScore is not None and AcRating is not None and AkRating is not None:
        cursor.execute("UPDATE Player SET Rating = %s"
                       "WHERE ID = %s", (AcRating, Acidic))
        cursor.execute("UPDATE Player SET Rating = %s"
                       "WHERE ID = %s", (AkRating, Alkaline))
    connection.commit()
    cursor.close()
    return True
#function to enter result of game *THAT DOES EXIST: r
def update_gameInfo(connection, date, time, Acidic, Alkaline, AcScore, AkScore, AcRating, AkRating):
    cursor = connection.cursor()
    datetime_str = f"{date[:4]}-{date[4:6]}-{date[6:]} {time[:2]}:{time[2:]}:00"
    if not does_player_have_result(connection, Alkaline, datetime_str) or not does_player_have_result(connection, Acidic, datetime_str):
        cursor.close()
        return False
    cursor.execute("UPDATE Game SET Time = %s, Acidic = %s, Alkaline = %s "
                   "WHERE AcScore = %s, AkScore = %s, AcRating = %s, AkRating = %s (datetime_str, Acidic, Alkaline, AcScore, AkScore, AcRating, AkRating)")
    cursor.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (AcRating, Acidic))
    cursor.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (AkRating, Alkaline))
    connection.commit()
    cursor.close()
    return True
#function to enter tournament game
def enter_tournament(connection, name, organizer):
    cursor = connection.cursor()
    cursor.execute("INSERT into Tournament (Name, Organizer) VALUES (%s,%s)", (name,organizer))
    connection.commit()
    cursor.close()
#queries P, T, H, D
#function to return information about player : P
def player_information_query(connection, id_of_player):
    cursor = connection.cursor()
    #get name and rating
    cursor.execute("SELECT Name, Rating FROM id_of_player WHERE ID = %s", (id_of_player))
    player = cursor.fetchone()
    if not player:
        print("None")
        return
    name, rating = player
    #get number of games played
    cursor.execute(""" SELECT COUNT(*) FROM Game 
        WHERE (Acidic = %s OR Alkaline = %s) 
        AND AcScore IS NOT NULL AND AkScore IS NOT NULL
    """, (id_of_player, id_of_player))
    num_games = cursor.fetchone()[0]
    #get number of wins
    cursor.execute("""SELECT COUNT(*) FROM Game
        WHERE (Acidic = %s AND AcScore > AkScore)
        OR (Alkaline = %s AND AkScore > AcScore)
        AND (AkScore IS NOT NULL and AcScore IS NOT NULL)""", (id_of_player, id_of_player))
    num_wins = cursor.fetchone()[0]
    #get number of ties
    cursor.execute("""SELECT COUNT(*) FROM Game
        WHERE (Acidic = %s AND Alkaline = %s)
        AND (AcScore = AkScore)
        AND (AkScore IS NOT NULL AND AkScore IS NOT NULL)""", (id_of_player, id_of_player))
    num_ties = cursor.fetchone()[0]
    #get number of losses
    cursor.execute("""SELECT COUNT(*) FROM Game
        WHERE (Acidic = %s AND AcScore < AkScore)
        OR (Alkaline = %s AND AkScore < AcScore)
        AND (AkScore IS NOT NULL and AcScore IS NOT NULL)""", (id_of_player, id_of_player))
    num_losses = cursor.fetchone()[0]
    print(f"{name},{rating:.2f},{num_games},{num_wins},{num_ties},{num_losses}")
    cursor.close()
#function to list the tournaments : T
def tournament_list_query(connection, tournament):
    cursor = connection.cursor()
    cursor.execute("""SELECT DATE_FORMAT(g.Time, '%Y/%m/%d'), DATE_FORMAT(g.Time, '%H:%i'), Name, Acidic, Name, Alkaline, AcScore, AkScore
        FROM game g JOIN Player p1 ON g.Acidic = p1.ID JOIN Player p2 ON g.Alkaline = p2.ID
        WHERE g.tournament = %s
        ORDER BY g.Time""", tournament)
    tournament_list = cursor.fetchall()
    if not tournament_list:
        print("False")
        return
    for games in tournament_list:
        date, time, name, acidic, name, alkaline, acscore, akscore = tournament_list
        print(f"{date},{name},{acidic},{name},{alkaline},{acscore},{akscore}")
#function to list head to head : H
def h2h_list_query():
    pass
#function to list rankings : D
def ranking_list_query():
    pass
#main
def main():
    pass

if __name__ == "__main__":
    main()





