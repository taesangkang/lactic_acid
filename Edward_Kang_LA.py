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
    cursor.execute("INSERT INTO Player (ID, Name, Rating) VALUES (%s, %s, %s)", (id_of_player, name_of_player, rating_of_player))
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
def player_exists(connection, player_id):
    cursor = connection.cursor()
    cursor.execute("SELECT 1 FROM Player WHERE ID = %s", (player_id,))
    exists = cursor.fetchone() is not None
    cursor.close()
    return exists
def enter_new_gameInfo(connection, date, time, Acidic, Alkaline, AcScore=None, AkScore=None, AcRating=None, AkRating=None, Tournament=None):
    cursor = connection.cursor()
    datetime_str = f"{date[:4]}-{date[4:6]}-{date[6:]} {time[:2]}:{time[2:]}:00"
    if AcScore is not None and AkScore is not None and AcRating is not None and AkRating is not None:
        if not does_player_have_result(connection, Alkaline, datetime_str) or not does_player_have_result(connection,Acidic,datetime_str):
            cursor.close()
            return False
    try:
        cursor.execute("INSERT INTO Game VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",(datetime_str, Acidic, Alkaline, AcScore, AkScore, AcRating, AkRating, Tournament))
    except:
        cursor.close()
        return False
    if AcScore is not None and AkScore is not None and AcRating is not None and AkRating is not None:
        cursor.execute("UPDATE Player SET Rating = %s"
                       "WHERE ID = %s", (AcRating, Acidic))
        cursor.execute("UPDATE Player SET Rating = %s"
                       "WHERE ID = %s", (AkRating, Alkaline))
    if not player_exists(connection, Acidic) or not player_exists(connection, Alkaline):
        cursor.close()
        return False
    if AcScore is not None and (int(AcScore) < 0 or int(AcScore) > 10):
        cursor.close()
        return False
    if AkScore is not None and (int(AkScore) < 0 or int(AkScore) > 10):
        cursor.close()
        return False
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
    cursor.execute("""UPDATE Game SET AcScore = %s, AkScore = %s, AcRating = %s, AkRating = %s
        WHERE Time = %s AND Acidic = %s AND Alkaline = %s""", (AcScore, AkScore, AcRating, AkRating, datetime_str, Acidic, Alkaline))
    cursor.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (AcRating, Acidic))
    cursor.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (AkRating, Alkaline))
    connection.commit()
    cursor.close()
    return True
#function to enter tournament game
def enter_tournament(connection, name, organizer):
    if not player_exists(connection, organizer):
        return False
    try:
        cursor = connection.cursor()
        cursor.execute("INSERT INTO Tournament (Name, Organizer) VALUES (%s, %s)", (name, organizer))
        connection.commit()
        cursor.close()
        return True
    except:
        connection.rollback()
        return False
#queries P, T, H, D
#function to return information about player : P
def player_information_query(connection, id_of_player):
    cursor = connection.cursor()
    #get name and rating
    cursor.execute("SELECT Name, Rating FROM Player WHERE ID = %s", (id_of_player,))
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
        WHERE (Acidic = %s OR Alkaline = %s)
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
    cursor.execute("""SELECT DATE_FORMAT(g.Time, '%Y/%m/%d'), DATE_FORMAT(g.Time, '%H:%i'), p1.Name, g.Acidic, p2.Name, g.Alkaline, g.AcScore, g.AkScore
        FROM game g JOIN Player p1 ON g.Acidic = p1.ID JOIN Player p2 ON g.Alkaline = p2.ID
        WHERE g.tournament = %s
        ORDER BY g.Time""",(tournament,))
    tournament_list = cursor.fetchall()
    for game in tournament_list:
        date, time, acidic_name, acidic_id, alkaline_name, alkaline_id, ac_score, ak_score = game
        print(f"{date},{time},{acidic_name},{acidic_id},{alkaline_name},{alkaline_id},{ac_score},{ak_score}")
    cursor.close()
#function to list head to head : H
def h2h_list_query(connection, id_player1, id_player2):
    cursor = connection.cursor()
    cursor.execute("""SELECT DATE_FORMAT(g.Time, '%Y/%m/%d'), DATE_FORMAT(g.Time, '%H:%i'), p1.Name, g.Acidic, p2.Name, g.Alkaline, g.AcScore, g.AkScore
        FROM game g
        JOIN Player p1 ON g.Acidic = p1.ID
        JOIN Player p2 ON g.Alkaline = p2.ID
        WHERE (g.Acidic = %s AND g.Alkaline = %s)
        OR (g.Acidic = %s AND g.Alkaline = %s)
        ORDER BY g.Time""", (id_player1, id_player2, id_player2, id_player1))
    h2h_list = cursor.fetchall()
    if not h2h_list:
        print("False")
        return
    for game in h2h_list:
        date, time, acidic_name, acidic_id, alkaline_name, alkaline_id, ac_score, ak_score = game
        print(f"{date},{time},{acidic_name},{acidic_id},{alkaline_name},{alkaline_id},{ac_score},{ak_score}")
    cursor.close()
#function to list rankings : D
def ranking_list_query(connection, start_date, end_date):
    cursor = connection.cursor()
    starting_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]} 00:00:00"
    ending_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]} 23:59:59"
    cursor.execute("""SELECT DISTINCT p.ID, p.Name FROM Player p
    JOIN Game g on p.ID = g.Acidic OR p.ID = g.Alkaline
    WHERE g.Time BETWEEN %s AND %s
    AND g.AcScore IS NOT NULL AND g.AkScore IS NOT NULL""", (starting_date, ending_date))
    players = cursor.fetchall()
    if not players:
        print("None")
        return
    player_stats = []
    for player_id, player_name in players:
        cursor.execute("""
            SELECT COUNT(*) FROM Game 
            WHERE (Acidic = %s OR Alkaline = %s)
            AND Time BETWEEN %s AND %s
            AND AcScore IS NOT NULL AND AkScore IS NOT NULL
        """, (player_id, player_id, starting_date, ending_date))
        games = cursor.fetchone()[0]
        cursor.execute("""
            SELECT COUNT(*) FROM Game
            WHERE ((Acidic = %s AND AcScore > AkScore)
                OR (Alkaline = %s AND AkScore > AcScore))
            AND Time BETWEEN %s AND %s
            AND AcScore IS NOT NULL AND AkScore IS NOT NULL
        """, (player_id, player_id, starting_date, ending_date))
        wins = cursor.fetchone()[0]
        cursor.execute("""
            SELECT COUNT(*) FROM Game
            WHERE (Acidic = %s OR Alkaline = %s)
            AND AcScore = AkScore
            AND Time BETWEEN %s AND %s
            AND AcScore IS NOT NULL AND AkScore IS NOT NULL
        """, (player_id, player_id, starting_date, ending_date))
        ties = cursor.fetchone()[0]
        losses = games - wins - ties
        points = 2 * wins + ties
        points = 2 * wins + ties
        player_stats.append((player_id, player_name, games, wins, ties, losses, points))
        player_stats.sort(key=lambda x: x[6], reverse=True)
    for player_data in player_stats:
        print(",".join(str(x) for x in player_data))
    cursor.close()
#main
#csv processor
def process_command(connection, row):
    command = row[0]
#data manip commands
    if command == 'e':
        create_tables(connection)
    elif command == 'c':
        clear_tables(connection)
    elif command == 'p':
        try:
            add_player(connection, row[1], row[2], row[3])
        except:
            print(f"{','.join(row)} Input Invalid")
    elif command == 'g':
        try:
            validated = False
            if len(row) >= 9:
                validated = enter_new_gameInfo(connection, row[1],row[2],row[3],row[4],int(row[5]),int(row[6]),float(row[7]),float(row[8]))
            else:
                validated = enter_new_gameInfo(connection, row[1], row[2], row[3], row[4])
            if not validated:
                print(f"{','.join(row)} Input Invalid")
        except:
            print(f"{','.join(row)} Input Invalid")
    elif command == 'r':
        try:
            validated = False
            validated = update_gameInfo(connection, row[1],row[2],row[3],row[4],int(row[5]),int(row[6]),float(row[7]),float(row[8]))
            if not validated:
                print(f"{','.join(row)} Input Invalid")
        except:
            print(f"{','.join(row)} Input Invalid")
#Queries
    elif command == 'P':
        player_information_query(connection, row[1])
    elif command == 'T':
        tournament_list_query(connection, row[1])
    elif command == 'H':
        h2h_list_query(connection, row[1], row[2])
    elif command == 'D':
        ranking_list_query(connection, row[1], row[2])
#main function
def main():
    connection = db_connection()
    if not connection:
        return
    filename = input("Enter the name of the file: ")
    #tournament mode
    tourney_active = False
    tourney_name = None
    games_left = 0
    with open(filename, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if not row:
                continue
            command = row[0]
            if tourney_active:
                if command == 'g':
                    validated = False
                    if len(row) >= 9:
                        validated = enter_new_gameInfo(connection, row[1],row[2],row[3],row[4],int(row[5]),int(row[6]),float(row[7]),float(row[8]),Tournament=tourney_name)
                    else:
                        validated =enter_new_gameInfo(connection, row[1], row[2], row[3], row[4],Tournament=tourney_name)
                    if not validated:
                        print(f"{','.join(row)} Input Invalid")
                    games_left = games_left - 1
                    if games_left == 0:
                        tourney_active = False
                elif command in ['t','e','c']:
                    pass
                else:
                    process_command(connection, row)
            else:
                if command == 't':
                    validated = False
                    tourney_name = row[1]
                    organizer_id = row[2]
                    games_left = int(row[3])
                    validated = enter_tournament(connection, tourney_name, organizer_id)
                    if validated:
                        tourney_active = True
                    if not validated:
                        print(f"{','.join(row)} Input Invalid")
                else:
                    process_command(connection, row)
if __name__ == "__main__":
    main()





