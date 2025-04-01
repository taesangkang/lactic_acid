import mysql.connector
import csv
from datetime import datetime

#DATABASE CONNECTION


# connect to database
def connect():
    try:
        return mysql.connector.connect(
            host='localhost',
            user='cs5330',
            password='pw5330',
            database='dbprog'
        )
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None


#DATA MANIPULATION COMMANDS


# command 'e' check if tables exist, if not create them
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
    )""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Tournament (
        Name VARCHAR(40) PRIMARY KEY,
        Organizer CHAR(8) NOT NULL
    )""")
    connection.commit()
    cursor.close()


# command 'c' : clear all tables in the database
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


# command 'p' : add a new player to the database
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
        return False


def check_earlier_unfinished_games(connection, datetime_str, acidic, alkaline):
    cursor = connection.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM Game 
        WHERE Time < %s AND (Acidic = %s OR Acidic = %s OR Alkaline = %s OR Alkaline = %s)
        AND (AcScore IS NULL OR AkScore IS NULL)
    """, (datetime_str, acidic, alkaline, acidic, alkaline))
    result = cursor.fetchone()[0] > 0
    cursor.close()
    return result


def player_exists(connection, player_id):
    cursor = connection.cursor()
    cursor.execute("SELECT 1 FROM Player WHERE ID = %s", (player_id,))
    result = cursor.fetchone() is not None
    cursor.close()
    return result


# command 'g': Enter information about a game that is not in the database
# if last four fields are not present, assume game result is not yet known
def add_game(connection, date, time, acidic, alkaline, ac_score=None, ak_score=None,
             ac_rating=None, ak_rating=None, tournament=None):
    cursor = connection.cursor()
    try:
        if not player_exists(connection, acidic) or not player_exists(connection, alkaline):
            return False

        if ac_score is not None and ak_score is not None:
            ac_score = int(ac_score)
            ak_score = int(ak_score)
            if ac_score < 0 or ac_score > 10 or ak_score < 0 or ak_score > 10:
                return False

        if ac_rating is not None:
            ac_rating = float(ac_rating)
        if ak_rating is not None:
            ak_rating = float(ak_rating)

        datetime_str = f"{date[:4]}-{date[4:6]}-{date[6:]} {time[:2]}:{time[2:]}:00"

        if ac_score is not None and ak_score is not None:
            if check_earlier_unfinished_games(connection, datetime_str, acidic, alkaline):
                return False

        cursor.execute(
            "INSERT INTO Game VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (datetime_str, acidic, alkaline, ac_score, ak_score, ac_rating, ak_rating, tournament)
        )

        if ac_score is not None and ak_score is not None and ac_rating is not None and ak_rating is not None:
            cursor.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (ac_rating, acidic))
            cursor.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (ak_rating, alkaline))

        connection.commit()
        return True
    except:
        connection.rollback()
        return False
    finally:
        cursor.close()


# command 'r': enter the result of a game that is already in the database, but without the results. The fields are the same as the 'g' command, but this time every field must be present.
# "NOTE" : one cannot enter the result of a game if there is an earlier game with any of the two players that does not have a result entered
def enter_game_result(connection, date, time, acidic, alkaline, ac_score, ak_score, ac_rating, ak_rating):
    cursor = connection.cursor()
    try:
        ac_score = int(ac_score)
        ak_score = int(ak_score)
        ac_rating = float(ac_rating)
        ak_rating = float(ak_rating)

        if ac_score < 0 or ac_score > 10 or ak_score < 0 or ak_score > 10:
            return False

        datetime_str = f"{date[:4]}-{date[4:6]}-{date[6:]} {time[:2]}:{time[2:]}:00"

        if check_earlier_unfinished_games(connection, datetime_str, acidic, alkaline):
            return False

        cursor.execute(
            "UPDATE Game SET AcScore = %s, AkScore = %s, AcRating = %s, AkRating = %s "
            "WHERE Time = %s AND Acidic = %s AND Alkaline = %s",
            (ac_score, ak_score, ac_rating, ak_rating, datetime_str, acidic, alkaline)
        )

        cursor.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (ac_rating, acidic))
        cursor.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (ak_rating, alkaline))

        connection.commit()
        return True
    except:
        connection.rollback()
        return False
    finally:
        cursor.close()


# command 't' : enter a tournament
def add_tournament(connection, name, organizer):
    try:
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO Tournament VALUES (%s, %s)",
            (name, organizer)
        )
        connection.commit()
        cursor.close()
        return True
    except:
        connection.rollback()
        return False


# QUERY COMMANDS
# NOTE For all queries, if there is no correct answers (e.g. the player ID does not exist) just output "None".
# 'P' : return information about the player. subsequent fields contain - player id, player name, player rating, number of games played, number of wins, number of losses, number of ties

def get_player_info(connection, player_id):
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT Name, Rating FROM Player WHERE ID = %s", (player_id,))
        player = cursor.fetchone()
        if not player:
            print("None")
            return

        name, rating = player

        cursor.execute("""
            SELECT 
                COUNT(*), 
                SUM(CASE WHEN 
                    (Acidic = %s AND AcScore > AkScore) OR 
                    (Alkaline = %s AND AkScore > AcScore) 
                THEN 1 ELSE 0 END),
                SUM(CASE WHEN 
                    ((Acidic = %s) OR (Alkaline = %s)) AND AcScore = AkScore
                THEN 1 ELSE 0 END)
            FROM Game 
            WHERE (Acidic = %s OR Alkaline = %s) 
            AND AcScore IS NOT NULL AND AkScore IS NOT NULL
        """, (player_id, player_id, player_id, player_id, player_id, player_id))

        stats = cursor.fetchone()
        games_played, wins, ties = stats[0] or 0, stats[1] or 0, stats[2] or 0
        losses = games_played - wins - ties

        print(f"{name},{rating:.2f},{games_played},{wins},{ties},{losses}")
    except:
        print("None")
    finally:
        cursor.close()


# 'T' : list a tournament. subsequent fields contain - tournament name, date, time, acidic player name, acidic player id, alkaline player name, alkaline player id, score of acidic player, score of alkaline player

def list_tournament(connection, tournament_name):
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT 
                DATE_FORMAT(g.Time, '%Y/%m/%d'),
                DATE_FORMAT(g.Time, '%H:%i'),
                p1.Name, g.Acidic,
                p2.Name, g.Alkaline,
                g.AcScore, g.AkScore
            FROM Game g
            JOIN Player p1 ON g.Acidic = p1.ID
            JOIN Player p2 ON g.Alkaline = p2.ID
            WHERE g.Tournament = %s
            ORDER BY g.Time
        """, (tournament_name,))

        games = cursor.fetchall()
        if not games:
            print("None")
            return

        for game in games:
            date, time, acidic_name, acidic_id, alkaline_name, alkaline_id, ac_score, ak_score = game
            ac_score_str = str(ac_score) if ac_score is not None else ""
            ak_score_str = str(ak_score) if ak_score is not None else ""
            print(
                f"{date},{time},{acidic_name},{acidic_id},{alkaline_name},{alkaline_id},{ac_score_str},{ak_score_str}")
    except:
        print("None")
    finally:
        cursor.close()


# 'H' : list the head-to-head results between two players. subsequent fields contain - date, time, acidic player name, acidic player id, alkaline player name, alkaline player id, score of acidic player, score of alkaline player

def list_head_to_head(connection, player1_id, player2_id):
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT 
                DATE_FORMAT(g.Time, '%Y/%m/%d'),
                DATE_FORMAT(g.Time, '%H:%i'),
                p1.Name, g.Acidic,
                p2.Name, g.Alkaline,
                g.AcScore, g.AkScore
            FROM Game g
            JOIN Player p1 ON g.Acidic = p1.ID
            JOIN Player p2 ON g.Alkaline = p2.ID
            WHERE (g.Acidic = %s AND g.Alkaline = %s) 
               OR (g.Acidic = %s AND g.Alkaline = %s)
            ORDER BY g.Time
        """, (player1_id, player2_id, player2_id, player1_id))

        games = cursor.fetchall()
        if not games:
            print("None")
            return

        for game in games:
            date, time, acidic_name, acidic_id, alkaline_name, alkaline_id, ac_score, ak_score = game
            ac_score_str = str(ac_score) if ac_score is not None else ""
            ak_score_str = str(ak_score) if ak_score is not None else ""
            print(
                f"{date},{time},{acidic_name},{acidic_id},{alkaline_name},{alkaline_id},{ac_score_str},{ak_score_str}")
    except:
        print("None")
    finally:
        cursor.close()


# 'D': Given a start date (no time) and an end date (no time) rank all the players that has played at least one game by their performance (from best to worst), which is measured by "points" = (2 * # of wins, + 1 * # of draws). The two subsequent input fields should contain the two dates.
# The output should contain the record of a player, id of the player, name of the player, # of games played, # of wins, # of ties, # of losses
def rank_players_by_performance(connection, start_date, end_date):
    cursor = connection.cursor()
    try:
        start_datetime = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]} 00:00:00"
        end_datetime = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]} 23:59:59"

        cursor.execute("""
            SELECT p.ID, p.Name,
                COUNT(*) AS games,
                SUM(CASE WHEN 
                    (p.ID = g.Acidic AND g.AcScore > g.AkScore) OR 
                    (p.ID = g.Alkaline AND g.AkScore > g.AcScore) 
                THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN g.AcScore = g.AkScore THEN 1 ELSE 0 END) AS ties
            FROM Player p
            JOIN Game g ON p.ID = g.Acidic OR p.ID = g.Alkaline
            WHERE g.Time BETWEEN %s AND %s
            AND g.AcScore IS NOT NULL AND g.AkScore IS NOT NULL
            GROUP BY p.ID, p.Name
        """, (start_datetime, end_datetime))

        players = cursor.fetchall()
        if not players:
            print("None")
            return

        player_stats = []
        for player_id, player_name, games, wins, ties in players:
            wins = wins or 0
            ties = ties or 0
            losses = games - wins - ties
            points = 2 * wins + ties
            player_stats.append((player_id, player_name, games, wins, ties, losses, points))

        player_stats.sort(key=lambda x: x[6], reverse=True)

        for player_data in player_stats:
            print(",".join(str(x) for x in player_data))
    except:
        print("None")
    finally:
        cursor.close()

# MAIN FUNCTION
# program to manage database tables
# read csv file (use csv module)
# each line of the file contains a command in the first field and list of parameters in the subsequent fields
# tournament mode - if tournament mode is active, 't', 'e', 'c' commands are not allowed until all the games are of the tourney are played

class TournamentHandler:
    def __init__(self, connection):
        self.connection = connection
        self.active = False
        self.name = None
        self.games_remaining = 0

    def start_tournament(self, line):
        if self.active:
            print(f"{','.join(line)} Input Invalid")
            return False

        self.name = line[1]
        organizer_id = line[2]
        self.games_remaining = int(line[3])

        if add_tournament(self.connection, self.name, organizer_id):
            self.active = True
            return True
        else:
            print(f"{','.join(line)} Input Invalid")
            return False

    def add_tournament_game(self, line):
        if not self.active:
            return False

        if len(line) >= 9:
            success = add_game(
                self.connection,
                line[1], line[2], line[3], line[4],
                line[5], line[6], line[7], line[8],
                self.name
            )
        else:
            success = add_game(
                self.connection, line[1], line[2], line[3], line[4],
                tournament=self.name
            )

        if success:
            self.games_remaining -= 1
            if self.games_remaining <= 0:
                self.active = False
            return True
        else:
            print(f"{','.join(line)} Input Invalid")
            return False


def handle_command(connection, tournament_handler, line):
    command = line[0]

    if command == 'e':
        if tournament_handler.active:
            return
        create_tables(connection)
    elif command == 'c':
        if tournament_handler.active:
            return
        clear_tables(connection)
    elif command == 'p':
        if len(line) < 4:
            return
        if not add_player(connection, line[1], line[2], line[3]):
            print(f"{','.join(line)} Input Invalid")
    elif command == 'g':
        if tournament_handler.active:
            tournament_handler.add_tournament_game(line)
            return

        if len(line) < 5:
            return

        if len(line) >= 9:
            if not add_game(
                    connection,
                    line[1], line[2], line[3], line[4],
                    line[5], line[6], line[7], line[8]
            ):
                print(f"{','.join(line)} Input Invalid")
        else:
            if not add_game(connection, line[1], line[2], line[3], line[4]):
                print(f"{','.join(line)} Input Invalid")
    elif command == 'r':
        if len(line) < 9:
            return
        if not enter_game_result(
                connection,
                line[1], line[2], line[3], line[4],
                line[5], line[6], line[7], line[8]
        ):
            print(f"{','.join(line)} Input Invalid")
    elif command == 'P':
        if len(line) < 2:
            return
        get_player_info(connection, line[1])
    elif command == 'T':
        if len(line) < 2:
            return
        list_tournament(connection, line[1])
    elif command == 'H':
        if len(line) < 3:
            return
        list_head_to_head(connection, line[1], line[2])
    elif command == 'D':
        if len(line) < 3:
            return
        rank_players_by_performance(connection, line[1], line[2])


def main():
    connection = connect()
    if not connection:
        return

    tournament_handler = TournamentHandler(connection)
    filename = input("Enter the name of the file: ")

    try:
        with open(filename, 'r') as file:
            for line in csv.reader(file):
                if not line or not line[0]:
                    continue

                command = line[0]

                if command == 't':
                    tournament_handler.start_tournament(line)
                else:
                    handle_command(connection, tournament_handler, line)
    except Exception as e:
        print(f"Error processing file: {e}")
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    main()