# In main.py
import database_connection as db
from game_manager import GameManager
from tournament_handler import TournamentHandler
from command_handlers import handle_command, process_command
import csv

def main():
    connection = db.connect_to_db()
    if not connection:
        return

    game_manager = GameManager(connection)
    tournament_handler = TournamentHandler(connection, game_manager)
    filename = input("Enter the name of the file: ")

    try:
        with open(filename, 'r') as file:
            for line in csv.reader(file):
                if line and line[0]:
                    process_command(line, connection, game_manager, tournament_handler)
    except Exception as e:
        print(f"Error processing file: {e}")
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    main()