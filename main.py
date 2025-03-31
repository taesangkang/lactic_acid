import database_connection as database
import commands as cmd
import queries as queries
import csv


def main():
    connection = database.connect_to_db()
    if connection is None:
        return

    filename = input("Enter the name of the file: ")

    tournament_mode = False
    tournament_name = None
    games_remaining = 0

    try:
        with open(filename, 'r') as file:
            csvreader = csv.reader(file)

            for row in csvreader:
                if not row or not row[0]:
                    continue

                command = row[0]

                if tournament_mode:
                    if command == 'g':
                        if len(row) >= 9:
                            success = cmd.add_game(
                                connection,
                                row[1], row[2], row[3], row[4],
                                int(row[5]), int(row[6]), float(row[7]), float(row[8]),
                                tournament_name
                            )
                        else:
                            success = cmd.add_game(
                                connection,
                                row[1], row[2], row[3], row[4],
                                tournament = tournament_name
                            )

                        if not success:
                            print(f"{','.join(row)} Input Invalid")

                        games_remaining -= 1
                        if games_remaining == 0:
                            tournament_mode = False

                    elif command in ['t', 'e', 'c']:
                        pass
                    else:
                        handle_command(connection, row)
                else:
                    if command == 't':
                        tournament_name = row[1]
                        organizer_id = row[2]
                        games_remaining = int(row[3])

                        success = cmd.add_tournament(connection, tournament_name, organizer_id)
                        if success:
                            tournament_mode = True
                        else:
                            print(f"{','.join(row)} Input Invalid")
                    else:
                        handle_command(connection, row)

    except Exception as e:
        print(f"Error processing file: {e}")

    finally:
        if connection:
            connection.close()


def handle_command(connection, row):
    command = row[0]

    if command == 'e':
        cmd.create_tables(connection)

    elif command == 'c':
        cmd.clear_tables(connection)

    elif command == 'p':
        success = cmd.add_player(connection, row[1], row[2], row[3])
        if not success:
            print(f"{','.join(row)} Input Invalid")

    elif command == 'g':
        if len(row) >= 9:
            success = cmd.add_game(
                connection,
                row[1], row[2], row[3], row[4],
                int(row[5]), int(row[6]), float(row[7]), float(row[8])
            )
        else:
            success = cmd.add_game(connection, row[1], row[2], row[3], row[4])

        if not success:
            print(f"{','.join(row)} Input Invalid")

    elif command == 'r':
        success = cmd.enter_game_result(
            connection,
            row[1], row[2], row[3], row[4],
            int(row[5]), int(row[6]), float(row[7]), float(row[8])
        )
        if not success:
            print(f"{','.join(row)} Input Invalid")

    elif command == 'P':
        queries.get_player_info(connection, row[1])

    elif command == 'T':
        queries.list_tournament(connection, row[1])

    elif command == 'H':
        queries.list_head_to_head(connection, row[1], row[2])

    elif command == 'D':
        queries.rank_players_by_performance(connection, row[1], row[2])


if __name__ == "__main__":
    main()