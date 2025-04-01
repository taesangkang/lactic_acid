import commands as cmd
import queries as q


def handle_command(connection, game_manager, line):
    cmd_map = {
        'e': lambda: cmd.create_tables(connection),
        'c': lambda: cmd.clear_tables(connection),
        'p': lambda: handle_add_player(connection, line),
        'g': lambda: handle_add_game(game_manager, line),
        'r': lambda: handle_enter_result(game_manager, line),
        'P': lambda: q.get_player_info(connection, line[1]),
        'T': lambda: q.list_tournament(connection, line[1]),
        'H': lambda: q.list_head_to_head(connection, line[1], line[2]),
        'D': lambda: q.rank_players_by_performance(connection, line[1], line[2])
    }

    if line[0] in cmd_map:
        cmd_map[line[0]]()


def handle_add_player(connection, line):
    if not cmd.add_player(connection, line[1], line[2], line[3]):
        print(f"{','.join(line)} Input Invalid")


def handle_add_game(game_manager, line):
    success = False
    if len(line) >= 9:
        success = game_manager.add_game(
            line[1], line[2], line[3], line[4],
            int(line[5]), int(line[6]), float(line[7]), float(line[8])
        )
    else:
        success = game_manager.add_game(line[1], line[2], line[3], line[4])

    if not success:
        print(f"{','.join(line)} Input Invalid")


def handle_enter_result(game_manager, line):
    if not game_manager.enter_game_result(
            line[1], line[2], line[3], line[4],
            int(line[5]), int(line[6]), float(line[7]), float(line[8])
    ):
        print(f"{','.join(line)} Input Invalid")


def process_command(line, connection, game_manager, tournament_handler):
    command = line[0]

    if tournament_handler.active and command == 'g':
        if not tournament_handler.add_tournament_game(line):
            print(f"{','.join(line)} Input Invalid")
    elif command == 't':
        if not tournament_handler.start_tournament(line):
            print(f"{','.join(line)} Input Invalid")
    elif not (tournament_handler.active and command in ['e', 'c']):
        handle_command(connection, game_manager, line)
