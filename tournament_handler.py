class TournamentHandler:
    def __init__(self, connection, game_manager):
        self.connection = connection
        self.game_manager = game_manager
        self.active = False
        self.name = None
        self.games_remaining = 0

    def start_tournament(self, line):
        self.name = line[1]
        organizer_id = line[2]
        self.games_remaining = int(line[3])

        if self._add_tournament(self.name, organizer_id):
            self.active = True
            return True
        return False

    def add_tournament_game(self, line):
        if not self.active:
            return False

        if len(line) >= 9:
            success = self.game_manager.add_game(
                line[1], line[2], line[3], line[4],
                int(line[5]), int(line[6]), float(line[7]), float(line[8]),
                self.name
            )
        else:
            success = self.game_manager.add_game(
                line[1], line[2], line[3], line[4],
                tournament=self.name
            )
        if success:
            self.games_remaining -= 1
            if self.games_remaining <= 0:
                self.active = False

        return success

    def _add_tournament(self, name, organizer):
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                "INSERT INTO Tournament VALUES (%s, %s)",
                (name, organizer)
            )
            self.connection.commit()
            cursor.close()
            return True
        except:
            self.connection.rollback()
            return False