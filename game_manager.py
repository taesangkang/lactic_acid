class GameManager:
    def __init__(self, connection):
        self.connection = connection

    def add_game(self, date, time, acidic, alkaline, ac_score=None, ak_score=None,
                 ac_rating=None, ak_rating=None, tournament=None):
        cursor = self.connection.cursor()
        try:
            if not self._player_exists(acidic) or not self._player_exists(alkaline):
                return False

            if not self._are_scores_valid(ac_score, ak_score):
                return False

            datetime_str = self._format_datetime(date, time)

            if ac_score is not None and ak_score is not None:
                if self._has_earlier_unfinished_games(datetime_str, acidic, alkaline):
                    return False

            cursor.execute(
                "INSERT INTO Game VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (datetime_str, acidic, alkaline, ac_score, ak_score, ac_rating, ak_rating, tournament)
            )
            if self._is_game_complete(ac_score, ak_score, ac_rating, ak_rating):
                self._update_player_ratings(acidic, alkaline, ac_rating, ak_rating)

            self.connection.commit()
            return True
        except Exception:
            self.connection.rollback()
            return False
        finally:
            cursor.close()

    def enter_game_result(self, date, time, acidic, alkaline, ac_score, ak_score, ac_rating, ak_rating):
        datetime_str = self._format_datetime(date, time)

        try:
            if self._has_earlier_unfinished_games(datetime_str, acidic, alkaline):
                return False

            cursor = self.connection.cursor()
            cursor.execute(
                "UPDATE Game SET AcScore = %s, AkScore = %s, AcRating = %s, AkRating = %s "
                "WHERE Time = %s AND Acidic = %s AND Alkaline = %s",
                (ac_score, ak_score, ac_rating, ak_rating, datetime_str, acidic, alkaline)
            )

            self._update_player_ratings(acidic, alkaline, ac_rating, ak_rating)

            self.connection.commit()
            cursor.close()
            return True
        except Exception:
            self.connection.rollback()
            return False

    def get_game_history(self, player1_id=None, player2_id=None, tournament=None,
                         start_date=None, end_date=None, limit=None):
        cursor = self.connection.cursor()
        try:
            query = """
                SELECT 
                    DATE_FORMAT(g.Time, '%Y/%m/%d'),
                    DATE_FORMAT(g.Time, '%H:%i'),
                    p1.Name, g.Acidic,
                    p2.Name, g.Alkaline,
                    g.AcScore, g.AkScore,
                    g.Tournament
                FROM Game g
                JOIN Player p1 ON g.Acidic = p1.ID
                JOIN Player p2 ON g.Alkaline = p2.ID
                WHERE 1=1
            """
            params = []

            # Apply filters
            if player1_id and player2_id:
                query += " AND ((g.Acidic = %s AND g.Alkaline = %s) OR (g.Acidic = %s AND g.Alkaline = %s))"
                params.extend([player1_id, player2_id, player2_id, player1_id])
            elif player1_id:
                query += " AND (g.Acidic = %s OR g.Alkaline = %s)"
                params.extend([player1_id, player1_id])

            if tournament:
                query += " AND g.Tournament = %s"
                params.append(tournament)

            if start_date:
                start_datetime = self._format_date_for_query(start_date, "00:00:00")
                query += " AND g.Time >= %s"
                params.append(start_datetime)

            if end_date:
                end_datetime = self._format_date_for_query(end_date, "23:59:59")
                query += " AND g.Time <= %s"
                params.append(end_datetime)

            query += " ORDER BY g.Time DESC"

            if limit:
                query += " LIMIT %s"
                params.append(limit)

            cursor.execute(query, params)
            games = cursor.fetchall()
            return games
        finally:
            cursor.close()

    def _player_exists(self, player_id):
        cursor = self.connection.cursor()
        cursor.execute("SELECT 1 FROM Player WHERE ID = %s", (player_id,))
        result = cursor.fetchone() is not None
        cursor.close()
        return result

    def _are_scores_valid(self, ac_score, ak_score):
        if ac_score is not None and (ac_score < 0 or ac_score > 10):
            return False
        if ak_score is not None and (ak_score < 0 or ak_score > 10):
            return False
        return True

    def _format_datetime(self, date, time):
        return f"{date[:4]}-{date[4:6]}-{date[6:]} {time[:2]}:{time[2:]}:00"

    def _format_date_for_query(self, date, time_str):
        return f"{date[:4]}-{date[4:6]}-{date[6:]} {time_str}"

    def _has_earlier_unfinished_games(self, datetime_str, acidic, alkaline):
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM Game 
            WHERE Time < %s AND (Acidic = %s OR Acidic = %s OR Alkaline = %s OR Alkaline = %s)
            AND (AcScore IS NULL OR AkScore IS NULL)
        """, (datetime_str, acidic, alkaline, acidic, alkaline))
        result = cursor.fetchone()[0] > 0
        cursor.close()
        return result

    def _is_game_complete(self, ac_score, ak_score, ac_rating, ak_rating):
        return (ac_score is not None and ak_score is not None and
                ac_rating is not None and ak_rating is not None)

    def _update_player_ratings(self, acidic, alkaline, ac_rating, ak_rating):
        cursor = self.connection.cursor()
        cursor.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (ac_rating, acidic))
        cursor.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (ak_rating, alkaline))
        cursor.close()