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