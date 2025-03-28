import mysql.connector

def connect_to_db():
    try:
        return mysql.connector.connect(
            host="localhost",
            user="cs5330",
            password="pw5330",
            database="dbprog"
        )
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None