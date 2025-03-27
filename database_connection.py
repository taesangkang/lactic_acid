import mysql.connector

def connect_to_db():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="cs5330",
            password="pw5330",
            database="dbprog"
        )
        return connection
    except mysql.connector.Error as err:
        if err.errno == 1049:
            connection = mysql.connector.connect(
                host="localhost",
                user="cs5330",
                password="pw5330"
            )
            cursor = connection.cursor()
            cursor.execute("CREATE DATABASE dbprog")
            connection.close()
            return mysql.connector.connect(
                host="localhost",
                user="cs5330",
                password="pw5330",
                database="dbprog"
            )
        else:
            print(f"Database connection error: {err}")
            exit(1)