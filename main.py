import database_connection as database
import commands as cmd
import queries as queries

def main():
    connection = database.connect_to_db()
    if connection is None:
        return
    #read csv file
    #check the first letter to determine which command to run
    #if p, run add_player
    #if g, run add_game
    #if c, run clear_tables
    #if q, run query
    #if etc

    filename = input("Enter the name of the file: ")
    with open(filename, 'r') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue

            command = line[0] # p
            #empty array
            # p,00000001,Alice,1500.0
            data = line[2:] if len(line) > 2 else "" #00000001,Alice,1500.0

            data_array = data.split(",") #['00000001', 'Alice', '1500.0']


            if command == "e":
                cmd.create_tables(connection)
            if command == "c":
                cmd.clear_tables(connection)
            if command == "p":
                cmd.add_player(connection, data_array[0], data_array[1], data_array[2])
            if command == "g":
                cmd.clear_tables(connection)

if __name__ == "__main__":
    main()

