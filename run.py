from datetime import datetime
from config import *
from databases import *
from main import *


def main():
    start = datetime.now()
    formatted_start = start.strftime("%Y-%m-%d %H:%M:%S")
    
    output(f"{PROJECT_NAME} v{VERSION_NUMBER}")
    output(f"Started: {formatted_start}")
    output("Initailizing databases...")

    for database, items in DATABASES.items():
        if not check_database_exists(database):
            create_database(database)

        with sqlite3.connect(database) as connection:
            for table, query in items["tables"].items():
                if not check_table_exists(connection, database, table):
                    create_table(connection, database, table, query)

    end_time = datetime.now()
    runtime = end_time - start
    formatted_end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")

    output(f"Exited: {formatted_end_time}")
    output(f"Runtime: {runtime}")


if __name__ == "__main__":
    main()
