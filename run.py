from datetime import datetime
from config import *
from main import *

def main():
    start = datetime.now()
    formatted_start = start.strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n{PROJECT_NAME} v{VERSION_NUMBER} - {formatted_start}")
    logging.info(f"{PROJECT_NAME} v{VERSION_NUMBER} - {formatted_start}")
    
    initialize(DATABASES)  # Creates databases & tables or verifies their existence
    update(DATABASES)  # Updates database tables or verifies they are up to date

    end_time = datetime.now() 
    runtime = end_time - start
    formatted_end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")

    print(f"\nProgram completed - {formatted_end_time}")
    print(f"\nRuntime: {runtime}\n")
    logging.info(f"Program completed - {formatted_end_time}")
    logging.info(f"Runtime: {runtime}")

if __name__ == "__main__":
    main()
