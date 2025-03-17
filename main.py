import requests
import json
import sqlite3
from collections import namedtuple
from contextlib import closing

from prefect import flow, task

# Database name
DB_NAME = "political_party.db"

# Function to create table
@task
def create_table():
    with closing(sqlite3.connect(DB_NAME)) as conn:  # Makes a quick connection
        with closing(conn.cursor()) as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS political_party (
                    id INTEGER PRIMARY KEY, 
                    name TEXT, 
                    long_name TEXT, 
                    image_url TEXT
                )
            """)  # Executes the function; if the table exists, do not create, otherwise, do it
            conn.commit()

# Getting the data from my API (Mexican political parties)
@task
def get_political_party_data():
    url = "http://192.168.68.103:8080/api/political_party"
    r = requests.get(url)
    response_json = json.loads(r.text)  # Loads into JSON
    return response_json  # Returns the JSON

# Extract and transform the data
@task
def parse_political_party_data(raw):
    parties = []  # To store the parties
    PoliticalParty = namedtuple("PoliticalParty", ['id', 'name', 'long_name', 'image_url'])
    
    for row in raw:  # Iteration to get the info from the JSON
        party = PoliticalParty(
            id=row.get('id'),
            name=row.get('name'),
            long_name=row.get('long_name'),
            image_url=row.get('image_url'),
        )
        parties.append(party)  # Once finished, appends it to the parties, one party at a time
    
    return parties  # Return parties

# Load into SQLite
@task
def store_political_parties(parsed):
    insert_cmd = "INSERT INTO political_party (id, name, long_name, image_url) VALUES (?, ?, ?, ?)"  # To insert
    
    with closing(sqlite3.connect(DB_NAME)) as conn:  # Quick connection
        with closing(conn.cursor()) as cursor:
            cursor.executemany(insert_cmd, parsed)  # Executes with the parsed data
            conn.commit()  # Save changes

# Function to print table
def table(data):
    """PARTIES TABLE"""
    if not data:  # No data, no print
        print("No data available.")
        return

    # Convert all values to strings
    data = [[str(item) for item in row] for row in data]

    # Calculate column widths
    col_widths = [max(len(row[i]) for row in data) for i in range(len(data[0]))]

    # Print separator
    def print_separator():
        print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")

    # Print table
    print_separator()
    for i, row in enumerate(data):
        print("| " + " | ".join(row[j].ljust(col_widths[j]) for j in range(len(row))) + " |")
        if i == 0:  # Print a separator after the header
            print_separator()
    print_separator()

# Show the data in a table
@task
def show_political_parties(party_list):
    party_list.insert(0, ("ID", "Name", "Long Name", "Image URL"))  # Insert into the table
    table(party_list)

# Get the first five parties
@task
def first_five_political_parties():
    with closing(sqlite3.connect(DB_NAME)) as conn:  # Quick connection
        with closing(conn.cursor()) as cursor:
            cursor.execute("SELECT * FROM political_party LIMIT 5")  # Show first 5 parties
            return cursor.fetchall()

# Define the ETL flow
@flow
def political_party_etl_flow():
    create_table()  # Create table if it does not exist
    raw = get_political_party_data()  # Fetch data from the API
    parsed = parse_political_party_data(raw)  # Parse the data
    store_political_parties(parsed)  # Store the parsed data in the database
    parties = first_five_political_parties()  # Retrieve first 5 parties
    show_political_parties(parties)  # Show the data

# Run the flow
if __name__ == "__main__":
    political_party_etl_flow()
