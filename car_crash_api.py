import pandas as pd
import os
import sqlite3
from sqlalchemy import text
from sodapy import Socrata


# Replace these placeholders with your actual credentials
app_token = "411bmgc3xjidmqazoprlv2052"
stan_app_token = "LZo8cDKk5efEwxM8ZhiHQGICB"
stan_api_key_id = "bxj7pkxp01zw8b4akgfxwvm84"
stan_api_key_secret = "4pkw8vup63igtmsypsq7cbd8c8kqkoiz5mfek90ptopt46nx7g"
secret_token = "3slx59h2gbk1ysoboklzqx81wkuo7wsylpb0wuyxihr1cf22fk"


# Authenticated client for accessing the API
client = Socrata("data.cityofnewyork.us", stan_app_token, username="sczaba@umich.edu", password="$kynT*Gy4CVgaF5")


# SQLite database
database_filename = "project_data.db"
database_path = os.path.join(os.getcwd(), database_filename)


# Function to create necessary tables
def setup_database():
   conn = sqlite3.connect(database_path)
   cur = conn.cursor()


   # Create the car_crash table
   cur.execute('''
       CREATE TABLE IF NOT EXISTS car_crash (
           date_id INTEGER,
           number_of_persons_injured INTEGER,
           number_of_persons_killed INTEGER,
           collision_id TEXT PRIMARY KEY
       )
   ''')


   # Create the day_mapping table
   cur.execute('''
       CREATE TABLE IF NOT EXISTS day_mapping (
           date_id INTEGER PRIMARY KEY,
           date TEXT UNIQUE
       )
   ''')


   conn.commit()
   conn.close()


# Function to fetch the next available `date_id`
def get_next_date_id():
   conn = sqlite3.connect(database_path)
   cur = conn.cursor()
   cur.execute("SELECT MAX(date_id) FROM day_mapping")
   result = cur.fetchone()[0]
   conn.close()
   return (result or 0) + 1


# Function to fetch crash data and insert into the database
def fetch_and_insert_car_crash_data():
   conn = sqlite3.connect(database_path)
   cur = conn.cursor()


   # Fetch unique dates from the weather_summary table
   cur.execute("SELECT DISTINCT date FROM weather_summary ORDER BY date")
   weather_dates = [row[0] for row in cur.fetchall()]


   rows_added = 0  # Track the number of rows added this run


   for date in weather_dates:
       next_date_id = get_next_date_id()


       # Check if the date is already in day_mapping
       cur.execute("SELECT COUNT(*) FROM day_mapping WHERE date = ?", (date,))
       if cur.fetchone()[0] > 0:
           continue  # Skip already processed dates


       # Insert date into day_mapping table
       cur.execute("INSERT INTO day_mapping (date_id, date) VALUES (?, ?)", (next_date_id, date))
       conn.commit()


       # Fetch crash data for this date
       results = client.get(
           "h9gi-nx95",
           select="crash_date,number_of_persons_injured,number_of_persons_killed,collision_id",
           where=f"crash_date >= '{date}T00:00:00.000' AND crash_date < '{date}T23:59:59.999'",
           limit=20
       )


       results_df = pd.DataFrame.from_records(results)


       if not results_df.empty:
           results_df['date_id'] = next_date_id
           results_df = results_df.rename(columns={'crash_date': 'date'})


           # Write data to car_crash table row by row
           for _, row in results_df.iterrows():
               try:
                   cur.execute('''
                       INSERT INTO car_crash (date_id, number_of_persons_injured, number_of_persons_killed, collision_id)
                       VALUES (?, ?, ?, ?)
                   ''', (row['date_id'], row['number_of_persons_injured'], row['number_of_persons_killed'], row['collision_id']))
                   rows_added += 1
               except sqlite3.IntegrityError:
                   print(f"Skipping duplicate collision_id: {row['collision_id']}")


               if rows_added >= 20:
                   break  # Stop after 20 rows have been added


       if rows_added >= 20:
           break  # Stop after 20 rows have been added


   conn.commit()
   conn.close()


   if rows_added == 0:
       print("No new rows added. Check if all available data has already been processed.")
   else:
       print(f"{rows_added} rows added to the database.")


# Main execution
if __name__ == "__main__":
   setup_database()
   fetch_and_insert_car_crash_data()
