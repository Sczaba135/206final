import pandas as pd
import os
from sqlalchemy import create_engine, text
from sodapy import Socrata
import re


# Replace these placeholders with your actual credentials
app_token = "411bmgc3xjidmqazoprlv2052"
stan_app_token = "LZo8cDKk5efEwxM8ZhiHQGICB"
stan_api_key_id = "bxj7pkxp01zw8b4akgfxwvm84"
stan_api_key_secret = "4pkw8vup63igtmsypsq7cbd8c8kqkoiz5mfek90ptopt46nx7g"
secret_token = "3slx59h2gbk1ysoboklzqx81wkuo7wsylpb0wuyxihr1cf22fk"

# Authenticated client for accessing the API
client = Socrata("data.cityofnewyork.us", stan_app_token, username="sczaba@umich.edu", password="$kynT*Gy4CVgaF5")

# SQLite connection strings
weather_db_filename = "project_data.db"
weather_db_path = os.path.join(os.getcwd(), weather_db_filename)
weather_db_url = f"sqlite:///{weather_db_path}"
weather_engine = create_engine(weather_db_url)

car_crash_db_filename = "car_crash_data.db"
car_crash_db_path = os.path.join(os.getcwd(), car_crash_db_filename)
car_crash_db_url = f"sqlite:///{car_crash_db_path}"
car_crash_engine = create_engine(car_crash_db_url)

database_filename = "project_data.db"
database_path = os.path.join(os.getcwd(), database_filename)
database_url = f"sqlite:///{database_path}"
engine = create_engine(database_url)

with engine.connect() as conn:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS car_crash (
        crash_date TEXT,
        number_of_persons_injured INTEGER,
        number_of_persons_killed INTEGER,
        collision_id TEXT PRIMARY KEY
    )
    """))

# Function to transform date format
def transform_date(date_str):
    date_match = re.match(r"(\d{4}-\d{2}-\d{2})T\d{2}:\d{2}:\d{2}\.\d{3}", date_str)
    return date_match.group(1) if date_match else date_str

# Fetch unique dates from the weather table
with engine.connect() as conn:
    weather_dates_query = text("SELECT DISTINCT date FROM weather_summary")
    weather_dates = conn.execute(weather_dates_query).fetchall()
    weather_dates = [row[0] for row in weather_dates]



# Process each date
for date in weather_dates:
    with engine.connect() as conn:
        # Check the number of entries already in the car_crash table for the date
        count_query = text("SELECT COUNT(*) FROM car_crash WHERE crash_date = :date")
        count_result = conn.execute(count_query, {"date": date}).scalar()

        # Skip dates that already have 20 or more entries
        if count_result >= 20:
            continue

        # Fetch the remaining number of entries needed
        limit = 20 - count_result

        # Fetch crash data from the API for the current date
        results = client.get("h9gi-nx95", 
                             select="crash_date,number_of_persons_injured,number_of_persons_killed,collision_id",
                             where=f"crash_date >= '{date}T00:00:00.000' AND crash_date < '{date}T23:59:59.999'",
                             limit=limit)

        # Convert results to a DataFrame
        results_df = pd.DataFrame.from_records(results)

        if not results_df.empty:
            # Apply the date transformation to the crash_date column
            results_df['crash_date'] = results_df['crash_date'].apply(transform_date)

            # Write the DataFrame to the car_crash table
            results_df.to_sql('car_crash', con=engine, if_exists='append', index=False)
            conn.close()

            break
