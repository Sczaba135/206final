#!/usr/bin/env python


# Make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
import os 
from sqlalchemy import create_engine, text
from sodapy import Socrata
import re

# Replace these placeholders with your actual credentials
app_token = "411bmgc3xjidmqazoprlv2052"  # Your App Token from Socrata
stan_app_token = "LZo8cDKk5efEwxM8ZhiHQGICB"
stan_api_key_id = "bxj7pkxp01zw8b4akgfxwvm84"
stan_api_key_secret = "4pkw8vup63igtmsypsq7cbd8c8kqkoiz5mfek90ptopt46nx7g"
secret_token = "3slx59h2gbk1ysoboklzqx81wkuo7wsylpb0wuyxihr1cf22fk"  # Optional: If required by the dataset

# Authenticated client for accessing the API
client = Socrata("data.cityofnewyork.us", stan_app_token, username="sczaba@umich.edu", password="$kynT*Gy4CVgaF5")

# SQLite connection string for the weather database
weather_db_filename = "project_data.db"
weather_db_path = os.path.join(os.getcwd(), weather_db_filename)
weather_db_url = f"sqlite:///{weather_db_path}"
weather_engine = create_engine(weather_db_url)

# SQLite connection string for the car crash data database
car_crash_db_filename = "car_crash_data.db"
car_crash_db_path = os.path.join(os.getcwd(), car_crash_db_filename)
car_crash_db_url = f"sqlite:///{car_crash_db_path}"
car_crash_engine = create_engine(car_crash_db_url)

database_filename = "project_data.db"
database_path = os.path.join(os.getcwd(), database_filename)
database_url = f"sqlite:///{database_path}"
engine = create_engine(database_url)

# Function to transform date format
def transform_date(date_str):
    # Match and capture groups for the date only from the crash date
    date_match = re.match(r"(\d{4}-\d{2}-\d{2})T\d{2}:\d{2}:\d{2}\.\d{3}", date_str)
    return date_match.group(1) if date_match else date_str  # Return the match if found

# Fetch unique dates from the weather table
with weather_engine.connect() as conn:
    weather_dates_query = text("SELECT DISTINCT date FROM weather")
    weather_dates = conn.execute(weather_dates_query).fetchall()
    weather_dates = [row[0] for row in weather_dates]  # Access by index to get the date column only

# Loop through each date and fetch corresponding crash data
for date in weather_dates:
    date_str = date  # date is already in 'YYYY-MM-DD' format
    
    # Fetch 20 matches from the API for the current date
    results = client.get("h9gi-nx95", select="crash_date,number_of_persons_injured,number_of_persons_killed,collision_id",
                         where=f"crash_date >= '{date_str}T00:00:00.000' AND crash_date < '{date_str}T23:59:59.999'", limit=20)
    
    # Convert results to a DataFrame
    results_df = pd.DataFrame.from_records(results)
    
    # Apply the date transformation to the crash_date column
    if not results_df.empty:
        results_df['crash_date'] = results_df['crash_date'].apply(transform_date)
    
        # Write the DataFrame to the car crash data database
        results_df.to_sql('car_crash', con=engine, if_exists='append', index=False)
