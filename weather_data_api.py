import openmeteo_requests
import requests_cache
import pandas as pd
import sqlite3
from retry_requests import retry

# Setup Open-Meteo API client
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

def fetch_weather_data_for_months():
    """
    Fetch weather data for 2023 starting from January 1st.
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 40.7143,  # Example: New York
        "longitude": -74.006,
        "start_date": "2022-01-01",
        "end_date": "2022-12-31",  # Full year for flexibility
        "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "snowfall_sum"],
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
        "timezone": "America/New_York"
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # Process daily data
    daily = response.Daily()
    daily_temperature_max = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_min = daily.Variables(1).ValuesAsNumpy()
    daily_precipitation = daily.Variables(2).ValuesAsNumpy()
    daily_snowfall = daily.Variables(3).ValuesAsNumpy()

    # Create date range
    date_range = pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq="D"
    )

    # Ensure all arrays are the same length
    min_length = min(len(date_range), len(daily_temperature_max), len(daily_temperature_min), len(daily_precipitation), len(daily_snowfall))
    date_range = date_range[:min_length]
    daily_data = {
        "date": date_range,
        "temperature_max": daily_temperature_max[:min_length],
        "temperature_min": daily_temperature_min[:min_length],
        "precipitation_sum": daily_precipitation[:min_length],
        "snowfall_sum": daily_snowfall[:min_length]
    }

    # Create DataFrame for the year
    return pd.DataFrame(daily_data)

def setup_database():
    """
    Setup the database with two tables: weather_summary and snowfall_data.
    """
    conn = sqlite3.connect('project_data.db')
    cur = conn.cursor()

    # Create the weather_summary table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS weather_summary (
            weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE,
            temperature_max REAL,
            temperature_min REAL,
            precipitation_sum REAL
        )
    ''')

    # Create the snowfall_data table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS snowfall_data (
            weather_id INTEGER PRIMARY KEY,
            snowfall_sum REAL,
            FOREIGN KEY (weather_id) REFERENCES weather_summary (weather_id)
        )
    ''')

    conn.commit()
    conn.close()

def insert_weather_data(dataframe, limit=25):
    """
    Insert weather data into the SQLite database, appending `limit` rows at a time.
    """
    conn = sqlite3.connect('project_data.db')
    try:
        cur = conn.cursor()

        # Check for the latest date in the weather_summary table
        cur.execute('SELECT MAX(date) FROM weather_summary')
        last_date = cur.fetchone()[0]

        if last_date:
            # Convert last_date to datetime and make both timezone-naive
            last_date = pd.to_datetime(last_date).tz_localize(None)
            dataframe['date'] = dataframe['date'].dt.tz_localize(None)

            # Filter for new data only
            dataframe = dataframe[dataframe['date'] > last_date]
        else:
            dataframe['date'] = dataframe['date'].dt.tz_localize(None)

        # Limit the number of rows to insert
        new_data = dataframe.head(limit)

        if new_data.empty:
            print("No new data to insert.")
            return

        for _, row in new_data.iterrows():
            try:
                # Insert into weather_summary table
                cur.execute('''
                    INSERT INTO weather_summary (date, temperature_max, temperature_min, precipitation_sum)
                    VALUES (?, ?, ?, ?)
                ''', (row['date'].strftime('%Y-%m-%d'), row['temperature_max'], row['temperature_min'], row['precipitation_sum']))

                # Get the weather_id of the inserted row
                weather_id = cur.lastrowid

                # Insert into snowfall_data table only if snowfall exists
                if row['snowfall_sum'] >= 0:
                    cur.execute('''
                        INSERT INTO snowfall_data (weather_id, snowfall_sum)
                        VALUES (?, ?)
                    ''', (weather_id, row['snowfall_sum']))
            except sqlite3.IntegrityError:
                print(f"Skipping duplicate date: {row['date'].strftime('%Y-%m-%d')}")

        conn.commit()
        print(f"Inserted {len(new_data)} new rows into the database.")
    finally:
        conn.close()



if __name__ == "__main__":
    setup_database()  # Ensure the database and tables are set up
    weather_df = fetch_weather_data_for_months()
    if weather_df.empty:
        print("No weather data available. Check API response.")
    else:
        print(weather_df.head())  # Preview data
        insert_weather_data(weather_df)
