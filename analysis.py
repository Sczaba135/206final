import sqlite3
import pandas as pd

# Connect to the database
db_path = 'project_data.db'
conn = sqlite3.connect(db_path)

# Step 1: Calculate daily average injuries, deaths, and snow/precipitation indicators
query_daily = """
SELECT 
    dm.date AS date,
    AVG(cc.number_of_persons_injured) AS avg_injuries,
    AVG(cc.number_of_persons_killed) AS avg_deaths,
    MAX(CASE WHEN sd.snowfall_sum > 0.1 THEN 1 ELSE 0 END) AS snow_over_0_1,
    MAX(CASE WHEN ws.precipitation_sum > 0.1 THEN 1 ELSE 0 END) AS precipitation_over_0_1,
    ws.precipitation_sum AS total_precipitation,
    sd.snowfall_sum AS total_snowfall
FROM car_crash cc
LEFT JOIN day_mapping dm ON cc.date_id = dm.date_id
LEFT JOIN weather_summary ws ON dm.date = ws.date
LEFT JOIN snowfall_data sd ON ws.weather_id = sd.weather_id
GROUP BY dm.date;
"""

# Step 2: Calculate injuries and deaths per accident for different conditions
query_conditions = """
SELECT 
    CASE 
        WHEN sd.snowfall_sum > 0.1 AND ws.precipitation_sum > 0.1 THEN 'Both'
        WHEN sd.snowfall_sum > 0.1 THEN 'Snow'
        WHEN ws.precipitation_sum > 0.1 THEN 'Precipitation'
        ELSE 'Neither'
    END AS day_condition,
    AVG(cc.number_of_persons_injured) AS avg_injuries_per_accident,
    AVG(cc.number_of_persons_killed) AS avg_deaths_per_accident
FROM car_crash cc
LEFT JOIN day_mapping dm ON cc.date_id = dm.date_id
LEFT JOIN weather_summary ws ON dm.date = ws.date
LEFT JOIN snowfall_data sd ON ws.weather_id = sd.weather_id
GROUP BY day_condition;
"""

# Step 3: Additional optional categories
query_totals = """
SELECT 
    COUNT(*) AS total_accidents,
    SUM(cc.number_of_persons_injured) AS total_injuries,
    SUM(cc.number_of_persons_killed) AS total_deaths
FROM car_crash cc;
"""

# Execute queries
daily_results = pd.read_sql_query(query_daily, conn)
conditions_results = pd.read_sql_query(query_conditions, conn)
totals_results = pd.read_sql_query(query_totals, conn)

# Write the results to a text file
output_file = 'accident_analysis_results.txt'
with open(output_file, 'w') as f:
    # Daily Averages
    f.write("Daily Averages and Snow/Precipitation Indicators:\n")
    f.write(daily_results.to_string(index=False))
    f.write("\n\n")

    # Injuries/Deaths Per Condition
    f.write("Average Injuries and Deaths Per Accident by Condition:\n")
    f.write(conditions_results.to_string(index=False))
    f.write("\n\n")

    # Totals
    f.write("Total Accidents, Injuries, and Deaths:\n")
    f.write(totals_results.to_string(index=False))

# Close the database connection
conn.close()