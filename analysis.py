import sqlite3
import pandas as pd

# Reconnect to the newly uploaded database file
db_path = 'project_data.db'
conn = sqlite3.connect(db_path)

# Updated Queries: Use weather_id from weather_summary to correlate car_crash and snowfall_data
# Step 1: Calculate daily average injuries and deaths, and whether snow or precipitation > 0.1
query_daily_updated = """
SELECT 
    car_crash.crash_date AS date,
    AVG(car_crash.number_of_persons_injured) AS avg_injuries,
    AVG(car_crash.number_of_persons_killed) AS avg_deaths,
    MAX(CASE WHEN snowfall_data.snowfall_sum > 0.1 THEN 1 ELSE 0 END) AS snow_over_0_1,
    MAX(CASE WHEN weather_summary.precipitation_sum > 0.1 THEN 1 ELSE 0 END) AS precipitation_over_0_1
FROM car_crash
LEFT JOIN weather_summary ON car_crash.crash_date = weather_summary.date
LEFT JOIN snowfall_data ON weather_summary.weather_id = snowfall_data.weather_id
GROUP BY car_crash.crash_date;
"""

# Step 2: Calculate average injuries and deaths per accident for snowy days, precipitation days, and days where neither occurred
query_conditions_updated = """
SELECT 
    CASE 
        WHEN snowfall_data.snowfall_sum > 0.1 AND weather_summary.precipitation_sum > 0.1 THEN 'Both'
        WHEN snowfall_data.snowfall_sum > 0.1 THEN 'Snow'
        WHEN weather_summary.precipitation_sum > 0.1 THEN 'Precipitation'
        ELSE 'Neither'
    END AS day_condition,
    AVG(car_crash.number_of_persons_injured) AS avg_injuries_per_accident,
    AVG(car_crash.number_of_persons_killed) AS avg_deaths_per_accident
FROM car_crash
LEFT JOIN weather_summary ON car_crash.crash_date = weather_summary.date
LEFT JOIN snowfall_data ON weather_summary.weather_id = snowfall_data.weather_id
GROUP BY day_condition;
"""

# Execute the updated queries and fetch the results
daily_results_updated = pd.read_sql_query(query_daily_updated, conn)
conditions_results_updated = pd.read_sql_query(query_conditions_updated, conn)

# Write the updated results to a new file
updated_script_output_path = 'accident_analysis_results.txt'
with open(updated_script_output_path, 'w') as f:
    f.write("Daily Averages and Snow/Precipitation Indicators (Updated):\n")
    f.write(daily_results_updated.to_string(index=False))
    f.write("\n\n")
    f.write("Average Injuries and Deaths Per Accident by Condition (Updated):\n")
    f.write(conditions_results_updated.to_string(index=False))

# Close the database connection
conn.close()

# Provide the updated script file path
updated_script_output_path