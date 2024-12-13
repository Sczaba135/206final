import matplotlib.pyplot as plt
import pandas as pd

# File path
file_path = 'accident_analysis_results.txt'

# Parse the data from the text file
with open(file_path, 'r') as file:
    content = file.read()

# Split the content into sections
daily_data, conditions_data, totals_data = content.split("\n\n")

# Parse daily data
daily_lines = daily_data.split("\n")[2:]  # Skip header lines
daily_values = [line.split() for line in daily_lines if line.strip()]
daily_df = pd.DataFrame(daily_values, columns=[
    "date", "avg_injuries", "avg_deaths", "snow_over_0_1",
    "precipitation_over_0_1", "total_precipitation", "total_snowfall"
])

# Parse conditions data
conditions_lines = conditions_data.split("\n")[2:]  # Skip header lines
conditions_values = [line.split() for line in conditions_lines if line.strip()]
conditions_df = pd.DataFrame(conditions_values, columns=[
    "day_condition", "avg_injuries_per_accident", "avg_deaths_per_accident"
])

# Parse totals data
totals_lines = totals_data.split("\n")[2:]  # Skip header lines
totals_values = [line.split() for line in totals_lines if line.strip()]
totals_df = pd.DataFrame(totals_values, columns=[
    "total_accidents", "total_injuries", "total_deaths"
])

# Convert columns to appropriate data types
daily_df[["avg_injuries", "avg_deaths", "total_precipitation", "total_snowfall"]] = daily_df[
    ["avg_injuries", "avg_deaths", "total_precipitation", "total_snowfall"]
].astype(float)
daily_df[["snow_over_0_1", "precipitation_over_0_1"]] = daily_df[
    ["snow_over_0_1", "precipitation_over_0_1"]
].astype(int)

conditions_df[["avg_injuries_per_accident", "avg_deaths_per_accident"]] = conditions_df[
    ["avg_injuries_per_accident", "avg_deaths_per_accident"]
].astype(float)

totals_df = totals_df.astype(int)

# Create bar charts for injuries and deaths by condition
plt.figure(figsize=(10, 6))
plt.bar(conditions_df["day_condition"], conditions_df["avg_injuries_per_accident"], alpha=0.7, label="Average Injuries")
plt.title("Average Injuries Per Accident by Condition")
plt.ylabel("Average Injuries")
plt.xlabel("Condition")
plt.legend()
plt.savefig("average_injuries_by_condition.png")
plt.close()

plt.figure(figsize=(10, 6))
plt.bar(conditions_df["day_condition"], conditions_df["avg_deaths_per_accident"], alpha=0.7, color="red", label="Average Deaths")
plt.title("Average Deaths Per Accident by Condition")
plt.ylabel("Average Deaths")
plt.xlabel("Condition")
plt.legend()
plt.savefig("average_deaths_by_condition.png")
plt.close()

# Create scatter plot for daily injuries and precipitation
plt.figure(figsize=(10, 6))
plt.scatter(daily_df["total_precipitation"], daily_df["avg_injuries"], alpha=0.5, label="Average Injuries")
plt.title("Daily Injuries vs. Total Precipitation")
plt.xlabel("Total Precipitation (inches)")
plt.ylabel("Average Injuries")
plt.legend()
plt.savefig("daily_injuries_vs_precipitation.png")
plt.close()

# Create scatter plot for daily injuries and snowfall
plt.figure(figsize=(10, 6))
plt.scatter(daily_df["total_snowfall"], daily_df["avg_injuries"], alpha=0.5, label="Average Injuries", color="blue")
plt.title("Daily Injuries vs. Total Snowfall")
plt.xlabel("Total Snowfall (inches)")
plt.ylabel("Average Injuries")
plt.legend()
plt.savefig("daily_injuries_vs_snowfall.png")
plt.close()
