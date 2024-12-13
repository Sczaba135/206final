import matplotlib.pyplot as plt
import pandas as pd

file_path = 'accident_analysis_results.txt'

# Parse the data from the file
with open(file_path, 'r') as file:
    content = file.read()

# Separate the data for the two parts
daily_data, conditions_data = content.split("\n\nAverage Injuries and Deaths Per Accident by Condition (Updated):\n")

# Parse daily data
daily_lines = daily_data.split("\n")[2:]  # Skip the header lines
daily_values = [line.split() for line in daily_lines if line.strip()]
daily_df = pd.DataFrame(daily_values, columns=["date", "avg_injuries", "avg_deaths", "snow_over_0_1", 
                                               "precipitation_over_0_1", "total_precipitation", "total_snowfall"])

# Parse conditions data
conditions_lines = conditions_data.split("\n")[2:]  # Skip the header lines
conditions_values = [line.split() for line in conditions_lines if line.strip()]
conditions_df = pd.DataFrame(conditions_values, columns=["day_condition", "avg_injuries_per_accident", 
                                                          "avg_deaths_per_accident"])

# Convert columns to appropriate data types
daily_df["avg_injuries"] = daily_df["avg_injuries"].astype(float)
daily_df["avg_deaths"] = daily_df["avg_deaths"].astype(float)
daily_df["snow_over_0_1"] = daily_df["snow_over_0_1"].astype(int)
daily_df["precipitation_over_0_1"] = daily_df["precipitation_over_0_1"].astype(int)
daily_df["total_precipitation"] = daily_df["total_precipitation"].astype(float)
daily_df["total_snowfall"] = daily_df["total_snowfall"].astype(float)

conditions_df["avg_injuries_per_accident"] = conditions_df["avg_injuries_per_accident"].astype(float)
conditions_df["avg_deaths_per_accident"] = conditions_df["avg_deaths_per_accident"].astype(float)

# Create bar charts for injuries and deaths
conditions = conditions_df["day_condition"]
avg_injuries = conditions_df["avg_injuries_per_accident"]
avg_deaths = conditions_df["avg_deaths_per_accident"]

plt.figure(figsize=(10, 6))
plt.bar(conditions, avg_injuries, alpha=0.7, label='Average Injuries')
plt.title('Average Injuries Per Accident by Condition')
plt.ylabel('Average Injuries')
plt.xlabel('Condition')
plt.legend()
plt.savefig("average_injuries_by_condition.png")  # Save the plot
plt.close()  # Close the figure

plt.figure(figsize=(10, 6))
plt.bar(conditions, avg_deaths, alpha=0.7, label='Average Deaths', color='red')
plt.title('Average Deaths Per Accident by Condition')
plt.ylabel('Average Deaths')
plt.xlabel('Condition')
plt.legend()
plt.savefig("average_deaths_by_condition.png")  # Save the plot
plt.close()  # Close the figure

# Reload and ensure all columns are properly handled
daily_df['avg_injuries'] = daily_df['avg_injuries'].astype(float)
daily_df['total_snowfall'] = daily_df['total_snowfall'].astype(float)
daily_df['total_precipitation'] = daily_df['total_precipitation'].astype(float)

# Filter for snowy and precipitation days again
snowy_days = daily_df[daily_df["snow_over_0_1"] == 1].copy()
precipitation_days = daily_df[daily_df["precipitation_over_0_1"] == 1].copy()

# Ensure modifications do not throw warnings
snowy_days['avg_injuries'] = snowy_days['avg_injuries'].astype(float)
precipitation_days['avg_injuries'] = precipitation_days['avg_injuries'].astype(float)

# Bin the data for snowy and precipitation days
snowfall_bins = pd.cut(snowy_days['total_snowfall'], bins=10)
snowfall_avg_injuries = snowy_days.groupby(snowfall_bins, observed=False)['avg_injuries'].mean()

precipitation_bins = pd.cut(precipitation_days['total_precipitation'], bins=10)
precipitation_avg_injuries = precipitation_days.groupby(precipitation_bins, observed=False)['avg_injuries'].mean()

# Snowfall chart
plt.figure(figsize=(12, 8))
snowfall_avg_injuries.plot(kind="bar", alpha=0.7, color="blue", label="Average Injuries")
plt.title("Average Injuries Per Accident for Snowfall Bins")
plt.xlabel("Total Snowfall (inches)")
plt.ylabel("Average Injuries")
plt.xticks(rotation=30, ha="center")  # Adjust rotation and alignment for better readability
plt.gcf().subplots_adjust(bottom=0.2)  # Add padding to the bottom
plt.legend()
plt.savefig("average_injuries_snowfall_bins.png")  # Save the plot
plt.close()  # Close the figure

# Precipitation chart
plt.figure(figsize=(12, 8))
precipitation_avg_injuries.plot(kind="bar", alpha=0.7, color="green", label="Average Injuries")
plt.title("Average Injuries Per Accident for Precipitation Bins")
plt.xlabel("Total Precipitation (inches)")
plt.ylabel("Average Injuries")
plt.xticks(rotation=30, ha="center")  # Adjust rotation and alignment for better readability
plt.gcf().subplots_adjust(bottom=0.2)  # Add padding to the bottom
plt.legend()
plt.savefig("average_injuries_precipitation_bins.png")  # Save the plot
plt.close()  # Close the figure