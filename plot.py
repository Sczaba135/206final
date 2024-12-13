import pandas as pd
import matplotlib.pyplot as plt

# Sample data
daily_averages = pd.DataFrame({
    'date': pd.date_range(start="2022-01-01", periods=100, freq='D'),
    'avg_injuries': [0.25, 0.30, 0.25, 0.30, 0.45] * 20,
    'avg_deaths': [0.0, 0.0, 0.0, 0.0, 0.05] * 20,
    'snow_over_0_1': [0, 0, 1, 0, 0] * 20,
    'precipitation_over_0_1': [1, 1, 0, 0, 0] * 20,
})

avg_per_accident = pd.DataFrame({
    'day_condition': ['Both', 'Neither', 'Precipitation', 'Snow'],
    'avg_injuries_per_accident': [0.445, 0.426, 0.455, 0.325],
    'avg_deaths_per_accident': [0.0, 0.0019, 0.0054, 0.0],
})

# 1. Average Injuries and Deaths Based on Indicators
indicators = ['Snow', 'Precipitation', 'Both', 'Neither']
avg_injuries = avg_per_accident['avg_injuries_per_accident']
avg_deaths = avg_per_accident['avg_deaths_per_accident']

x = range(len(indicators))
plt.bar(x, avg_injuries, width=0.4, label='Injuries', color='blue', align='center')
plt.bar([p + 0.4 for p in x], avg_deaths, width=0.4, label='Deaths', color='red', align='center')
plt.xticks([p + 0.2 for p in x], indicators)
plt.title("Average Injuries and Deaths by Weather Condition")
plt.ylabel("Average Count")
plt.xlabel("Weather Condition")
plt.legend()
plt.show()

# 2. Distribution of Weather Conditions
condition_counts = [
    daily_averages['snow_over_0_1'].sum(),
    daily_averages['precipitation_over_0_1'].sum(),
    (daily_averages['snow_over_0_1'] & daily_averages['precipitation_over_0_1']).sum(),
    len(daily_averages) - daily_averages[['snow_over_0_1', 'precipitation_over_0_1']].any(axis=1).sum()
]

plt.pie(condition_counts, labels=['Snow', 'Precipitation', 'Both', 'Neither'], autopct='%1.1f%%', startangle=140)
plt.title("Distribution of Weather Conditions")
plt.show()

# 3. Time Series Analysis of Daily Averages
plt.plot(daily_averages['date'], daily_averages['avg_injuries'], label="Average Injuries", color='blue')
plt.plot(daily_averages['date'], daily_averages['avg_deaths'], label="Average Deaths", color='red')
plt.title("Daily Average Injuries and Deaths Over Time")
plt.xlabel("Date")
plt.ylabel("Average Count")
plt.legend()
plt.show()

# 4. Injuries and Deaths per Accident by Condition
plt.bar(indicators, avg_per_accident['avg_injuries_per_accident'], color='blue', label="Injuries")
plt.bar(indicators, avg_per_accident['avg_deaths_per_accident'], color='red', bottom=avg_per_accident['avg_injuries_per_accident'], label="Deaths")
plt.title("Average Injuries and Deaths per Accident by Condition")
plt.xlabel("Weather Condition")
plt.ylabel("Average Count")
plt.legend()
plt.show()
