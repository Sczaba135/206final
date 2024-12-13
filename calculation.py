import sqlite3
import pandas as pd

def calculate_and_write_to_file():
    """
    Perform database join, calculate average crashes on snowfall days, and write results to a file.
    """
    conn = sqlite3.connect('project_data.db')
    try:
        # SQL query to join tables and calculate
        query = """
        SELECT
            w.date,
            SUM(c.number_of_persons_injured) AS total_injuries,
            SUM(c.number_of_persons_killed) AS total_fatalities,
            AVG(c.number_of_persons_injured + c.number_of_persons_killed) AS avg_crashes,
            s.snowfall_sum
        FROM
            weather_summary w
        JOIN
            snowfall_data s ON w.weather_id = s.weather_id
        JOIN
            car_crash c ON w.date = c.crash_date
        WHERE
            s.snowfall_sum > 0
        GROUP BY
            w.date
        ORDER BY
            w.date ASC
        """
        # Execute the query and fetch data
        df = pd.read_sql_query(query, conn)

        # Perform additional calculations if needed (already handled in SQL for simplicity)
        print(df.head())

        # Write the data to a file
        with open("calculated_data.txt", "w") as f:
            f.write("Average Crashes on Snowfall Days (New York City):\n")
            f.write("="*50 + "\n")
            for _, row in df.iterrows():
                f.write(f"Date: {row['date']}\n")
                f.write(f"Total Injuries: {row['total_injuries']}\n")
                f.write(f"Total Fatalities: {row['total_fatalities']}\n")
                f.write(f"Average Crashes: {row['avg_crashes']:.2f}\n")
                f.write(f"Snowfall: {row['snowfall_sum']} inches\n")
                f.write("-"*50 + "\n")
        print("Results written to 'calculated_data.txt'")
    finally:
        conn.close()

if __name__ == "__main__":
    calculate_and_write_to_file()
