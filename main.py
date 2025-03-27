import requests
import os
import pandas as pd
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")

locations = [
    (51.50853, -0.12574, "London"),
]

url = "https://history.openweathermap.org/data/2.5/history/city"

# Directory to save CSV files
output_dir = "Extracted_Data"
os.makedirs(output_dir, exist_ok=True)

def generate_time_ranges(start_date, end_date, days_per_batch=5):
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())

    time_ranges = []
    current = start_timestamp

    while current < end_timestamp:
        next_time = min(current + (days_per_batch * 86400), end_timestamp)
        time_ranges.append((current, next_time))
        current = next_time

    return time_ranges

write_lock = threading.Lock()

# Function to fetch weather data in parallel
def fetch_weather_data(lat, lon, location_name, start_date, end_date):
    """Fetches weather data in chunks using multi-threading and saves as a CSV."""
    time_ranges = generate_time_ranges(start_date, end_date)
    all_data = []

    for start, end in time_ranges:
        params = {
            "lat": lat,
            "lon": lon,
            "appid": API_KEY,
            "type": "hour",
            "start": start,
            "end": end,
            "units": "metric"
        }

        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            for entry in data.get("list", []):
                all_data.append({
                    "timestamp": datetime.utcfromtimestamp(entry["dt"]).strftime('%Y-%m-%d %H:%M:%S'),
                    "temperature": entry["main"]["temp"],
                    "feels_like": entry["main"]["feels_like"],
                    "pressure": entry["main"]["pressure"],
                    "humidity": entry["main"]["humidity"],
                    "wind_speed": entry["wind"]["speed"],
                    "cloud_coverage": entry["clouds"]["all"],
                    "rain_1h": entry.get("rain", {}).get("1h", 0),
                    "rain_3h": entry.get("rain", {}).get("3h", 0),
                    "snow_1h": entry.get("snow", {}).get("1h", 0),
                    "snow_3h": entry.get("snow", {}).get("3h", 0),
                    "weather_main": entry["weather"][0]["main"],
                    "weather_desc": entry["weather"][0]["description"]
                })

            print(f"{location_name}: {len(data.get('list', []))} records fetched ({start}-{end})")
        else:
            print(f"Error {response.status_code} for {location_name}: {response.text}")

    if all_data:
        df = pd.DataFrame(all_data)
        filename = os.path.join(output_dir, f"weather_{location_name.replace(' ', '_')}.csv")

        # Ensure only one thread writes to the file at a time
        with write_lock:
            df.to_csv(filename, index=False)
            print(f" {location_name}: {len(df)} records saved to '{filename}'")

start_date = datetime(2024, 5, 1)  # Start (YYYY, MM, DD)
end_date = datetime(2025, 3, 1)    # End (YYYY, MM, DD)

# Multi-threading: Fetch data for all locations in parallel
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(fetch_weather_data, lat, lon, name, start_date, end_date) for lat, lon, name in locations]

# Wait for all threads to complete
for future in futures:
    future.result()

print("All data fetched successfully!")
