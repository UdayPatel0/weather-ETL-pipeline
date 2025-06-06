import requests
import pandas as pd
import sqlite3
from datetime import datetime

def extract_wether_data():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 19.07,        # Mumbai
        "longitude": 72.87,
        "current_weather": True
    }
    
    response = requests.get(url, params=params)
    data = response.json()

    current = data["current_weather"]
    result = {
        "city":"mumbai",
        "temperature": current["temperature"],
        "windspeed": current["windspeed"],
        "time": current["time"]
    
    }
    return result

def transform_data(data_dic):
    df = pd.DataFrame([data_dic])
    return df

def load_to_sqlite(df):
    conn = sqlite3.connect("weather_data.db")
    df.to_sql("weather",conn,if_exists ="append", index=False)
    conn.close()
   

if __name__ == "__main__":
    try:
        print("running ETL...")
        data = extract_wether_data()
        df = transform_data(data)
        load_to_sqlite(df)
        print("ETL completed successfully.")
        print (df)
    
    except Exception as e:
        print("Error:", e)
