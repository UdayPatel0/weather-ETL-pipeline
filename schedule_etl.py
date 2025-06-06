import schedule
import time
from ETL_PIPELINE import extract_wether_data, transform_data, load_to_sqlite

def job():
    print("Running scheduled ETL...")

    try:
        data = extract_wether_data()
        df = transform_data(data)
        load_to_sqlite(df)
        print("ETL ran at", time.ctime())
    except Exception as e:
        print("Error during ETL:", e)

# ✅ EITHER: Run job every 1 minute (for testing)
schedule.every(1).minutes.do(job)

# ✅ OR: Run job daily at a specific time (e.g., 15:14)
#schedule.every().day.at("15:14").do(job)

print("Scheduler started. Waiting for scheduled time...")

while True:
    schedule.run_pending()
    time.sleep(1)
