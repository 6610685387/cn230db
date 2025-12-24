import requests
import sqlite3
from datetime import date, timedelta
from collections import Counter
import re

# === CONFIG ===
DB_NAME = "nasa_neo.db"
API_KEY = "3eobKsja9UAbxoj2FxEWmyfOTYUfvbpSKKmVlrL0"
DAYS_BACK = 7


# === STEP 1: ดึงข้อมูล NEO จาก NASA API ===
def fetch_neos(api_key, days_back=7):
    start_date = date.today() - timedelta(days=days_back)
    end_date = date.today()
    url = "https://api.nasa.gov/neo/rest/v1/feed"
    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "api_key": api_key,
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()["near_earth_objects"]

    neos = []
    for day in data:
        for obj in data[day]:
            est_diameter = obj["estimated_diameter"]["meters"]
            approach_data = obj["close_approach_data"][0]
            neos.append(
                {
                    "id": obj["id"],
                    "name": obj["name"].strip("()"),
                    "diameter_min": est_diameter["estimated_diameter_min"],
                    "diameter_max": est_diameter["estimated_diameter_max"],
                    "velocity_km_s": float(
                        approach_data["relative_velocity"]["kilometers_per_second"]
                    ),
                    "miss_distance_km": float(
                        approach_data["miss_distance"]["kilometers"]
                    ),
                    "approach_date": approach_data["close_approach_date"],
                }
            )
    return neos


# === STEP 2: จัดการ SQLite ===
def create_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS neos (
            id TEXT PRIMARY KEY,
            name TEXT,
            diameter_min REAL,
            diameter_max REAL,
            velocity_km_s REAL,
            miss_distance_km REAL,
            approach_date TEXT
        )
    """
    )
    conn.commit()


def insert_neos(conn, neos):
    cur = conn.cursor()
    for n in neos:
        cur.execute(
            """
            INSERT OR REPLACE INTO neos VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (
                n["id"],
                n["name"],
                n["diameter_min"],
                n["diameter_max"],
                n["velocity_km_s"],
                n["miss_distance_km"],
                n["approach_date"],
            ),
        )
    conn.commit()


# === STEP 3: วิเคราะห์ข้อมูลด้วย SQL + Python  ===
def run_analytics(conn):
    cur = conn.cursor()

    print("\n📊 1. จำนวน NEOs ทั้งหมด:")
    cur.execute("SELECT COUNT(*) FROM neos")
    print("  →", cur.fetchone()[0])

    print("\n📏 2. ขนาดเฉลี่ย (diameter):")
    cur.execute("SELECT ROUND(AVG((diameter_min + diameter_max)/2), 2) FROM neos")
    print("  →", cur.fetchone()[0], "meters")

    print("\n🚀 3. ความเร็วเฉลี่ย (km/s):")
    cur.execute("SELECT ROUND(AVG(velocity_km_s), 2) FROM neos")
    print("  →", cur.fetchone()[0], "km/s")

    print("\n🌍 4. วัตถุที่เข้าใกล้โลกที่สุด:")
    cur.execute(
        "SELECT name, ROUND(miss_distance_km, 2) FROM neos ORDER BY miss_distance_km ASC LIMIT 1"
    )
    result = cur.fetchone()
    print(f"  → Name: {result[0]}\n    Closest Distance: {result[1]:,.2f} km")

    print("\n☄️ 5. วัตถุที่มีขนาดใหญ่ที่สุด:")
    cur.execute(
        "SELECT name, ROUND(diameter_max, 2) FROM neos ORDER BY diameter_max DESC LIMIT 1"
    )
    result = cur.fetchone()
    print(f"  → Name: {result[0]}\n    Max Diameter: {result[1]:,.2f} meters")

    print("\n📅 6. จำนวน NEOs แต่ละวัน:")
    approach_counts = []
    print("\n  | Date       | Number of NEOs |")
    print("  |------------|----------------|")
    for row in cur.execute(
        "SELECT approach_date, COUNT(*) FROM neos GROUP BY approach_date ORDER BY approach_date"
    ):
        print(f"  | {row[0]} | {row[1]:>14} |")
        approach_counts.append(row)

    print("\n📆 7. วันที่มี NEO เข้าใกล้โลกมากที่สุด:")
    cur.execute(
        "SELECT approach_date, COUNT(*) as c FROM neos GROUP BY approach_date ORDER BY c DESC LIMIT 1"
    )
    row = cur.fetchone()
    print(f"  → Date: {row[0]}\n    Number of NEOs: {row[1]}")


# === MAIN ===
def main():
    neos = fetch_neos(API_KEY, DAYS_BACK)

    conn = sqlite3.connect(DB_NAME)
    create_table(conn)
    insert_neos(conn, neos)
    run_analytics(conn)
    conn.close()


if __name__ == "__main__":
    main()
