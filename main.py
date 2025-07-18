import requests
import pandas as pd
import sqlite3
import os
import json

# Create data folder if not exists
os.makedirs("data", exist_ok=True)

# Step 1: Fetch from API
url = "https://fakestoreapi.com/products"
try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()  # Raise an exception for bad status codes
    data = response.json()
except requests.exceptions.RequestException as e:
    print(f"❌ Error fetching data from API: {e}")
    exit(1)
except json.JSONDecodeError as e:
    print(f"❌ Error parsing JSON response: {e}")
    exit(1)

# Save raw data
try:
    with open("data/raw_data.json", "w") as f:
        json.dump(data, f, indent=2)
except IOError as e:
    print(f"❌ Error saving raw data: {e}")
    exit(1)

# Step 2: Convert to DataFrame
try:
    df = pd.DataFrame(data)
    if df.empty:
        print("❌ No data received from API")
        exit(1)
    
    # Flatten nested dictionary columns for SQLite compatibility
    if 'rating' in df.columns:
        df['rating_rate'] = df['rating'].apply(lambda x: x.get('rate', 0) if isinstance(x, dict) else 0)
        df['rating_count'] = df['rating'].apply(lambda x: x.get('count', 0) if isinstance(x, dict) else 0)
        df = df.drop('rating', axis=1)  # Remove the original nested column
    
except Exception as e:
    print(f"❌ Error creating DataFrame: {e}")
    exit(1)

# Step 3: Sort by rating (descending) and take top 5
try:
    # Use the flattened rating_rate column for sorting
    if 'rating_rate' not in df.columns:
        print("❌ 'rating_rate' column not found in data")
        exit(1)
    
    top5 = df.sort_values(by="rating_rate", ascending=False).head(5)
except Exception as e:
    print(f"❌ Error processing rating data: {e}")
    exit(1)

# Step 4: Save full data to SQLite
try:
    conn = sqlite3.connect("data/products.db")
    df.to_sql("products", conn, if_exists="replace", index=False)
    conn.close()
    print("✅ Data saved to SQLite database")
except Exception as e:
    print(f"❌ Error saving to SQLite: {e}")
    exit(1)

# Step 5: Export top 5 to CSV
try:
    top5.to_csv("data/top5_products.csv", index=False)
    print("✅ Top 5 products exported to CSV")
except Exception as e:
    print(f"❌ Error saving CSV: {e}")
    exit(1)

print("✅ Done! Data saved to SQLite and top 5 to CSV.")