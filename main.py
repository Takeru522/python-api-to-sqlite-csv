port requests
import pandas as pd
import sqlite3
import os

# Create data folder if not exists
os.makedirs("data", exist_ok=True)

# Step 1: Fetch from API
url = "https://fakestoreapi.com/products"
response = requests.get(url)
data = response.json()

# Save raw data
with open("data/raw_data.json", "w") as f:
    import json
    json.dump(data, f, indent=2)

# Step 2: Convert to DataFrame
df = pd.DataFrame(data)

# Step 3: Sort by rating (descending) and take top 5
df['rating_score'] = df['rating'].apply(lambda x: x['rate'])  # unpack nested rating
top5 = df.sort_values(by="rating_score", ascending=False).head(5)

# Step 4: Save full data to SQLite
conn = sqlite3.connect("data/products.db")
df.to_sql("products", conn, if_exists="replace", index=False)

# Step 5: Export top 5 to CSV
top5.to_csv("data/top5_products.csv", index=False)

print("✅ Done! Data saved to SQLite and top 5 to CSV.")