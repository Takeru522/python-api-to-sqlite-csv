# 🛠️ API to SQLite to CSV Automation

A simple Python automation script that pulls product data from a public API, processes it using pandas, stores the full dataset in a SQLite database, and exports the top 5 highest-rated products to a CSV file.

---

## 📌 Project Goal

> Automate a data pipeline that:
1. Pulls data from a REST API
2. Cleans and filters the data
3. Stores it in a local database
4. Outputs a report to CSV

This simulates real-world data workflows for e-commerce, product analytics, and business automation.

---

## 🧰 Tech Stack

- `Python 3`
- `requests` for API calls
- `pandas` for data processing
- `sqlite3` for local database
- `JSON` and `CSV` I/O

---

## 🔍 Sample API Used

- URL: [https://fakestoreapi.com/products](https://fakestoreapi.com/products)
- Returns fake e-commerce product data (title, price, rating, etc.)

---

## 📁 Project Structure
