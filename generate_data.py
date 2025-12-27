import sqlite3
import random
from datetime import datetime, timedelta
import pandas as pd

def generate_synthetic_data():
    """Generate realistic Uber ride-sharing data"""
    
    # Configuration
    NUM_CITIES = 20
    NUM_DRIVERS = 200
    NUM_RIDERS = 500
    NUM_TRIPS = 2000
    
    # City data (real US cities)
    cities = [
        'San Francisco', 'Los Angeles', 'New York', 'Chicago', 'Seattle',
        'Boston', 'Austin', 'Denver', 'Miami', 'Atlanta',
        'Portland', 'San Diego', 'Phoenix', 'Dallas', 'Houston',
        'Philadelphia', 'Detroit', 'Minneapolis', 'Las Vegas', 'Nashville'
    ]
    
    # Connect to SQLite
    conn = sqlite3.connect('uber_data.db')
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cities (
            city_id INTEGER PRIMARY KEY,
            city_name TEXT NOT NULL,
            state TEXT,
            population INTEGER,
            avg_fare_multiplier REAL
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS drivers (
            driver_id INTEGER PRIMARY KEY,
            driver_name TEXT NOT NULL,
            driver_email TEXT,
            phone TEXT,
            city_id INTEGER,
            rating REAL,
            total_trips INTEGER,
            join_date DATE,
            vehicle_type TEXT,
            FOREIGN KEY (city_id) REFERENCES cities(city_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS riders (
            rider_id INTEGER PRIMARY KEY,
            rider_name TEXT NOT NULL,
            rider_email TEXT,
            phone TEXT,
            payment_method TEXT,
            total_rides INTEGER,
            signup_date DATE,
            preferred_city_id INTEGER,
            FOREIGN KEY (preferred_city_id) REFERENCES cities(city_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trips (
            trip_id INTEGER PRIMARY KEY,
            rider_id INTEGER,
            driver_id INTEGER,
            city_id INTEGER,
            pickup_time TIMESTAMP,
            dropoff_time TIMESTAMP,
            distance_miles REAL,
            duration_minutes INTEGER,
            fare_amount REAL,
            tip_amount REAL,
            total_amount REAL,
            payment_status TEXT,
            trip_status TEXT,
            FOREIGN KEY (rider_id) REFERENCES riders(rider_id),
            FOREIGN KEY (driver_id) REFERENCES drivers(driver_id),
            FOREIGN KEY (city_id) REFERENCES cities(city_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pii_metadata (
            table_name TEXT,
            column_name TEXT,
            pii_type TEXT,
            requires_masking BOOLEAN,
            access_level INTEGER
        )
    ''')
    
    # Insert cities
    states = ['CA', 'NY', 'IL', 'WA', 'MA', 'TX', 'CO', 'FL', 'GA', 'OR', 'AZ', 'PA', 'MI', 'MN', 'NV', 'TN']
    city_data = []
    for i, city in enumerate(cities):
        city_data.append((
            i + 1,
            city,
            random.choice(states),
            random.randint(500000, 5000000),
            round(random.uniform(0.8, 1.3), 2)
        ))
    
    cursor.executemany('INSERT INTO cities VALUES (?, ?, ?, ?, ?)', city_data)
    
    # Insert drivers
    first_names = ['John', 'Sarah', 'Mike', 'Emily', 'David', 'Lisa', 'Chris', 'Anna', 'James', 'Maria']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
    vehicle_types = ['UberX', 'UberXL', 'Uber Comfort', 'Uber Black']
    
    driver_data = []
    for i in range(NUM_DRIVERS):
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        driver_data.append((
            i + 1,
            name,
            f"{name.lower().replace(' ', '.')}@email.com",
            f"+1-555-{random.randint(1000, 9999)}",
            random.randint(1, len(cities)),
            round(random.uniform(4.0, 5.0), 2),
            random.randint(50, 5000),
            (datetime.now() - timedelta(days=random.randint(30, 1095))).date(),
            random.choice(vehicle_types)
        ))
    
    cursor.executemany('INSERT INTO drivers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', driver_data)
    
    # Insert riders
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Apple Pay', 'Google Pay']
    rider_data = []
    for i in range(NUM_RIDERS):
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        rider_data.append((
            i + 1,
            name,
            f"{name.lower().replace(' ', '.')}@email.com",
            f"+1-555-{random.randint(1000, 9999)}",
            random.choice(payment_methods),
            random.randint(1, 500),
            (datetime.now() - timedelta(days=random.randint(30, 1095))).date(),
            random.randint(1, len(cities))
        ))
    
    cursor.executemany('INSERT INTO riders VALUES (?, ?, ?, ?, ?, ?, ?, ?)', rider_data)
    
    # Insert trips
    trip_statuses = ['completed', 'completed', 'completed', 'completed', 'cancelled']  # 80% completed
    payment_statuses = ['paid', 'paid', 'paid', 'pending']  # 75% paid
    
    trip_data = []
    for i in range(NUM_TRIPS):
        distance = round(random.uniform(1, 30), 2)
        duration = int(distance * random.uniform(2, 5))  # Minutes
        base_fare = distance * random.uniform(1.5, 3.0)
        tip = round(random.uniform(0, base_fare * 0.2), 2)
        
        pickup_time = datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
        dropoff_time = pickup_time + timedelta(minutes=duration)
        
        trip_data.append((
            i + 1,
            random.randint(1, NUM_RIDERS),
            random.randint(1, NUM_DRIVERS),
            random.randint(1, len(cities)),
            pickup_time,
            dropoff_time,
            distance,
            duration,
            round(base_fare, 2),
            tip,
            round(base_fare + tip, 2),
            random.choice(payment_statuses),
            random.choice(trip_statuses)
        ))
    
    cursor.executemany('INSERT INTO trips VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', trip_data)
    
    # Insert PII metadata
    pii_data = [
        ('drivers', 'driver_email', 'EMAIL', True, 2),
        ('drivers', 'phone', 'PHONE', True, 2),
        ('drivers', 'driver_name', 'NAME', True, 1),
        ('riders', 'rider_email', 'EMAIL', True, 2),
        ('riders', 'phone', 'PHONE', True, 2),
        ('riders', 'rider_name', 'NAME', True, 1),
        ('riders', 'payment_method', 'PAYMENT_INFO', False, 2),
    ]
    
    cursor.executemany('INSERT INTO pii_metadata VALUES (?, ?, ?, ?, ?)', pii_data)
    
    conn.commit()
    conn.close()
    
    print("✅ Synthetic Uber data generated successfully!")
    print(f"   - {len(cities)} cities")
    print(f"   - {NUM_DRIVERS} drivers")
    print(f"   - {NUM_RIDERS} riders")
    print(f"   - {NUM_TRIPS} trips")

if __name__ == "__main__":
    generate_synthetic_data()
