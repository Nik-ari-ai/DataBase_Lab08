import psycopg2
from geopy.geocoders import Nominatim
import time

conn = psycopg2.connect(
    dbname="demo",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)
cur = conn.cursor()
cur.execute("""
    DROP FUNCTION IF EXISTS get_valid_coordinates();
    CREATE OR REPLACE FUNCTION get_valid_coordinates()
    RETURNS TABLE (
        airport_code CHAR(3),
        longitude FLOAT,
        latitude FLOAT
    ) AS $$
    BEGIN
        RETURN QUERY
        SELECT
            a.airport_code,
            a.coordinates[0] AS longitude,
            a.coordinates[1] AS latitude
        FROM airports_data a
        WHERE a.coordinates[0] BETWEEN 35 AND 50
          AND a.coordinates[1] BETWEEN 35 AND 50;
    END;
    $$ LANGUAGE plpgsql;
""")
# Create table
cur.execute("DROP TABLE IF EXISTS address;")
cur.execute("""
    CREATE TABLE address (
        address_id SERIAL PRIMARY KEY,
        address_text TEXT,
        address_x FLOAT,
        address_y FLOAT
    );
""")
cur.execute("SELECT * FROM get_valid_coordinates();")
rows = cur.fetchall()
geolocator = Nominatim(user_agent="task4_app")
for row in rows:
    airport_code, lon, lat = row
    try:
        location = geolocator.reverse((lat, lon), language='en')
        if location:
            print("Saving:", location.address)
            cur.execute("""
                INSERT INTO address (address_text, address_x, address_y)
                VALUES (%s, %s, %s)
            """, (location.address, lon, lat))
        time.sleep(1)
    except Exception as e:
        print("Error:", e)
conn.commit()
cur.close()
conn.close()

print("DONE")
