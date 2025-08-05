
import os
import psycopg2
from dotenv import load_dotenv
from api_request import mock_fetch_data


load_dotenv()


def connect_to_db():
    print("Connecting to the database...")
    try:
        conn = psycopg2.connect(
            host="postgres",
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=os.getenv("POSTGRES_POST")
        )
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise


def create_table(conn):
    print("Creating table if it does not exist...")
    try:
        cursor = conn.cursor()
        cursor.execute("""--sql
            create schema if not exists weather;
            create table if not exists weather.raw_weather_data (
                id serial primary key,
                city text,
                temperature float,
                weather_descriptions text,
                wind_speed float,
                time timestamp,
                inserted_at timestamp default now(),
                utc_offset text
            );
        """)
        conn.commit()
        print("Table was created successfully.")

    except psycopg2.Error as e:
        print(f"Table creation failed: {e}")
        raise


def insert_records(conn, data):
    print("Inserting records into the database...")
    try:
        weather = data["current"]
        location = data["location"]
        cursor = conn.cursor()
        cursor.execute("""--sql
            insert into weather.raw_weather_data (
                city, 
                temperature,
                weather_descriptions,
                wind_speed,
                time,
                inserted_at,
                utc_offset
            ) values (%s, %s, %s, %s, %s, now(), %s)
        """,(
            location["name"],
            weather["temperature"],
            weather["weather_descriptions"][0],
            weather["wind_speed"],
            location["localtime"],
            location["utc_offset"]
        ))
        conn.commit()
        print("successfully inserted records into the database.")
    except psycopg2.Error as e:
        print(f"error inserting records: {e}")
        raise


def main():
    try:
        data = mock_fetch_data()  # Use the mock function for testing
        conn = connect_to_db()
        create_table(conn)
        insert_records(conn, data)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if "conn" in locals():
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()