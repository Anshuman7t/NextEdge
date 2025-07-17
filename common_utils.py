import os
import psycopg2
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

def log_error(error_message, player_url=None):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO rugby_logs (error_message, player_url)
            VALUES (%s, %s);
        """, (str(error_message), player_url))
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Failed to log error to database: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def insert_player(data):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO players (name, age, weight, height, country, position, team, source, player_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_url) DO UPDATE SET
                age = EXCLUDED.age,
                weight = EXCLUDED.weight,
                height = EXCLUDED.height,
                country = EXCLUDED.country,
                position = EXCLUDED.position,
                team = EXCLUDED.team,
                updated_at = CURRENT_TIMESTAMP;
        """, (
            data['name'], data['age'], data['weight'], data['height'], data['country'],
            data['position'], data['team'], data['source'], data['player_url']
        ))
        conn.commit()
        return True
    except Exception as e:
        log_error(f"DB Error inserting {data['name']}: {e}", data['player_url'])
        return False
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
