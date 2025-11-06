import psycopg2
import sys

def check_postgres():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5433",
            database="tfm_database",
            user="tfm_user",
            password="tfm_password_2024"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        print("✅ PostgreSQL está funcionando correctamente")
        return True
    except Exception as e:
        print(f"❌ Error conectando a PostgreSQL: {e}")
        return False

if __name__ == "__main__":
    check_postgres()