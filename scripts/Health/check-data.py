import psycopg2
from psycopg2 import sql, OperationalError


def get_connection():
    """Crea y devuelve una conexión a la base de datos PostgreSQL."""
    conn_params = {
        "dbname": "tfm_database",
        "user": "tfm_user",
        "password": "tfm_password_2024",
        "host": "localhost",  # Usa 'postgres-tfm' si ejecutas dentro de Docker
        "port": 5433,
    }
    try:
        conn = psycopg2.connect(**conn_params)
        print("✅ Conexión exitosa a la base de datos.")
        return conn
    except OperationalError as e:
        print(f"❌ Error de conexión a PostgreSQL:\n{e}")
        return None


def fetch_all_users(conn):
    """Obtiene y muestra todos los usuarios de la tabla usuarios_tfm."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT id, nombre, email, fecha_creacion FROM usuarios_tfm;"))
            rows = cursor.fetchall()

            if not rows:
                print("⚠️ No hay usuarios registrados en la tabla.")
                return

            print("\n👥 Usuarios registrados:")
            for row in rows:
                print(f" - ID: {row[0]}, Nombre: {row[1]}, Email: {row[2]}, Fecha creación: {row[3]}")

    except psycopg2.Error as e:
        print(f"❌ Error al ejecutar la consulta SQL:\n{e}")


def main():
    conn = get_connection()
    if conn is None:
        print("🚫 No se pudo establecer la conexión. Verifica los parámetros o el estado del contenedor Docker.")
        return

    try:
        fetch_all_users(conn)
    finally:
        conn.close()
        print("\n🔒 Conexión cerrada correctamente.")


if __name__ == "__main__":
    main()
