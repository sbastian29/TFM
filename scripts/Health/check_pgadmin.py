import requests

def check_pgadmin():
    try:
        response = requests.get("http://localhost:8080", timeout=10)
        if response.status_code == 200:
            print("✅ PgAdmin está funcionando correctamente")
            return True
        else:
            print(f"❌ PgAdmin respondió con código: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error conectando a PgAdmin: {e}")
        return False

if __name__ == "__main__":
    check_pgadmin()