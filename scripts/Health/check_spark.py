import requests

def check_spark():
    try:
        response = requests.get("http://localhost:8081", timeout=10)
        if response.status_code == 200:
            print("✅ Spark Master está funcionando correctamente")
            return True
        else:
            print(f"❌ Spark Master respondió con código: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error conectando a Spark Master: {e}")
        return False

if __name__ == "__main__":
    check_spark()