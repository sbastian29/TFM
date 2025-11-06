import requests

def check_jupyter():
    try:
        response = requests.get("http://localhost:8888", timeout=10)
        if response.status_code == 200:
            print("✅ Jupyter Notebook está funcionando correctamente")
            return True
        else:
            print(f"❌ Jupyter Notebook respondió con código: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error conectando a Jupyter Notebook: {e}")
        return False

if __name__ == "__main__":
    check_jupyter()