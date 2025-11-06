import socket

def check_zookeeper():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('localhost', 2181))
        sock.close()
        
        if result == 0:
            print("✅ Zookeeper está funcionando correctamente")
            return True
        else:
            print("❌ Zookeeper no está respondiendo en el puerto 2181")
            return False
    except Exception as e:
        print(f"❌ Error conectando a Zookeeper: {e}")
        return False

if __name__ == "__main__":
    check_zookeeper()