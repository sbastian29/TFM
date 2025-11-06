# check_kafka_complete.py
import socket
import subprocess
import time
import sys

def run_command(cmd, timeout=15, shell=True):
    """Ejecuta un comando y retorna el resultado"""
    try:
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            timeout=timeout, 
            shell=shell
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Timeout"
    except Exception as e:
        return False, "", str(e)

def check_kafka_complete():
    print("🔍 INICIANDO VERIFICACIÓN COMPLETA DE KAFKA")
    print("=" * 50)
    
    # 1. Verificar Docker Desktop
    print("\n1. ✅ Verificando Docker Desktop...")
    success, stdout, stderr = run_command("docker version")
    if not success:
        print("❌ Docker Desktop no está funcionando")
        print("💡 Inicia Docker Desktop primero")
        return False
    print("✅ Docker Desktop está funcionando")
    
    # 2. Verificar contenedor Kafka
    print("\n2. ✅ Verificando contenedor Kafka...")
    success, stdout, stderr = run_command('docker ps --filter "name=kafka" --format "{{.Names}}: {{.Status}}"')
    if not success or "kafka" not in stdout:
        print("❌ Contenedor Kafka no encontrado")
        print("🚀 Iniciando Kafka...")
        success, stdout, stderr = run_command("docker-compose up -d kafka", timeout=60)
        if not success:
            print("❌ Error iniciando Kafka:", stderr)
            return False
        print("✅ Kafka iniciado, esperando 10 segundos...")
        time.sleep(10)
    else:
        print(f"✅ Contenedor Kafka: {stdout.strip()}")
    
    # 3. Verificar logs y estado interno
    print("\n3. 📋 Analizando logs de Kafka...")
    success, stdout, stderr = run_command("docker logs kafka --tail 30", timeout=20)
    if success:
        logs_lower = stdout.lower()
        if "error" in logs_lower or "exception" in logs_lower:
            print("❌ Errores encontrados en logs")
            print("🔄 Reiniciando Kafka...")
            run_command("docker-compose restart kafka", timeout=45)
            print("⏳ Esperando 15 segundos...")
            time.sleep(15)
        elif "started" in logs_lower or "ready" in logs_lower:
            print("✅ Logs indican que Kafka está listo")
        else:
            print("⚠️  Kafka está en proceso de inicio")
    else:
        print("⚠️  No se pudieron obtener logs")
    
    # 4. Verificar conexión interna
    print("\n4. 🔍 Verificando Kafka internamente...")
    success, stdout, stderr = run_command(
        'docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list', 
        timeout=20
    )
    if success:
        print("✅ Kafka funciona INTERNAMENTE")
        if stdout.strip():
            print(f"   Topics: {stdout.strip()}")
        return True
    else:
        print("❌ Kafka no responde internamente")
    
    # 5. Verificar puerto externo con múltiples intentos
    print("\n5. 🌐 Verificando puerto 9092...")
    for attempt in range(3):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex(('localhost', 9092))
            sock.close()
            
            if result == 0:
                print("✅ Kafka está respondiendo en puerto 9092")
                return True
            else:
                print(f"❌ Intento {attempt + 1}/3: Puerto no responde")
        except Exception as e:
            print(f"❌ Intento {attempt + 1}/3: Error - {e}")
        
        if attempt < 2:  # No esperar después del último intento
            print("⏳ Reintentando en 5 segundos...")
            time.sleep(5)
    
    # 6. Último intento - reinicio completo
    print("\n6. 🚀 EJECUTANDO REINICIO COMPLETO...")
    print("🛑 Deteniendo servicios...")
    run_command("docker-compose stop kafka zookeeper", timeout=30)
    time.sleep(5)
    
    print("🗑️  Limpiando contenedores...")
    run_command("docker rm kafka zookeeper", timeout=10)
    time.sleep(3)
    
    print("🚀 Iniciando Zookeeper y Kafka...")
    run_command("docker-compose up -d zookeeper kafka", timeout=60)
    
    print("⏳ Esperando 30 segundos para inicio completo...")
    for i in range(6):
        print(f"   {i * 5 + 5}s/30s...")
        time.sleep(5)
    
    # 7. Verificación final
    print("\n7. ✅ VERIFICACIÓN FINAL...")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('localhost', 9092))
        sock.close()
        
        if result == 0:
            print("🎉 ¡KAFKA FUNCIONANDO CORRECTAMENTE!")
            return True
        else:
            print("❌ Kafka aún no responde después del reinicio completo")
            print("\n💡 SOLUCIONES MANUALES:")
            print("1. Ejecuta: docker-compose down")
            print("2. Ejecuta: docker-compose up -d")
            print("3. Espera 2 minutos y vuelve a probar")
            return False
            
    except Exception as e:
        print(f"❌ Error en verificación final: {e}")
        return False

def main():
    print("🔄 Script de diagnóstico y reparación de Kafka")
    print("⏰ Este proceso puede tomar 2-3 minutos...")
    
    start_time = time.time()
    
    if check_kafka_complete():
        print(f"\n✅ VERIFICACIÓN EXITOSA - Tiempo: {time.time() - start_time:.1f}s")
        sys.exit(0)
    else:
        print(f"\n❌ VERIFICACIÓN FALLIDA - Tiempo: {time.time() - start_time:.1f}s")
        sys.exit(1)

if __name__ == "__main__":
    main()