# check_all_services_complete.py
import sys
import os

# Agregar el directorio actual al path para importar los otros scripts
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

def check_all_services_complete():
    print("🔍 VERIFICACIÓN COMPLETA DE TODOS LOS SERVICIOS")
    print("=" * 60)
    
    results = {}
    
    # PostgreSQL
    try:
        from check_postgres import check_postgres
        print("\n🗄️  Verificando PostgreSQL...")
        if check_postgres():
            results["PostgreSQL"] = "✅ OK"
        else:
            results["PostgreSQL"] = "❌ ERROR"
    except Exception as e:
        results["PostgreSQL"] = f"❌ ERROR: {e}"
    
    # PgAdmin
    try:
        from check_pgadmin import check_pgadmin
        print("\n🧭 Verificando PgAdmin...")
        if check_pgadmin():
            results["PgAdmin"] = "✅ OK"
        else:
            results["PgAdmin"] = "❌ ERROR"
    except Exception as e:
        results["PgAdmin"] = f"❌ ERROR: {e}"
    
    # Zookeeper
    try:
        from check_zookeeper import check_zookeeper
        print("\n🦍 Verificando Zookeeper...")
        if check_zookeeper():
            results["Zookeeper"] = "✅ OK"
        else:
            results["Zookeeper"] = "❌ ERROR"
    except Exception as e:
        results["Zookeeper"] = f"❌ ERROR: {e}"
    
    # Kafka
    try:
        from check_kafka import check_kafka_complete
        print("\n📡 Verificando Kafka...")
        if check_kafka_complete():
            results["Kafka"] = "✅ OK"
        else:
            results["Kafka"] = "❌ ERROR"
    except Exception as e:
        results["Kafka"] = f"❌ ERROR: {e}"
    
    # Spark
    try:
        from check_spark import check_spark
        print("\n⚙️  Verificando Spark...")
        if check_spark():
            results["Spark"] = "✅ OK"
        else:
            results["Spark"] = "❌ ERROR"
    except Exception as e:
        results["Spark"] = f"❌ ERROR: {e}"
    
    # Jupyter
    try:
        from check_jupyter import check_jupyter
        print("\n🧠 Verificando Jupyter Notebook...")
        if check_jupyter():
            results["Jupyter"] = "✅ OK"
        else:
            results["Jupyter"] = "❌ ERROR"
    except Exception as e:
        results["Jupyter"] = f"❌ ERROR: {e}"
    
    # Mostrar resumen
    print("\n" + "=" * 60)
    print("📊 RESUMEN FINAL DE ESTADO")
    print("=" * 60)
    
    for service, status in results.items():
        print(f"{service:15} {status}")
    
    print("=" * 60)
    
    # Contar servicios OK
    ok_count = sum(1 for status in results.values() if "✅" in status)
    total_count = len(results)
    
    print(f"Servicios OK: {ok_count}/{total_count}")
    
    if ok_count == total_count:
        print("🎉 ¡TODOS LOS SERVICIOS FUNCIONAN CORRECTAMENTE!")
        return True
    else:
        print("⚠️  Algunos servicios necesitan atención")
        return False

if __name__ == "__main__":
    check_all_services_complete()