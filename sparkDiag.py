import os
import sys
import findspark 
from pyspark.sql import SparkSession
from pyspark.sql import Row

print("--- DIAGNÓSTICO DE AMBIENTE PYSPARK ---")

# 1. Rutas confirmadas por el usuario
SPARK_HOME = '/opt/spark'
# Usaremos Java 17, que es la ruta que tienes
JAVA_HOME = '/usr/lib/jvm/java-11-openjdk-amd64' 
# Usaremos el Python de tu entorno Conda 'api_data'
PYSPARK_PYTHON = '/home/yuyots/anaconda3/envs/api_data/bin/python'

# 2. Forzar las variables de entorno dentro del script
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ['JAVA_HOME'] = JAVA_HOME
os.environ['PYSPARK_PYTHON'] = PYSPARK_PYTHON
os.environ['PYSPARK_SUBMIT_ARGS'] = f"--conf spark.pyspark.python={PYSPARK_PYTHON} pyspark-shell"

print(f"DEBUG: SPARK_HOME seteado a: {os.environ['SPARK_HOME']}")
print(f"DEBUG: JAVA_HOME seteado a: {os.environ['JAVA_HOME']}")
print(f"DEBUG: PYSPARK_PYTHON seteado a: {os.environ['PYSPARK_PYTHON']}")

# 3. Inicializar findspark
try:
    findspark.init(spark_home=SPARK_HOME)
    print("\n[ÉXITO] findspark se inicializó correctamente.")
except Exception as e:
    print(f"\n[FALLO] findspark falló: {e}")
    sys.exit(1)

# 4. Inicializar SparkSession
spark = None
try:
    spark = SparkSession.builder \
        .appName("SparkDiagnosticTest") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()
    
    print("\n[ÉXITO] SparkSession se inicializó correctamente. ¡El puente Py4J funciona!")
    print(f"Versión de Spark: {spark.version}")

    # 5. Prueba: Crear y mostrar un DataFrame
    data = [Row(id=1, nombre="Luis"), Row(id=2, nombre="Gemini")]
    df = spark.createDataFrame(data)
    
    print("\n[ÉXITO] Ejecutando trabajo simple de Spark:")
    df.show()

    print("\n--- DIAGNÓSTICO TERMINADO ---")
    
except Exception as e:
    print(f"\n[FALLO] ERROR CRÍTICO al iniciar SparkSession: {e}")
    print("Esto significa que la JVM (Java) o el Py4J no se pudieron comunicar.")
    print("Verifique la compatibilidad de Spark y Java (Ej: Spark 3.2+ requiere Java 17).")
    sys.exit(1)
finally:
    if spark:
        spark.stop()