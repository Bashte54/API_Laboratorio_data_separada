import os
import sys
import findspark 
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, StringType, BooleanType, DateType
import re
from io import BytesIO, StringIO
import tempfile 
from typing import Union, Dict, Any, Tuple
from datetime import datetime
from pathlib import Path
import pandas as pd


# =========================================================
# 0. CONFIGURACIÓN CRÍTICA DE RUTAS Y ENTORNO
# =========================================================

# 1. Rutas del entorno (Variables a usar)
spark_home = os.environ.get('SPARK_HOME', '/opt/spark') 
# RUTA ABSOLUTA DE PYTHON: Apuntando a su entorno Conda 'api_data'
python_executable = '/home/yuyots/anaconda3/envs/api_data/bin/python' 
# Ruta de Java 11 (Directorio raíz, como debe ser)
java_home = os.environ.get('JAVA_HOME', '/usr/lib/jvm/java-17-openjdk-amd64') 


# 2. ESTABLECER VARIABLES DE ENTORNO ANTES DE CUALQUIER OTRA COSA
# Es crítico que estas variables estén disponibles para la JVM y PySpark al inicio.
os.environ['SPARK_HOME'] = spark_home
os.environ['JAVA_HOME'] = java_home
os.environ['PYSPARK_PYTHON'] = python_executable 

# Configuración adicional para asegurar que Spark use el Python correcto
os.environ['PYSPARK_SUBMIT_ARGS'] = f"--conf spark.pyspark.python={python_executable} pyspark-shell"


# 3. Configuración de findspark
try:
    # findspark.init solo necesita SPARK_HOME. La ruta de Python se maneja con os.environ
    findspark.init(spark_home=spark_home) 
    print(f"INFO: findspark inicializado. Usando SPARK_HOME: {spark_home}")
except Exception as e:
    print(f"ERROR: findspark falló al inicializar. Error: {e}")
    # Si findspark falla, las variables de entorno ya están establecidas.


# =========================================================
# 4. FUNCIÓN DE INICIALIZACIÓN DE SPARK (SINGLETON)
# =========================================================

_spark_session = None

def get_spark_session() -> SparkSession:
    """
    Patrón Singleton: Inicializa la SparkSession, forzando la configuración de Python 
    y deshabilitando las optimizaciones de Arrow/Pandas para compatibilidad de versión.
    """
    global _spark_session
    if _spark_session is None:
        try:
            _spark_session = SparkSession.builder \
                .appName("DataCleanSparkAPI") \
                .config("spark.driver.host", "127.0.0.1") \
                .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8") \
                .config("spark.driver.maxResultSize", "4g") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .config("spark.sql.execution.pandas.grouped.structuredDataConversion", "false") \
                .getOrCreate()
            
            # Ajuste de log level (opcional, pero reduce ruido)
            _spark_session.sparkContext.setLogLevel("ERROR") 

            print("INFO: SparkSession iniciada correctamente. (Configuraciones de compatibilidad aplicadas)")
        except Exception as e:
            # Si el error persiste, puede ser un problema de permisos o de Java
            print(f"ERROR: Falló al inicializar SparkSession. Error: {e}")
            raise e
    return _spark_session


# ***************************************************************
# FORZAR LA INICIALIZACIÓN AL CARGAR EL MÓDULO
# ***************************************************************
try:
    get_spark_session()
except Exception as e:
    print(f"FATAL: No se pudo inicializar Spark al cargar el módulo. Error: {e}")

# =========================================================================
# 1. FUNCIÓN DE LECTURA (Gestión de Archivos Temporales)
# =========================================================================

def read_file_from_buffer_spark(file_buffer: Union[BytesIO, StringIO], filename: str) -> DataFrame:
    """
    Escribe el buffer a un archivo temporal (sin borrado automático), obliga a Spark 
    a leerlo/cachearlo y luego realiza la limpieza manual.
    """
    spark = get_spark_session()
    ext = filename.split('.')[-1].lower()
    df = None
    tmp_path = None
    
    # 1. Crear archivo temporal de manera segura (delete=False)
    # Importante: No usar 'with' para controlar la vida del archivo
    # Usamos NamedTemporaryFile desde el módulo tempfile importado
    tmp_file = tempfile.NamedTemporaryFile(delete=False) 
    tmp_path = tmp_file.name
    
    try:
        # 2. Escribir el buffer
        file_buffer.seek(0)
        
        # Escribir el contenido del buffer al archivo temporal
        if isinstance(file_buffer, BytesIO):
            tmp_file.write(file_buffer.read())
        elif isinstance(file_buffer, StringIO):
            tmp_file.write(file_buffer.read().encode('utf-8'))
        
        # 3. Cerrar el descriptor: CRÍTICO para liberar el archivo antes de que Spark lo lea.
        tmp_file.close() 

        # 4. Lectura perezosa de Spark
        if ext == "csv":
            df = spark.read.csv(
                tmp_path, 
                header=True, 
                inferSchema=True, 
                sep=',', 
                mode="PERMISSIVE"
            )
        elif ext == "json":
            df = spark.read.json(tmp_path)
        elif ext in ["xls", "xlsx"]:
            raise ValueError(f"Formato Excel (.{ext}) requiere dependencias adicionales (ej. spark-excel). Use CSV o JSON.")
        else:
            raise ValueError(f"Formato no soportado: .{ext}")

        # 5. FORZAR ACCIÓN Y CACHE: Obliga a Spark a leer el archivo y cachear los datos.
        df = df.cache() # Cachea el DF en memoria
        df.count()      # Ejecuta el job de lectura
        
        return df
        
    except Exception as e:
        # Si hay error en lectura o cacheo, relanzar el error.
        raise IOError(f"Error al leer/procesar el archivo con Spark: {e}")
        
    finally:
        # 6. Limpieza: Eliminar el archivo temporal del disco si existe.
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


# =========================================================================
# 2. FUNCIONES DE LIMPIEZA (Puras - Devuelven DF de Spark modificado)
# =========================================================================

# Definición de UDF para la lógica de limpieza compleja de números
def _limpiar_valor_spark_udf(valor):
    """Función de Python para aplicar la lógica de regex."""
    if valor is None:
        return None
    valor = str(valor)
    match = re.search(r'(\d+(?:\.\d+)?)', valor)
    if match:
        num = float(match.group(1))
        if '%' in valor:
            num = num / 100
        return num
    return None

# Registrar la UDF con el tipo de retorno FloatType
limpiar_valor_udf = F.udf(_limpiar_valor_spark_udf, FloatType())

def extraer_numeros_spark(df: DataFrame, columna: str) -> DataFrame:
    if columna not in df.columns:
        return df
    
    # Aplicar la UDF y cachear el resultado
    df_new = df.withColumn(columna, limpiar_valor_udf(F.col(columna))).cache()
    df_new.count() # Forzar persistencia
    return df_new

def eliminar_columna_spark(df: DataFrame, columna: str) -> DataFrame:
    if columna not in df.columns:
        return df
    return df.drop(columna)

def transformar_columna_spark(df: DataFrame, columna: str, tipo: str) -> DataFrame:
    """Tipos: 'int', 'float', 'str', 'bool', 'date'"""
    if columna not in df.columns:
        return df

    tipo_spark_map = {
        "int": IntegerType(),
        "float": FloatType(),
        "str": StringType(),
        "bool": BooleanType(),
        "date": DateType()
    }

    if tipo not in tipo_spark_map:
        raise ValueError(f"Tipo de dato '{tipo}' no reconocido por Spark.")

    try:
        if tipo == "date":
            # Usar F.to_date para intentar parsear el string a una fecha.
            df_new = df.withColumn(columna, F.to_date(F.col(columna).cast(StringType()), 'yyyy-MM-dd')).cache() 
        else:
            df_new = df.withColumn(columna, F.col(columna).cast(tipo_spark_map[tipo])).cache()
        
        df_new.count() # Forzar persistencia
        return df_new
            
    except Exception as e:
        raise ValueError(f"Error al convertir la columna a {tipo} en Spark: {e}")

def eliminar_nulos_spark(df: DataFrame, columna: str) -> DataFrame:
    if columna not in df.columns:
        return df
    df_new = df.na.drop(subset=[columna]).cache()
    df_new.count()
    return df_new

def renombrar_columna_spark(df: DataFrame, columna_vieja: str, columna_nueva: str) -> DataFrame:
    if columna_vieja not in df.columns:
        return df
    return df.withColumnRenamed(columna_vieja, columna_nueva)

def separar_valores_spark(df: DataFrame, columna: str, separador: str, nuevo_nombre: str) -> DataFrame:
    if columna not in df.columns:
         raise ValueError(f"Columna '{columna}' no encontrada.")

    # Usamos F.split con limit=2 para dividir solo en dos partes
    df_new = df.withColumn('temp_split', F.split(F.col(columna).cast(StringType()), separador, 2))
    
    # Extraer las dos partes por índice
    df_new = df_new.withColumn(columna, F.col('temp_split').getItem(0))      # Parte izquierda
    df_new = df_new.withColumn(nuevo_nombre, F.col('temp_split').getItem(1)) # Parte derecha
    
    # Eliminar columna temporal y cachear
    df_new = df_new.drop('temp_split').cache()
    df_new.count()
    
    return df_new

# =========================================================================
# 3. FUNCIONES DE ANÁLISIS (Puras - Devuelven un diccionario de resultados)
# =========================================================================

def tipo_datos_spark(df: DataFrame, columna: str) -> Dict[str, str]:
    if columna not in df.columns:
        raise ValueError(f"Columna '{columna}' no encontrada.")
        
    schema_type = dict(df.dtypes)[columna]
    return {
        "schema_type": schema_type,
        "note": "PySpark solo puede reportar el tipo de dato definido en el esquema (schema_type)."
    }


def cantidad_nulos_spark(df: DataFrame, columna: str) -> int:
    if columna not in df.columns:
        raise ValueError(f"Columna '{columna}' no encontrada.")
    
    null_count = df.filter(F.col(columna).isNull()).count()
    return int(null_count)


def detectar_patrones_spark(df: DataFrame, columna: str) -> Dict[str, Any]:
    if columna not in df.columns:
        raise ValueError(f"Columna '{columna}' no encontrada.")
        
    total_count = df.count()
    unique_count = df.select(columna).distinct().count()
    
    frequent_values_df = df.groupBy(columna).count().withColumnRenamed('count', 'frequency')
    
    top_values = frequent_values_df.orderBy(F.desc('frequency')).limit(10)
    # Convertir a Pandas para llevar los resultados al driver y hacer el diccionario
    top_values_dict = top_values.toPandas().set_index(columna).to_dict()['frequency']
    
    return {
        "unique_count": int(unique_count),
        "total_count": int(total_count),
        "frequent_values": top_values_dict
    }


def correlaciones_spark(df: DataFrame) -> Dict[str, Any]:
    numerical_cols = [c for c, t in df.dtypes if t in ['int', 'double', 'float', 'long']]
    
    if not numerical_cols:
        return {"error": "No hay columnas numéricas para correlación."}
    
    numeric_df = df.select(*numerical_cols)
    
    try:
        # Convertir a Pandas para calcular la matriz de correlación (limitación común en APIs Spark)
        pandas_corr_matrix = numeric_df.toPandas().corr().to_dict()
        return pandas_corr_matrix
    except Exception as e:
        return {"error": f"Error al calcular la matriz de correlación (el DF puede ser demasiado grande para el driver): {e}"}

# =========================================================================
# 4. FUNCIÓN DE GUARDADO (Adaptación al Servidor)
# =========================================================================

def guardar_dataframe_spark(df: DataFrame, nombre_base: str, ext: str) -> Tuple[bool, str]:
    """
    Guarda el DataFrame en el servidor usando Spark. 
    Spark siempre guarda en directorios (part-files)
    """
    base_dir = Path("data_limpia")
    
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        if ext in [".csv", ".json", ".parquet"]:
            tipo_archivo = ext[1:] # e.g., 'csv'
            
            # Ruta de guardado: directorio
            ruta_directorio = base_dir / f"{tipo_archivo}_limpia/{nombre_base}_limpio_{fecha}.{tipo_archivo}_dir"
            
            # coalece(1) para forzar un único archivo de salida dentro del directorio (recomendado para APIs)
            writer = df.coalesce(1).write.mode('overwrite')
            
            if ext == ".csv":
                writer.csv(str(ruta_directorio), header=True)
            elif ext == ".json":
                writer.json(str(ruta_directorio))
            elif ext == ".parquet":
                writer.parquet(str(ruta_directorio))
            
            ruta_final = str(ruta_directorio)
            
        else:
            return False, f"Extensión {ext} no implementada para guardado API con Spark. Se recomienda .parquet, .csv o .json."

        return True, ruta_final

    except Exception as e:
        return False, str(e)

# =========================================================================
# 5. FUNCIÓN DE CONVERSIÓN DE SPARK A RESPUESTA JSON
# =========================================================================

def spark_df_to_api_response(df: DataFrame) -> Dict[str, Any]:
    """
    Convierte un DataFrame de Spark a un formato serializable por FastAPI.
    """
    get_spark_session() 
    
    total_rows = df.count() 
    df_preview = df.limit(100).toPandas() # Limitar el preview y traer al driver
    
    # Reemplazar NaN/NaT (que vienen de Pandas) con None para ser serializable en JSON
    df_preview = df_preview.replace({float('nan'): None}) 

    return {
        "status": "success",
        "rows_count": total_rows,
        "columns": df.columns,
        "data_types": dict(df.dtypes),
        "data_preview": df_preview.to_dict('records')
    }