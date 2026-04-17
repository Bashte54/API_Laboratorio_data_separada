import os 
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


def get_spark_session(app_name="DataCleanSparkAPI"):
    """
    Configura y devuelve una SparkSession. 
    Se utiliza getOrCreate() para seguir el patrón Singleton de Spark.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()

    # Configuración de logs para limpieza de consola
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark


# Funcion para la lectura de archivos

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
            df = spark.read.option("multiline", "true").json(tmp_path)
        elif ext in ["xls", "xlsx"]:
            raise ValueError(f"Formato Excel (.{ext}) requiere dependencias adicionales (ej. spark-excel). Use CSV o JSON.")
        else:
            raise ValueError(f"Formato no soportado: .{ext}")

        # 5. FORZAR ACCIÓN Y CACHE: Obliga a Spark a leer el archivo y cachear los datos.
        df = df.cache() # Cachea el DF en memoria
        df.count()      # Ejecuta el job de lectura
        
        return df
        
    except Exception as e:
        # Si falló, SÍ borramos el temporal porque no sirve
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise IOError(f"Error al leer/procesar el archivo con Spark: {e}")
        

# Funcion para normalizacion
def normalizar_texto_spark(df: DataFrame, column_name: str) -> DataFrame:
    """
    Convierte todo el texto de una columna a minúsculas y elimina espacios 
    en blanco al inicio y al final.
    """
    return df.withColumn(
        column_name, 
        F.lower(F.trim(F.col(column_name)))
    )


# Funciones para la limpieza

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

def remplazar_valor_spark(df: DataFrame, column_name:str, viejo_valor:str, nuevo_valor:str):
    '''
    Buscaremos el valor especifico y hacemos el cambio
    '''

    return df.withColumn(
        column_name, 
        F.when(F.col(column_name) == viejo_valor, nuevo_valor).otherwise(F.col(column_name))
    )

#funcion para manejo de deuplicados 
def eliminar_duplicados_spark(df:DataFrame) -> DataFrame:
    #eliminando filar con valores iguales
    return df.dropDuplicates()

#funcion para imputacion de valores
def rellenar_valores_spark(df:DataFrame, column_name:str, valor:Any) -> DataFrame:
    #rellenando los valores de una columna con un valor especifico
    return df.na.fill({column_name:valor})

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

def cantidad_nulos_total_spark(df: DataFrame) -> Dict[str,int]:
    '''
    Cuenta los nulos de todas las columnas del dataframe 
    '''
    expresiones = [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    #ejecutamos la agregacion 
    resultado_row = df.select(expresiones).collect()[0]

    #convertimoa el objeto Row de spark en un diccionario de pythin 
    return resultado_row.asDict()

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

#funcion para filtrar 
def filtrar_datos_spark(df:DataFrame, condicion:str) -> DataFrame:
    #aplicando un filtro tipo sql 
    return df.filter(condicion)

def desdoblar_columna_spark(df: DataFrame, columna: str) -> DataFrame:
    """
    Aplica 'explode' a una columna que contiene arreglos (arrays),
    creando una fila nueva por cada elemento del arreglo.
    """
    if columna not in df.columns:
        raise ValueError(f"La columna '{columna}' no existe.")
    
    # explode crea una nueva fila por cada elemento en el array de la columna
    return df.withColumn(columna, F.explode(F.col(columna)))

#funcion para guardar daraframe

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
    df_preview = df.limit(10).toPandas() # Limitar el preview y traer al driver
    
    # Reemplazar NaN/NaT (que vienen de Pandas) con None para ser serializable en JSON
    df_preview = df_preview.replace({float('nan'): None}) 

    return {
        "status": "success",
        "rows_count": total_rows,
        "columns": df.columns,
        "data_types": dict(df.dtypes),
        "data_preview": df_preview.to_dict('records')
    }