from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, StringType, BooleanType, DateType
import re
from io import BytesIO, StringIO
from tempfile import NamedTemporaryFile
from typing import Union, Dict, Any, Tuple
from datetime import datetime
from pathlib import Path

# =========================================================================
# 0. CONFIGURACIÓN E INICIALIZACIÓN DE SPARK
# =========================================================================

# Inicializar la SparkSession
spark = SparkSession.builder \
    .appName("DataCleanSparkAPI") \
    .getOrCreate()

# =========================================================================
# 1. FUNCIÓN DE LECTURA (ADAPTADA A SPARK)
# NOTA: Spark necesita una RUTA para leer archivos, no buffers. 
# La solución API-Friendly es escribir el buffer a un archivo temporal.
# =========================================================================

def read_file_from_buffer_spark(file_buffer: Union[BytesIO, StringIO], filename: str) -> DataFrame:
    """
    Escribe el buffer a un archivo temporal y luego usa Spark para leerlo.
    """
    ext = filename.split('.')[-1].lower()
    
    # Usamos un archivo temporal que se elimina automáticamente al salir
    with NamedTemporaryFile(delete=True) as tmp:
        file_buffer.seek(0)
        
        # Escribir el contenido del buffer al archivo temporal
        if isinstance(file_buffer, BytesIO):
            tmp.write(file_buffer.read())
        elif isinstance(file_buffer, StringIO):
            # Para StringIO, necesitamos codificar a bytes
            tmp.write(file_buffer.read().encode('utf-8'))
        
        tmp.flush() # Asegurar que los datos se escriben a disco

        try:
            if ext == "csv":
                # PySpark CSV reader. Asumimos delimitador ',' y que infiera el esquema.
                df = spark.read.csv(
                    tmp.name, 
                    header=True, 
                    inferSchema=True,
                    sep=',' # Simplificado: detección de separador es costosa en Spark
                )
            elif ext == "json":
                df = spark.read.json(tmp.name)
            elif ext in ["xls", "xlsx"]:
                raise ValueError(f"Formato Excel (.{ext}) requiere dependencias adicionales (ej. spark-excel). Use CSV o JSON.")
            else:
                raise ValueError(f"Formato no soportado: .{ext}")

            return df

        except Exception as e:
            raise IOError(f"Error al leer el archivo con Spark: {e}")

# =========================================================================
# 2. FUNCIONES DE LIMPIEZA (Puras - Devuelven DF de Spark modificado)
# =========================================================================

def eliminar_columna_spark(df: DataFrame, columna: str) -> DataFrame:
    if columna not in df.columns:
        return df # Spark DataFrames son inmutables
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
        "date": DateType() # Para fechas, es mejor usar F.to_date() o F.to_timestamp()
    }

    if tipo not in tipo_spark_map:
        raise ValueError(f"Tipo de dato '{tipo}' no reconocido por Spark.")

    try:
        if tipo == "date":
            # Intentar parsear el string a una fecha. Formato inferido o estándar.
            df_new = df.withColumn(columna, F.to_date(F.col(columna).cast(StringType()), 'yyyy-MM-dd')) 
            # Si el formato no es estándar ('yyyy-MM-dd'), se requeriría un formato explícito
        else:
            # Castear al tipo de PySpark
            df_new = df.withColumn(columna, F.col(columna).cast(tipo_spark_map[tipo]))
            
    except Exception as e:
        raise ValueError(f"Error al convertir la columna a {tipo} en Spark: {e}")
        
    return df_new

def eliminar_nulos_spark(df: DataFrame, columna: str) -> DataFrame:
    if columna not in df.columns:
        return df
    # Equivalente a df.dropna(subset=[columna]) en Pandas
    return df.na.drop(subset=[columna])

def renombrar_columna_spark(df: DataFrame, columna_vieja: str, columna_nueva: str) -> DataFrame:
    if columna_vieja not in df.columns:
        return df
    return df.withColumnRenamed(columna_vieja, columna_nueva)

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
    
    # Aplicar la UDF a la columna
    # Nota: Las UDF son más lentas que las funciones nativas de Spark
    df_new = df.withColumn(columna, limpiar_valor_udf(F.col(columna)))
    return df_new

def separar_valores_spark(df: DataFrame, columna: str, separador: str, nuevo_nombre: str) -> DataFrame:
    if columna not in df.columns:
         raise ValueError(f"Columna '{columna}' no encontrada.")

    # Usamos F.split con limit=2 para dividir solo en dos partes
    df_new = df.withColumn('temp_split', F.split(F.col(columna).cast(StringType()), separador, 2))
    
    # Extraer las dos partes por índice
    df_new = df_new.withColumn(columna, F.col('temp_split').getItem(0))      # Parte izquierda
    df_new = df_new.withColumn(nuevo_nombre, F.col('temp_split').getItem(1)) # Parte derecha
    
    # Eliminar columna temporal
    df_new = df_new.drop('temp_split')
    
    return df_new

# =========================================================================
# 3. FUNCIONES DE ANÁLISIS (Puras - Devuelven un diccionario de resultados)
# =========================================================================

def tipo_datos_spark(df: DataFrame, columna: str) -> Dict[str, str]:
    """
    Spark solo puede reportar el tipo de dato definido en el esquema.
    """
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
    
    # Usar .count() en el filtro es una acción y devuelve el número exacto
    null_count = df.filter(F.col(columna).isNull()).count()
    return int(null_count)


def detectar_patrones_spark(df: DataFrame, columna: str) -> Dict[str, Any]:
    if columna not in df.columns:
        raise ValueError(f"Columna '{columna}' no encontrada.")
        
    # Estas llamadas son acciones y fuerzan el cálculo
    total_count = df.count()
    unique_count = df.select(columna).distinct().count()
    
    # Top 10 valores frecuentes. Se convierte a Pandas temporalmente para el resultado final.
    frequent_values_df = df.groupBy(columna).count().withColumnRenamed('count', 'frequency')
    
    # toPandas() trae los resultados al driver, necesario para el dict final
    top_values = frequent_values_df.orderBy(F.desc('frequency')).limit(10)
    top_values_dict = top_values.toPandas().set_index(columna).to_dict()['frequency']
    
    return {
        "unique_count": int(unique_count),
        "total_count": int(total_count),
        "frequent_values": top_values_dict
    }


def correlaciones_spark(df: DataFrame) -> Dict[str, Any]:
    # Filtrar para columnas numéricas
    numerical_cols = [c for c, t in df.dtypes if t in ['int', 'double', 'float', 'long']]
    
    if not numerical_cols:
        return {"error": "No hay columnas numéricas para correlación."}
    
    numeric_df = df.select(*numerical_cols)
    
    try:
        # CONCESIÓN: Se convierte a Pandas para calcular la matriz de correlación completa 
        # (más fácil que hacer un bucle de Spark para todas las combinaciones).
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
        # Definir la ruta como un directorio de guardado
        if ext in [".csv", ".json", ".parquet"]:
            # Spark guarda en un directorio.
            tipo_archivo = ext[1:] # e.g., 'csv'
            
            ruta_directorio = base_dir / f"{tipo_archivo}_limpia/{nombre_base}_limpio_{fecha}.{tipo_archivo}_dir"
            
            # coalece(1) para forzar un único archivo de salida dentro del directorio (buena práctica para resultados de API)
            writer = df.coalesce(1).write.mode('overwrite')
            
            if ext == ".csv":
                writer.csv(str(ruta_directorio), header=True)
            elif ext == ".json":
                writer.json(str(ruta_directorio))
            elif ext == ".parquet":
                 # Parquet es el formato recomendado por Spark
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
    # .limit(100) es una buena práctica para previsualizar sin sobrecargar la red
    # .toPandas() lleva los datos al driver (necesario para serializar)
    # .to_dict('records') convierte las filas a una lista de diccionarios
    
    df_preview = df.limit(100).toPandas()
    
    return {
        "status": "success",
        "rows_count": df.count(), # Una acción más lenta, pero precisa
        "columns": df.columns,
        "data_types": dict(df.dtypes),
        "data_preview": df_preview.to_dict('records')
    }