from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, Any, List
import uuid

# IMPORTANTE: Importar el archivo con la lógica de PySpark
import dataCleanSpark as dc 

# =========================================================
# 0. ESTRUCTURA DE CACHÉ Y INICIALIZACIÓN
# =========================================================

# Diccionario simple para almacenar DataFrames de Spark en memoria
# { df_id: Spark DataFrame }
# NOTA: En producción, esto debería ser un sistema de caché distribuida (Redis/Memcached).
DF_CACHE: Dict[str, dc.DataFrame] = {}

app = FastAPI(title="Spark Data Cleaning API", version="1.0.0 (Spark Edition)")

# =========================================================
# 1. MODELOS DE DATOS (Pydantic)
# =========================================================

# Modelo de Solicitud de Operación (Ahora usa df_id)
class OperationRequest(BaseModel):
    df_id: str # ID único del DataFrame en la caché del servidor
    column_name: str
    operation: str
    param: str | None = None

# Modelo de Solicitud de Guardado (Ahora usa df_id)
class SaveRequest(BaseModel):
    df_id: str
    filename: str 
    
# =========================================================
# 2. ENDPOINTS DE ORQUESTACIÓN
# =========================================================

@app.post("/upload_and_preview")
async def upload_and_preview(file: UploadFile = File(...)):
    """
    Carga el archivo, crea un DataFrame de Spark, lo almacena en caché 
    y devuelve una vista previa.
    """
    try:
        contents = await file.read()
        file_buffer = dc.BytesIO(contents) # Usamos BytesIO de dataCleanSpark
        
        # 1. Leer el archivo usando la función de PySpark
        df_spark = dc.read_file_from_buffer_spark(file_buffer, file.filename)
        
        # 2. Generar un ID único para la caché
        df_id = str(uuid.uuid4())
        
        # 3. Almacenar el DF de Spark en la caché del servidor
        DF_CACHE[df_id] = df_spark.cache() # Usamos .cache() para persistir en memoria

        # 4. Convertir para la respuesta de la API (solo una vista previa limitada)
        response_data = dc.spark_df_to_api_response(df_spark)
        
        return {
            "filename": file.filename,
            "df_id": df_id, # IMPORTANTE: Devolver el ID al cliente
            "rows": response_data["rows_count"],
            "columns": response_data["columns"],
            "data_types": response_data["data_types"],
            "head": response_data["data_preview"]
        }
        
    except (IOError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Error de procesamiento: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")


@app.post("/apply_operation")
async def apply_operation(request: OperationRequest):
    """
    Aplica una operación de limpieza o análisis sobre el DataFrame cacheado.
    """
    df_id = request.df_id
    
    if df_id not in DF_CACHE:
        raise HTTPException(status_code=404, detail=f"DataFrame con ID '{df_id}' no encontrado o expirado.")
        
    df_current = DF_CACHE[df_id]
    col = request.column_name
    op = request.operation
    param = request.param

    try:
        # 2. Mapeo del Orquestador a las Funciones Puras de Spark
        
        # --- Operaciones que devuelven un DF modificado (Limpieza) ---
        
        if op == "eliminar_nulos":
            df_result = dc.eliminar_nulos_spark(df_current, col)
            
        elif op == "eliminar_columna":
            df_result = dc.eliminar_columna_spark(df_current, col)
            
        elif op == "transformar":
            if not param: raise ValueError("Parámetro 'tipo' de transformación es requerido.")
            df_result = dc.transformar_columna_spark(df_current, col, param)
            
        elif op == "renombrar":
            if not param: raise ValueError("Parámetro 'nuevo nombre' es requerido.")
            df_result = dc.renombrar_columna_spark(df_current, col, param)
            
        elif op == "extraer_numeros":
            df_result = dc.extraer_numeros_spark(df_current, col)
            
        elif op == "separar_valores":
            if not param: raise ValueError("Parámetro 'separador' es requerido.")
            # Nota: Spark no necesita convertir a str antes de split si la columna es String
            df_result = dc.separar_valores_spark(df_current, col, separador=param, nuevo_nombre=f"{col}_new")

        # --- Operaciones que devuelven ANÁLISIS (Reportes) ---

        elif op == "tipo_datos":
            results = dc.tipo_datos_spark(df_current, col)
            return {"status": "analysis", "operation": op, "column": col, "results": results}
            
        elif op == "cantidad_nulos":
            results = dc.cantidad_nulos_spark(df_current, col)
            return {"status": "analysis", "operation": op, "column": col, "nulos_count": results}
            
        elif op == "detectar_patrones":
            results = dc.detectar_patrones_spark(df_current, col)
            return {"status": "analysis", "operation": op, "column": col, "results": results}
            
        elif op == "correlaciones":
            results = dc.correlaciones_spark(df_current)
            return {"status": "analysis", "operation": op, "results": results}
        
        else:
            raise ValueError(f"Operación '{op}' no reconocida.")

        # 3. Persistir el nuevo DataFrame limpio en la caché (solo si hubo limpieza)
        if op in ["eliminar_nulos", "eliminar_columna", "transformar", "renombrar", "extraer_numeros", "separar_valores"]:
            
            # Liberar la caché del DF viejo y persistir el nuevo
            df_current.unpersist()
            DF_CACHE[df_id] = df_result.cache()
            
            # Devolver el DataFrame limpio como JSON (solo preview)
            response_data = dc.spark_df_to_api_response(df_result)
            
            return {
                "status": "cleaned",
                "operation": op,
                "df_id": df_id,
                "rows_after": response_data["rows_count"],
                "new_columns": response_data["columns"],
                "data_preview": response_data["data_preview"] # Preview
            }
        
    except (ValueError, KeyError) as e:
        raise HTTPException(status_code=400, detail=f"Error en la solicitud: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")


@app.post("/save_cleaned_data")
async def save_cleaned_data(request: SaveRequest):
    """
    Recibe el df_id, busca el DF en caché y lo guarda en disco usando Spark.
    """
    df_id = request.df_id
    
    if df_id not in DF_CACHE:
        raise HTTPException(status_code=404, detail=f"DataFrame con ID '{df_id}' no encontrado o expirado.")
        
    df_current = DF_CACHE[df_id]
    
    try:
        # 1. Extraer el nombre base y la extensión
        filename = request.filename
        parts = filename.split('.')
        nombre_base = ".".join(parts[:-1]) 
        # Forzamos la extensión a .csv para usar la lógica de guardado de Spark
        ext = ".csv" 
        
        # 2. Llamar a la función pura de guardado de Spark
        success, message = dc.guardar_dataframe_spark(df_current, nombre_base, ext)

        # 3. Opcional: Liberar el DF de la caché después de guardarlo
        df_current.unpersist()
        del DF_CACHE[df_id]
        
        if success:
            return {
                "status": "saved",
                "message": f"Archivo (directorio) guardado exitosamente en: {message}",
                "path": message
            }
        else:
            raise ValueError(f"Error al guardar: {message}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al guardar el archivo: {e}")