# api.py (EL NUEVO ORQUESTADOR API)

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pandas as pd
from io import BytesIO, StringIO
from typing import Dict, Any, List
import numpy as np

# IMPORTANTE: Asegúrate de que el nombre del archivo importado coincida con tu archivo
import dataClean_api as dc 

app = FastAPI(title="Data Cleaning API", version="1.0.0")

# =========================================================
# 1. MODELOS DE DATOS (Pydantic)
# =========================================================

# Modelo para enviar el DataFrame a través del cuerpo de la solicitud JSON
class DataFrameData(BaseModel):
    data: List[Dict[str, Any]] # Lista de registros (filas)
    columns: List[str]

# Modelo para la solicitud de limpieza en una columna (Sustituye al "Menú Interactivo")
class OperationRequest(BaseModel):
    df_data: DataFrameData
    column_name: str
    operation: str # Ejemplos: "eliminar_nulos", "transformar_int", "renombrar"
    param: str | None = None # Usado para tipos (int, float) o nuevo nombre

# =========================================================
# 2. ENDPOINTS DE ORQUESTACIÓN
# =========================================================
@app.post("/upload_and_preview")
async def upload_and_preview(file: UploadFile = File(...)):
    # ... (código de lectura de archivo omitido)
    try:
        contents = await file.read()
        file_buffer = BytesIO(contents)
        df = dc.read_file_from_buffer(file_buffer, file.filename)
        
        # === SOLUCIÓN: Reemplazar NaN con None ===
        # Convertir NaN a None para que sea compatible con JSON
        df_json_safe = df.replace({np.nan: None})
        # ========================================
        
        # Usamos df_json_safe para la conversión final
        return {
            "filename": file.filename,
            "rows": len(df_json_safe),
            "columns": list(df_json_safe.columns),
            "head": df_json_safe.head(5).to_dict(orient="records")
        }
        
    except (IOError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Error de procesamiento: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")


@app.post("/apply_operation")
async def apply_operation(request: OperationRequest):
    """
    Aplica una operación de limpieza o análisis sobre un DataFrame recibido.
    Sustituye la lógica de 'orquestador_lim' y 'menu_columna'.
    """
    try:
        # 1. Recrear el DataFrame a partir de los datos JSON
        df = pd.DataFrame(request.df_data.data, columns=request.df_data.columns)
        col = request.column_name
        op = request.operation
        param = request.param

        # 2. Mapeo del Orquestador a las Funciones Puras (Lógica)
        
        # --- Operaciones que devuelven un DF modificado (Limpieza) ---
        
        if op == "eliminar_nulos":
            df_result = dc.eliminar_nulos_api(df, col)
            
        elif op == "eliminar_columna":
            df_result = dc.eliminar_columna_api(df, col)
            
        elif op == "transformar":
            if not param: raise ValueError("Parámetro 'tipo' de transformación es requerido.")
            df_result = dc.transformar_columna_api(df, col, param)
            
        elif op == "renombrar":
            if not param: raise ValueError("Parámetro 'nuevo nombre' es requerido.")
            df_result = dc.renombrar_columna_api(df, col, param)
            
        elif op == "extraer_numeros":
            df_result = dc.extraer_numeros_api(df, col)
            
        elif op == "separar_valores":
            # Aquí necesitarías un modelo más complejo si se requiere más de 1 parámetro,
            # pero asumiremos que 'param' es el separador.
            if not param: raise ValueError("Parámetro 'separador' es requerido.")
            # Nota: Necesitarías un nuevo nombre de columna. Usaremos 'col_new' temporalmente.
            df_result = dc.separar_valores_api(df, col, separador=param, nuevo_nombre=f"{col}_new")

        # --- Operaciones que devuelven ANÁLISIS (Reportes) ---

        elif op == "tipo_datos":
            results = dc.tipo_datos_api(df, col)
            return {"status": "analysis", "operation": op, "column": col, "results": results}
            
        elif op == "cantidad_nulos":
            results = dc.cantidad_nulos_api(df, col)
            return {"status": "analysis", "operation": op, "column": col, "nulos_count": results}
            
        elif op == "detectar_patrones":
            results = dc.detectar_patrones_api(df, col)
            return {"status": "analysis", "operation": op, "column": col, "results": results}
            
        elif op == "correlaciones":
            results = dc.correlaciones_api(df)
            return {"status": "analysis", "operation": op, "results": results}
        
        else:
            raise ValueError(f"Operación '{op}' no reconocida.")

        # 3. Devolver el DataFrame limpio como JSON (Si fue una operación de limpieza)
        return {
            "status": "cleaned",
            "operation": op,
            "rows_after": len(df_result),
            # Devuelve el DataFrame completo como lista de diccionarios para el frontend
            "new_data": df_result.to_dict(orient="records"), 
            "new_columns": list(df_result.columns)
        }

    except (ValueError, KeyError) as e:
        raise HTTPException(status_code=400, detail=f"Error en la solicitud: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")
    
# Dentro de api.py, después de /apply_operation

# Modelo para la solicitud de guardado
class SaveRequest(BaseModel):
    df_data: DataFrameData
    filename: str # Nombre original del archivo (e.g., "banco.csv")
    
@app.post("/save_cleaned_data")
async def save_cleaned_data(request: SaveRequest):
    """
    Recibe el DataFrame limpio y lo guarda en el estructura de carpetas
    definida en la función pura.
    """
    try:
        # 1. Recrear el DataFrame a partir de los datos JSON
        df = pd.DataFrame(request.df_data.data, columns=request.df_data.columns)
        
        # 2. Extraer el nombre base y la extensión
        filename = request.filename
        parts = filename.split('.')
        nombre_base = ".".join(parts[:-1]) # todo excepto la extensión
        ext = f".{parts[-1]}" # e.g., ".csv"
        
        # 3. Llamar a la función pura de guardado
        success, message = dc.guardar_dataframe_api(df, nombre_base, ext)

        if success:
            return {
                "status": "saved",
                "message": f"Archivo guardado exitosamente en: {message}",
                "path": message
            }
        else:
            # Si la función pura devuelve False (ej. formato no soportado)
            raise ValueError(f"Error al guardar: {message}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al guardar el archivo: {e}")