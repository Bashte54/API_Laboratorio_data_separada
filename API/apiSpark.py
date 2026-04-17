from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, Any, List
import uuid
import csv

from fastapi import Depends, status, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from passlib.context import CryptContext
from datetime import datetime, timedelta

from pathlib import Path as FilePath

# importando el dataCleanSpark
import dataCleanSpark as dc 

# Diccionario simple para almacenar DataFrames de Spark en memoria
# { df_id: Spark DataFrame }
DF_CACHE: Dict[str, dc.DataFrame] = {}

app = FastAPI(title="Spark Data Cleaning API", version="2.0.0 (Spark Edition)")

## seguridad token
#configuracion de la seguridad de la api

#JWT
SECRET_KEY = "chulada_de_cosa1091"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")

users = {
    "admin": {
        "username": "admin",
        "hashed_password": pwd_context.hash("datalab0129"),
        "role": "admin"
    }
}

# ==================== FUNCIONES DE SEGURIDAD DE LA API =======================
def verify_password(plain, hashed):
    return pwd_context.verify(plain, hashed)

def authenticate_user(username: str, password: str):
    user = users.get(username)
    if not user or not verify_password(password, user["hashed_password"]):
        return False
    return user

def create_access_token(data: dict, expires_delta: timedelta):
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401)
        return username
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido o expirado"
        )

#funcion para los logs 
def registrar_log(ip: str, usuario: str, token: str, accion: str, detalles: str = ""):
    base_dir = FilePath(__file__).resolve().parent.parent
    archivo_log = base_dir /"LOGS"/"Logs_apiSpark.csv"
    archivo_log.parent.mkdir(parents=True, exist_ok=True)
    #archivo_log = "/home/papime2442/Descargas/cleanDataToolv2/LOGS/Logs_apiSpark.csv"
    ahora = datetime.now()
    # Formato: Fecha, Hora, IP, Usuario, Token, Acción, Detalles
    with open(archivo_log, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            ahora.strftime("%Y-%m-%d"), 
            ahora.strftime("%H:%M:%S"), 
            ip, 
            usuario, 
            token,
            accion, 
            detalles
        ])

# ==================== ENDPOINT DE LOGIN ==============================
@app.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Credenciales incorrectas")

    token = create_access_token(
        data={"sub": user["username"]},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )

    return {"access_token": token, "token_type": "bearer"}

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
    

#iniciio de los endpoints de orquestacion

@app.post("/spark/upload")
async def spark_upload(
    request: Request,
    file: UploadFile = File(...),
    user: str = Depends(get_current_user)
    ):
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

        ## logs
        auth_header = request.headers.get("Authorization")
        token_str = auth_header.split(" ")[1] if auth_header else "N/A"

        registrar_log(
            ip=request.client.host,
            usuario=user,
            token=token_str,
            accion="Subida de Archivo",
            detalles=f"Archivo: {file.filename}"
        )
        
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

#=========================OPERACIONES DE LIMPIEZA

@app.post("/spark/{df_id}/eliminar_nulos/{column_name}")
async def spark_eliminar_nulos( df_id: str, column_name: str, request: Request, user: str = Depends(get_current_user)):
    df = get_df(df_id)
    df_result = dc.eliminar_nulos_spark(df, column_name)
    DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: eliminar_nulos", f"{df_id} col:{column_name}")
    
    response_data = dc.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned","operation": "eliminar_nulos","df_id": df_id,
        "rows_after": response_data["rows_count"], columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }
    









@app.post("/apply_operation")
async def apply_operation(
    request_log: Request, # Para IP y Headers
    request: OperationRequest, # Datos de la operación
    user: str = Depends(get_current_user)
    ):
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

    # --- EXTRAER TOKEN E IP AL INICIO ---
    auth_header = request_log.headers.get("Authorization")
    token_str = auth_header.split(" ")[1] if auth_header and " " in auth_header else "N/A"
    client_ip = request_log.client.host

    try:
        # 2. Mapeo del Orquestador
        
        # --- OPERACIONES DE LIMPIEZA ---
        if op == "eliminar_nulos":
            df_result = dc.eliminar_nulos_spark(df_current, col)
        elif op == "eliminar_columna":
            df_result = dc.eliminar_columna_spark(df_current, col)
        elif op == "transformar":
            if not param: raise ValueError("Parámetro 'tipo' es requerido.")
            df_result = dc.transformar_columna_spark(df_current, col, param)
        elif op == "renombrar":
            if not param: raise ValueError("Parámetro 'nuevo nombre' es requerido.")
            df_result = dc.renombrar_columna_spark(df_current, col, param)
        elif op == "extraer_numeros":
            df_result = dc.extraer_numeros_spark(df_current, col)
        elif op == "separar_valores":
            if not param: raise ValueError("Parámetro 'separador' es requerido.")
            df_result = dc.separar_valores_spark(df_current, col, separador=param, nuevo_nombre=f"{col}_new")
        elif op == "normalizar_texto":
            df_result = dc.normalizar_texto_spark(df_current, col)
        elif op == "reemplazar_valor":
            if not param or "," not in param: raise ValueError("Formato 'viejo_valor,nuevo_valor' requerido")
            viejo, nuevo = param.split(",")
            df_result = dc.remplazar_valor_spark(df_current, col, viejo.strip(), nuevo.strip())
        elif op == "eliminar_duplicados":
            df_result = dc.eliminar_duplicados_spark(df_current)
        elif op == "rellenar_nulos":
            if not param: raise ValueError("Valor de relleno requerido.")
            df_result = dc.rellenar_nulos_spark(df_current, col, param)
        elif op == "filtrar":
            if not param: raise ValueError("Condición SQL requerida.")
            df_result = dc.filtrar_datos_spark(df_current, param)
        elif op == "desdoblar":
            df_result = dc.desdoblar_columna_spark(df_current, col)

        # --- OPERACIONES DE ANÁLISIS (Llevan return y su propio log) ---
        elif op in ["tipo_datos", "cantidad_nulos", "detectar_patrones", "correlaciones", "nulos_totales"]:
            if op == "tipo_datos":
                results = dc.tipo_datos_spark(df_current, col)
            elif op == "cantidad_nulos":
                results = dc.cantidad_nulos_spark(df_current, col)
            elif op == "detectar_patrones":
                results = dc.detectar_patrones_spark(df_current, col)
            elif op == "correlaciones":
                results = dc.correlaciones_spark(df_current)
            elif op == "nulos_totales":
                results = dc.cantidad_nulos_total_spark(df_current)

            # Log para análisis
            registrar_log(client_ip, user, token_str, f"Análisis: {op}", f"Col: {col}")
            return {"status": "analysis", "operation": op, "results": results}
        
        else:
            raise ValueError(f"Operación '{op}' no reconocida.")

        # 3. Persistir y Log para LIMPIEZA
        operaciones_limpieza = ["eliminar_nulos", "eliminar_columna", "transformar", "renombrar", 
                                "extraer_numeros","separar_valores", "normalizar_texto",
                                "reemplazar_valor","eliminar_duplicados", "rellenar_nulos", "filtrar","desdoblar"]
        
        if op in operaciones_limpieza:
            df_current.unpersist()
            DF_CACHE[df_id] = df_result.cache()
            response_data = dc.spark_df_to_api_response(df_result)
            
            # Log para limpieza
            registrar_log(client_ip, user, token_str, f"Limpieza: {op}", f"Col: {col}, Param: {param}")
            
            return {
                "status": "cleaned",
                "operation": op,
                "df_id": df_id,
                "rows_after": response_data["rows_count"],
                "new_columns": response_data["columns"],
                "data_preview": response_data["data_preview"]
            }

    except (ValueError, KeyError) as e:
        registrar_log(client_ip, user, token_str, f"ERROR: {op}", str(e))
        raise HTTPException(status_code=400, detail=f"Error en la solicitud: {e}")
    except Exception as e:
        registrar_log(client_ip, user, token_str, f"CRITICAL_ERROR: {op}", str(e))
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")


@app.post("/save_cleaned_data")
async def save_cleaned_data(
    request_log: Request, # Inyectamos Request para los logs
    request: SaveRequest,
    user: str = Depends(get_current_user)
    ):
    """
    Recibe el df_id, busca el DF en caché y lo guarda en disco usando Spark.
    """
    df_id = request.df_id
    
    if df_id not in DF_CACHE:
        raise HTTPException(status_code=404, detail=f"DataFrame con ID '{df_id}' no encontrado o expirado.")
        
    df_current = DF_CACHE[df_id]
    
    # --- EXTRAER TOKEN E IP ---
    auth_header = request_log.headers.get("Authorization")
    token_str = auth_header.split(" ")[1] if auth_header and " " in auth_header else "N/A"
    client_ip = request_log.client.host

    try:
        # 1. Extraer el nombre base y la extensión
        filename = request.filename
        parts = filename.split('.')
        nombre_base = ".".join(parts[:-1]) 
        ext = ".csv" 
        
        # 2. Llamar a la función pura de guardado de Spark
        success, message = dc.guardar_dataframe_spark(df_current, nombre_base, ext)

        if success:
            # --- REGISTRAR LOG DE ÉXITO ---
            registrar_log(
                ip=client_ip,
                usuario=user,
                token=token_str,
                accion="Guardado Exitoso",
                detalles=f"Archivo: {filename}, Path: {message}"
            )

            # 3. Liberar el DF de la caché después de guardarlo exitosamente
            df_current.unpersist()
            del DF_CACHE[df_id]
            
            return {
                "status": "saved",
                "message": f"Archivo (directorio) guardado exitosamente en: {message}",
                "path": message
            }
        else:
            raise ValueError(f"Error al guardar: {message}")

    except Exception as e:
        # --- REGISTRAR LOG DE ERROR ---
        registrar_log(
            ip=client_ip,
            usuario=user,
            token=token_str,
            accion="Error al Guardar",
            detalles=str(e)
        )
        raise HTTPException(status_code=500, detail=f"Error al guardar el archivo: {e}")
