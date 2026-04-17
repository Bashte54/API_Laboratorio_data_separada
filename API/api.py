# api.py (EL NUEVO ORQUESTADOR API)

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pandas as pd
from io import BytesIO, StringIO
from typing import Dict, Any, List
import numpy as np
import csv

import uuid

# ====================================
from fastapi import Depends, status, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from passlib.context import CryptContext
from datetime import datetime, timedelta


import dataClean_api as dc 

app = FastAPI(title="Data Cleaning API", version="1.0.0")

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
    if not user:
        return False
    if not verify_password(password, user["hashed_password"]):
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
    
#funcion para lo logs
def registrar_log(ip: str, usuario: str, token: str, accion: str, detalles: str = ""):
    # Puedes usar el mismo archivo de logs o uno diferente como "Logs_apiPandas.csv"
    archivo_log = "/home/papime2442/Descargas/cleanDataToolv2/LOGS/Logs_api.csv"
    ahora = datetime.now()
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

# Diccionario global para guardar los DataFrames en la RAM
DF_CACHE = {}
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


# =========================================================
# 1. MODELOS DE DATOS (Pydantic)
# =========================================================

# Modelo para enviar el DataFrame a través del cuerpo de la solicitud JSON
class DataFrameData(BaseModel):
    data: List[Dict[str, Any]] # Lista de registros (filas)
    columns: List[str]

# Modelo para la solicitud de limpieza en una columna (Sustituye al "Menú Interactivo")
class OperationRequest(BaseModel):
    df_id: str
    column_name: str
    operation: str # Ejemplos: "eliminar_nulos", "transformar_int", "renombrar"
    param: str | None = None # Usado para tipos (int, float) o nuevo nombre

# 2. ENDPOINTS DE ORQUESTACIÓN
@app.post("/upload_and_preview")
async def upload_and_preview(
    request: Request,
    file: UploadFile = File(...), 
    user: str = Depends(get_current_user)
    ):
    try:
        contents = await file.read()
        file_buffer = BytesIO(contents)
        df = dc.read_file_from_buffer(file_buffer, file.filename)
        
        # --- NUEVA LÓGICA DE CACHÉ ---
        df_id = str(uuid.uuid4())
        DF_CACHE[df_id] = df  # Guardamos el objeto DF original en la memoria
        # -----------------------------

        # Convertir NaN a None solo para la vista previa (JSON)
        df_preview = df.head(5).replace({np.nan: None})

        ### registros de los logs
        auth_header = request.headers.get("Authorization")
        token_str = auth_header.split(" ")[1] if auth_header and " " in auth_header else "N/A"
        
        registrar_log(
            ip=request.client.host,
            usuario=user,
            token=token_str,
            accion="PANDAS: Subida de Archivo",
            detalles=f"Archivo: {file.filename}, ID: {df_id}"
        )
        
        return {
            "df_id": df_id, # <--- Ahora devolvemos el ID
            "filename": file.filename,
            "rows": len(df),
            "columns": list(df.columns),
            "head": df_preview.to_dict(orient="records")
        }
        
    except (IOError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Error de procesamiento: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")

@app.post("/apply_operation")
async def apply_operation(
    request_log: Request,
    request: OperationRequest,
    user: str = Depends(get_current_user)
    ):
    # --- EXTRAER TOKEN E IP ---
    auth_header = request_log.headers.get("Authorization")
    token_str = auth_header.split(" ")[1] if auth_header and " " in auth_header else "N/A"
    client_ip = request_log.client.host

    try:
        # 1. Buscar en la Caché de la ThinkPad
        df_id = request.df_id
        if df_id not in DF_CACHE:
            raise HTTPException(status_code=404, detail="ID de DataFrame no encontrado.")
            
        df = DF_CACHE[df_id] # Obtenemos el DF de la RAM
        col = request.column_name
        op = request.operation
        param = request.param

        # 2. Orquestador
        # --- OPERACIONES DE LIMPIEZA ---
        if op == "eliminar_nulos":
            df_result = dc.eliminar_nulos_api(df, col)
        elif op == "eliminar_columna":
            df_result = dc.eliminar_columna_api(df, col)
        elif op == "transformar":
            if not param: raise ValueError("Parámetro 'tipo' requerido.")
            df_result = dc.transformar_columna_api(df, col, param)
        elif op == "renombrar":
            if not param: raise ValueError("Parámetro 'nuevo nombre' requerido.")
            df_result = dc.renombrar_columna_api(df, col, param)
        elif op == "extraer_numeros":
            df_result = dc.extraer_numeros_api(df, col)
        elif op == "separar_valores":
            if not param: raise ValueError("Parámetro 'separador' requerido.")
            df_result = dc.separar_valores_api(df, col, separador=param, nuevo_nombre=f"{col}_new")

        # --- OPERACIONES DE ANÁLISIS ---
        elif op in ["tipo_datos", "cantidad_nulos", "detectar_patrones", "correlaciones"]:
            if op == "tipo_datos":
                results = dc.tipo_datos_api(df, col)
            elif op == "cantidad_nulos":
                results = dc.cantidad_nulos_api(df, col)
            elif op == "detectar_patrones":
                results = dc.detectar_patrones_api(df, col)
            elif op == "correlaciones":
                results = dc.correlaciones_api(df)

            registrar_log(client_ip, user, token_str, f"PANDAS_ANALISIS: {op}", f"ID: {df_id}, Col: {col}")
            return {"status": "analysis", "operation": op, "results": results}
        
        else:
            raise ValueError(f"Operación '{op}' no reconocida.")

        # 3. PERSISTENCIA EN CACHÉ Y RESPUESTA
        # Actualizamos el DataFrame en la memoria con los nuevos cambios
        DF_CACHE[df_id] = df_result 
        
        registrar_log(client_ip, user, token_str, f"PANDAS_LIMPIEZA: {op}", f"ID: {df_id}, Col: {col}")
        
        return {
            "status": "cleaned",
            "operation": op,
            "df_id": df_id,
            "rows_after": len(df_result),
            "new_columns": list(df_result.columns),
            "data_preview": df_result.head(10).replace({np.nan: None}).to_dict(orient="records")
        }

    except Exception as e:
        registrar_log(client_ip, user, token_str, f"PANDAS_ERROR: {op}", str(e))
        raise HTTPException(status_code=500, detail=str(e))

# Modelo para la solicitud de guardado
class SaveRequest(BaseModel):
    df_id: str
    filename: str 
    
@app.post("/save_cleaned_data")
async def save_cleaned_data(
    request_log: Request,
    request: SaveRequest,
    user: str = Depends(get_current_user)
    ):
    """
    Busca el DataFrame en caché por su ID y lo guarda físicamente.
    """
    # --- EXTRAER TOKEN E IP ---
    auth_header = request_log.headers.get("Authorization")
    token_str = auth_header.split(" ")[1] if auth_header and " " in auth_header else "N/A"
    client_ip = request_log.client.host

    try:
        # 1. Recuperar desde la Caché de la RAM
        df_id = request.df_id
        if df_id not in DF_CACHE:
            raise HTTPException(status_code=404, detail="ID de DataFrame no encontrado o expirado.")
        
        df = DF_CACHE[df_id]
        
        # 2. Extraer nombre y extensión
        filename = request.filename
        parts = filename.split('.')
        nombre_base = ".".join(parts[:-1])
        ext = f".{parts[-1]}"
        
        # 3. Guardado físico usando la función pura
        success, message = dc.guardar_dataframe_api(df, nombre_base, ext)

        if success:
            # === LOG DE GUARDADO EXITOSO ===
            registrar_log(
                ip=client_ip,
                usuario=user,
                token=token_str,
                accion="PANDAS_GUARDADO: Exito",
                detalles=f"ID: {df_id}, Archivo: {filename}, Path: {message}"
            )

            return {
                "status": "saved",
                "message": f"Archivo guardado exitosamente en: {message}",
                "path": message,
                "df_id": df_id
            }
        else:
            raise ValueError(f"Error al guardar: {message}")

    except Exception as e:
        # === LOG DE ERROR EN GUARDADO ===
        registrar_log(
            ip=client_ip,
            usuario=user,
            token=token_str,
            accion="PANDAS_ERROR: Guardado Fallido",
            detalles=str(e)
        )
        raise HTTPException(status_code=500, detail=f"Error al guardar el archivo: {e}")
