from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, status, Request
from pydantic import BaseModel
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from passlib.context import CryptContext
from datetime import datetime, timedelta
from pathlib import Path as FilePath
from typing import Dict, Any
from io import BytesIO
import uuid
import csv

import dataCleanSpark as dc

app = FastAPI(title="Spark Data Cleaning API", version="2.0.0 (Spark Edition)")

# ===================== SEGURIDAD =====================
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

# Caché para DataFrames de Spark
DF_CACHE: Dict[str, dc.DataFrame] = {}

def verify_password(plain, hashed):
    return pwd_context.verify(plain, hashed)

def authenticate_user(username: str, password: str):
    user = users.get(username)
    if not user or not verify_password(password, user["hashed_password"]):
        return False
    return user

def create_access_token(data: dict, expires_delta: timedelta):
    to_encode = data.copy()
    to_encode.update({"exp": datetime.utcnow() + expires_delta})
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

def registrar_log(ip: str, usuario: str, token: str, accion: str, detalles: str = ""):
    base_dir = FilePath(__file__).resolve().parent.parent
    archivo_log = base_dir / "LOGS" / "Logs_apiSpark.csv"
    archivo_log.parent.mkdir(parents=True, exist_ok=True)
    ahora = datetime.now()
    with open(archivo_log, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            ahora.strftime("%Y-%m-%d"),
            ahora.strftime("%H:%M:%S"),
            ip, usuario, token, accion, detalles
        ])

def get_df(df_id: str) -> dc.DataFrame:
    if df_id not in DF_CACHE:
        raise HTTPException(status_code=404, detail=f"DataFrame '{df_id}' no encontrado o expirado.")
    return DF_CACHE[df_id]

# ===================== LOGIN =====================
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

# ===================== SPARK UPLOAD =====================
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
        file_buffer = BytesIO(contents)
        
        df_spark = dc.read_file_from_buffer_spark(file_buffer, file.filename)
        df_id = str(uuid.uuid4())
        DF_CACHE[df_id] = df_spark.cache()

        auth_header = request.headers.get("Authorization")
        token_str = auth_header.split(" ")[1] if auth_header else "N/A"
        registrar_log(request.client.host, user, token_str, "SPARK: upload", file.filename)

        response_data = dc.spark_df_to_api_response(df_spark)

        return {
            "df_id": df_id,
            "filename": file.filename,
            "rows": response_data["rows_count"],
            "columns": response_data["columns"],
            "data_types": response_data["data_types"],
            "head": response_data["data_preview"]
        }
    except (IOError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ===================== OPERACIONES DE LIMPIEZA =====================

@app.post("/spark/{df_id}/eliminar_nulos/{column_name}")
async def spark_eliminar_nulos(
    df_id: str, 
    column_name: str, 
    request: Request, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    df_result = dc.eliminar_nulos_spark(df, column_name)
    DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: eliminar_nulos", f"{df_id} col:{column_name}")
    
    response_data = dc.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", 
        "operation": "eliminar_nulos", 
        "df_id": df_id,
        "rows_after": response_data["rows_count"], 
        "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


@app.post("/spark/{df_id}/eliminar_columna/{column_name}")
async def spark_eliminar_columna(
    df_id: str, 
    column_name: str, 
    request: Request, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    df_result = dc.eliminar_columna_spark(df, column_name)
    DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: eliminar_columna", f"{df_id} col:{column_name}")
    
    response_data = dc.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", 
        "operation": "eliminar_columna", 
        "df_id": df_id,
        "rows_after": response_data["rows_count"], 
        "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


class TransformarRequest(BaseModel):
    tipo: str  # int, float, str, bool, date


@app.post("/spark/{df_id}/transformar/{column_name}")
async def spark_transformar(
    df_id: str, 
    column_name: str, 
    body: TransformarRequest, 
    request: Request, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    df_result = dc.transformar_columna_spark(df, column_name, body.tipo)
    DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: transformar", f"{df_id} col:{column_name} tipo:{body.tipo}")
    
    response_data = dc.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", 
        "operation": "transformar", 
        "df_id": df_id,
        "rows_after": response_data["rows_count"], 
        "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


class RenombrarRequest(BaseModel):
    nuevo_nombre: str


@app.post("/spark/{df_id}/renombrar/{column_name}")
async def spark_renombrar(
    df_id: str, 
    column_name: str, 
    body: RenombrarRequest, 
    request: Request, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    df_result = dc.renombrar_columna_spark(df, column_name, body.nuevo_nombre)
    DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: renombrar", f"{df_id} col:{column_name} -> {body.nuevo_nombre}")
    
    response_data = dc.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", 
        "operation": "renombrar", 
        "df_id": df_id,
        "rows_after": response_data["rows_count"], 
        "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


@app.post("/spark/{df_id}/extraer_numeros/{column_name}")
async def spark_extraer_numeros(
    df_id: str, 
    column_name: str, 
    request: Request, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    df_result = dc.extraer_numeros_spark(df, column_name)
    DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: extraer_numeros", f"{df_id} col:{column_name}")
    
    response_data = dc.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", 
        "operation": "extraer_numeros", 
        "df_id": df_id,
        "rows_after": response_data["rows_count"], 
        "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


class SepararRequest(BaseModel):
    separador: str


@app.post("/spark/{df_id}/separar_valores/{column_name}")
async def spark_separar_valores(
    df_id: str, 
    column_name: str, 
    body: SepararRequest, 
    request: Request, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    df_result = dc.separar_valores_spark(df, column_name, body.separador, f"{column_name}_new")
    DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: separar_valores", f"{df_id} col:{column_name} sep:{body.separador}")
    
    response_data = dc.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", 
        "operation": "separar_valores", 
        "df_id": df_id,
        "rows_after": response_data["rows_count"], 
        "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


@app.post("/spark/{df_id}/normalizar_texto/{column_name}")
async def spark_normalizar_texto(
    df_id: str, 
    column_name: str, 
    request: Request, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    df_result = dc.normalizar_texto_spark(df, column_name)
    DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: normalizar_texto", f"{df_id} col:{column_name}")
    
    response_data = dc.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", 
        "operation": "normalizar_texto", 
        "df_id": df_id,
        "rows_after": response_data["rows_count"], 
        "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


class ReemplazarRequest(BaseModel):
    viejo_valor: str
    nuevo_valor: str


@app.post("/spark/{df_id}/reemplazar_valor/{column_name}")
async def spark_reemplazar_valor(
    df_id: str, 
    column_name: str, 
    body: ReemplazarRequest, 
    request: Request, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    df_result = dc.remplazar_valor_spark(df, column_name, body.viejo_valor, body.nuevo_valor)
    DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: reemplazar_valor", f"{df_id} col:{column_name} {body.viejo_valor}->{body.nuevo_valor}")
    
    response_data = dc.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", 
        "operation": "reemplazar_valor", 
        "df_id": df_id,
        "rows_after": response_data["rows_count"], 
        "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


@app.post("/spark/{df_id}/eliminar_duplicados")
async def spark_eliminar_duplicados(
    df_id: str, 
    request: Request, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    df_result = dc.eliminar_duplicados_spark(df)
    DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: eliminar_duplicados", f"{df_id}")
    
    response_data = dc.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", 
        "operation": "eliminar_duplicados", 
        "df_id": df_id,
        "rows_after": response_data["rows_count"], 
        "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


class RellenarRequest(BaseModel):
    valor: str


@app.post("/spark/{df_id}/rellenar_nulos/{column_name}")
async def spark_rellenar_nulos(
    df_id: str, 
    column_name: str, 
    body: RellenarRequest, 
    request: Request, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    df_result = dc.rellenar_valores_spark(df, column_name, body.valor)
    DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: rellenar_nulos", f"{df_id} col:{column_name} valor:{body.valor}")
    
    response_data = dc.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", 
        "operation": "rellenar_nulos", 
        "df_id": df_id,
        "rows_after": response_data["rows_count"], 
        "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


@app.post("/spark/{df_id}/filtrar")
async def spark_filtrar(
    df_id: str, 
    condicion: str, 
    request: Request, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    df_result = dc.filtrar_datos_spark(df, condicion)
    DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: filtrar", f"{df_id} condicion:{condicion}")
    
    response_data = dc.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", 
        "operation": "filtrar", 
        "df_id": df_id,
        "rows_after": response_data["rows_count"], 
        "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


@app.post("/spark/{df_id}/desdoblar/{column_name}")
async def spark_desdoblar(
    df_id: str, 
    column_name: str, 
    request: Request, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    df_result = dc.desdoblar_columna_spark(df, column_name)
    DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: desdoblar", f"{df_id} col:{column_name}")
    
    response_data = dc.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", 
        "operation": "desdoblar", 
        "df_id": df_id,
        "rows_after": response_data["rows_count"], 
        "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


# ===================== OPERACIONES DE ANÁLISIS =====================

@app.get("/spark/{df_id}/tipo_datos/{column_name}")
async def spark_tipo_datos(
    df_id: str, 
    column_name: str, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    results = dc.tipo_datos_spark(df, column_name)
    return {"status": "analysis", "operation": "tipo_datos", "results": results}


@app.get("/spark/{df_id}/cantidad_nulos/{column_name}")
async def spark_cantidad_nulos(
    df_id: str, 
    column_name: str, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    results = dc.cantidad_nulos_spark(df, column_name)
    return {"status": "analysis", "operation": "cantidad_nulos", "results": results}


@app.get("/spark/{df_id}/nulos_totales")
async def spark_nulos_totales(
    df_id: str, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    results = dc.cantidad_nulos_total_spark(df)
    return {"status": "analysis", "operation": "nulos_totales", "results": results}


@app.get("/spark/{df_id}/detectar_patrones/{column_name}")
async def spark_detectar_patrones(
    df_id: str, 
    column_name: str, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    results = dc.detectar_patrones_spark(df, column_name)
    return {"status": "analysis", "operation": "detectar_patrones", "results": results}


@app.get("/spark/{df_id}/correlaciones")
async def spark_correlaciones(
    df_id: str, 
    user: str = Depends(get_current_user)
):
    df = get_df(df_id)
    results = dc.correlaciones_spark(df)
    return {"status": "analysis", "operation": "correlaciones", "results": results}


# ===================== GUARDAR =====================

class SaveRequest(BaseModel):
    filename: str


@app.post("/spark/{df_id}/save")
async def spark_save(
    df_id: str, 
    body: SaveRequest, 
    request: Request, 
    user: str = Depends(get_current_user)
):
    """
    Recibe el df_id, busca el DF en caché y lo guarda en disco usando Spark.
    """
    df = get_df(df_id)
    
    parts = body.filename.split('.')
    nombre_base = ".".join(parts[:-1])
    ext = f".{parts[-1]}" if len(parts) > 1 else ".csv"
    
    success, message = dc.guardar_dataframe_spark(df, nombre_base, ext)
    
    if success:
        registrar_log(
            request.client.host, 
            user, 
            "N/A", 
            "SPARK: save", 
            f"{df_id} -> {message}"
        )
        # Liberar el DF de la caché después de guardarlo exitosamente
        df.unpersist()
        del DF_CACHE[df_id]
        return {"status": "saved", "path": message, "df_id": df_id}
    
    raise HTTPException(status_code=500, detail=message)
