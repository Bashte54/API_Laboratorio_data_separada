from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, status, Request
from pydantic import BaseModel
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from passlib.context import CryptContext
from datetime import datetime, timedelta
from pathlib import Path as FilePath
from typing import Dict, Any, Union
from io import BytesIO
import uuid
import csv
import pandas as pd
import numpy as np
import dataClean_api as dc_pandas
import dataCleanSpark as dc_spark

app = FastAPI(title="Data Cleaning API (Pandas + Spark)", version="2.0.0")

# ===================== SEGURIDAD (Compartida) =====================
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

# Caché separada para cada motor
PANDAS_DF_CACHE: Dict[str, pd.DataFrame] = {}
SPARK_DF_CACHE: Dict[str, dc_spark.DataFrame] = {}


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
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token inválido o expirado")


def registrar_log(ip: str, usuario: str, token: str, accion: str, detalles: str = ""):
    base_dir = FilePath(__file__).resolve().parent.parent
    archivo_log = base_dir / "LOGS" / "Logs_api_combined.csv"
    archivo_log.parent.mkdir(parents=True, exist_ok=True)
    ahora = datetime.now()
    with open(archivo_log, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            ahora.strftime("%Y-%m-%d"),
            ahora.strftime("%H:%M:%S"),
            ip, usuario, token, accion, detalles
        ])


def get_pandas_df(df_id: str) -> pd.DataFrame:
    if df_id not in PANDAS_DF_CACHE:
        raise HTTPException(status_code=404, detail=f"DataFrame Pandas '{df_id}' no encontrado.")
    return PANDAS_DF_CACHE[df_id]


def get_spark_df(df_id: str) -> dc_spark.DataFrame:
    if df_id not in SPARK_DF_CACHE:
        raise HTTPException(status_code=404, detail=f"DataFrame Spark '{df_id}' no encontrado.")
    return SPARK_DF_CACHE[df_id]


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


# ===================== PANDAS ENDPOINTS =====================

@app.post("/pandas/upload")
async def pandas_upload(
    request: Request,
    file: UploadFile = File(...),
    user: str = Depends(get_current_user)
):
    try:
        contents = await file.read()
        df = dc_pandas.read_file_from_buffer(BytesIO(contents), file.filename)
        df_id = str(uuid.uuid4())
        PANDAS_DF_CACHE[df_id] = df

        auth_header = request.headers.get("Authorization")
        token_str = auth_header.split(" ")[1] if auth_header else "N/A"
        registrar_log(request.client.host, user, token_str, "PANDAS: upload", file.filename)

        return {
            "df_id": df_id,
            "filename": file.filename,
            "rows": len(df),
            "columns": list(df.columns),
            "head": df.head(5).replace({np.nan: None}).to_dict(orient="records")
        }
    except (IOError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pandas/{df_id}/eliminar_nulos/{column_name}")
async def pandas_eliminar_nulos(
    df_id: str,
    column_name: str,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_pandas_df(df_id)
    df_result = dc_pandas.eliminar_nulos_api(df, column_name)
    PANDAS_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "PANDAS: eliminar_nulos", f"{df_id} col:{column_name}")
    return {
        "status": "cleaned", "operation": "eliminar_nulos", "df_id": df_id,
        "rows_after": len(df_result), "columns": list(df_result.columns),
        "preview": df_result.head(10).replace({np.nan: None}).to_dict(orient="records")
    }


@app.post("/pandas/{df_id}/eliminar_columna/{column_name}")
async def pandas_eliminar_columna(
    df_id: str,
    column_name: str,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_pandas_df(df_id)
    df_result = dc_pandas.eliminar_columna_api(df, column_name)
    PANDAS_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "PANDAS: eliminar_columna", f"{df_id} col:{column_name}")
    return {
        "status": "cleaned", "operation": "eliminar_columna", "df_id": df_id,
        "rows_after": len(df_result), "columns": list(df_result.columns),
        "preview": df_result.head(10).replace({np.nan: None}).to_dict(orient="records")
    }


class TransformarRequest(BaseModel):
    tipo: str


@app.post("/pandas/{df_id}/transformar/{column_name}")
async def pandas_transformar(
    df_id: str,
    column_name: str,
    body: TransformarRequest,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_pandas_df(df_id)
    df_result = dc_pandas.transformar_columna_api(df, column_name, body.tipo)
    PANDAS_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "PANDAS: transformar", f"{df_id} col:{column_name} tipo:{body.tipo}")
    return {
        "status": "cleaned", "operation": "transformar", "df_id": df_id,
        "rows_after": len(df_result), "columns": list(df_result.columns),
        "preview": df_result.head(10).replace({np.nan: None}).to_dict(orient="records")
    }


class RenombrarRequest(BaseModel):
    nuevo_nombre: str


@app.post("/pandas/{df_id}/renombrar/{column_name}")
async def pandas_renombrar(
    df_id: str,
    column_name: str,
    body: RenombrarRequest,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_pandas_df(df_id)
    df_result = dc_pandas.renombrar_columna_api(df, column_name, body.nuevo_nombre)
    PANDAS_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "PANDAS: renombrar", f"{df_id} col:{column_name}")
    return {
        "status": "cleaned", "operation": "renombrar", "df_id": df_id,
        "rows_after": len(df_result), "columns": list(df_result.columns),
        "preview": df_result.head(10).replace({np.nan: None}).to_dict(orient="records")
    }


@app.post("/pandas/{df_id}/extraer_numeros/{column_name}")
async def pandas_extraer_numeros(
    df_id: str,
    column_name: str,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_pandas_df(df_id)
    df_result = dc_pandas.extraer_numeros_api(df, column_name)
    PANDAS_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "PANDAS: extraer_numeros", f"{df_id} col:{column_name}")
    return {
        "status": "cleaned", "operation": "extraer_numeros", "df_id": df_id,
        "rows_after": len(df_result), "columns": list(df_result.columns),
        "preview": df_result.head(10).replace({np.nan: None}).to_dict(orient="records")
    }


class SepararRequest(BaseModel):
    separador: str


@app.post("/pandas/{df_id}/separar_valores/{column_name}")
async def pandas_separar_valores(
    df_id: str,
    column_name: str,
    body: SepararRequest,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_pandas_df(df_id)
    df_result = dc_pandas.separar_valores_api(df, column_name, body.separador, f"{column_name}_new")
    PANDAS_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "PANDAS: separar_valores", f"{df_id} col:{column_name}")
    return {
        "status": "cleaned", "operation": "separar_valores", "df_id": df_id,
        "rows_after": len(df_result), "columns": list(df_result.columns),
        "preview": df_result.head(10).replace({np.nan: None}).to_dict(orient="records")
    }


# Análisis Pandas
@app.get("/pandas/{df_id}/tipo_datos/{column_name}")
async def pandas_tipo_datos(df_id: str, column_name: str, user: str = Depends(get_current_user)):
    df = get_pandas_df(df_id)
    return {"status": "analysis", "operation": "tipo_datos", "results": dc_pandas.tipo_datos_api(df, column_name)}


@app.get("/pandas/{df_id}/cantidad_nulos/{column_name}")
async def pandas_cantidad_nulos(df_id: str, column_name: str, user: str = Depends(get_current_user)):
    df = get_pandas_df(df_id)
    return {"status": "analysis", "operation": "cantidad_nulos", "results": dc_pandas.cantidad_nulos_api(df, column_name)}


@app.get("/pandas/{df_id}/detectar_patrones/{column_name}")
async def pandas_detectar_patrones(df_id: str, column_name: str, user: str = Depends(get_current_user)):
    df = get_pandas_df(df_id)
    return {"status": "analysis", "operation": "detectar_patrones", "results": dc_pandas.detectar_patrones_api(df, column_name)}


@app.get("/pandas/{df_id}/correlaciones")
async def pandas_correlaciones(df_id: str, user: str = Depends(get_current_user)):
    df = get_pandas_df(df_id)
    return {"status": "analysis", "operation": "correlaciones", "results": dc_pandas.correlaciones_api(df)}


class SaveRequest(BaseModel):
    filename: str


@app.post("/pandas/{df_id}/save")
async def pandas_save(
    df_id: str,
    body: SaveRequest,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_pandas_df(df_id)
    parts = body.filename.split('.')
    nombre_base = ".".join(parts[:-1])
    ext = f".{parts[-1]}"
    success, message = dc_pandas.guardar_dataframe_api(df, nombre_base, ext)
    if success:
        registrar_log(request.client.host, user, "N/A", "PANDAS: save", f"{df_id} -> {message}")
        del PANDAS_DF_CACHE[df_id]
        return {"status": "saved", "path": message, "df_id": df_id}
    raise HTTPException(status_code=500, detail=message)


# ===================== SPARK ENDPOINTS =====================

@app.post("/spark/upload")
async def spark_upload(
    request: Request,
    file: UploadFile = File(...),
    user: str = Depends(get_current_user)
):
    try:
        contents = await file.read()
        file_buffer = BytesIO(contents)
        
        df_spark = dc_spark.read_file_from_buffer_spark(file_buffer, file.filename)
        df_id = str(uuid.uuid4())
        SPARK_DF_CACHE[df_id] = df_spark.cache()

        auth_header = request.headers.get("Authorization")
        token_str = auth_header.split(" ")[1] if auth_header else "N/A"
        registrar_log(request.client.host, user, token_str, "SPARK: upload", file.filename)

        response_data = dc_spark.spark_df_to_api_response(df_spark)

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


@app.post("/spark/{df_id}/eliminar_nulos/{column_name}")
async def spark_eliminar_nulos(
    df_id: str,
    column_name: str,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_spark_df(df_id)
    df_result = dc_spark.eliminar_nulos_spark(df, column_name)
    SPARK_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: eliminar_nulos", f"{df_id} col:{column_name}")
    
    response_data = dc_spark.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", "operation": "eliminar_nulos", "df_id": df_id,
        "rows_after": response_data["rows_count"], "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


@app.post("/spark/{df_id}/eliminar_columna/{column_name}")
async def spark_eliminar_columna(
    df_id: str,
    column_name: str,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_spark_df(df_id)
    df_result = dc_spark.eliminar_columna_spark(df, column_name)
    SPARK_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: eliminar_columna", f"{df_id} col:{column_name}")
    
    response_data = dc_spark.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", "operation": "eliminar_columna", "df_id": df_id,
        "rows_after": response_data["rows_count"], "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


@app.post("/spark/{df_id}/transformar/{column_name}")
async def spark_transformar(
    df_id: str,
    column_name: str,
    body: TransformarRequest,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_spark_df(df_id)
    df_result = dc_spark.transformar_columna_spark(df, column_name, body.tipo)
    SPARK_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: transformar", f"{df_id} col:{column_name} tipo:{body.tipo}")
    
    response_data = dc_spark.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", "operation": "transformar", "df_id": df_id,
        "rows_after": response_data["rows_count"], "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


@app.post("/spark/{df_id}/renombrar/{column_name}")
async def spark_renombrar(
    df_id: str,
    column_name: str,
    body: RenombrarRequest,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_spark_df(df_id)
    df_result = dc_spark.renombrar_columna_spark(df, column_name, body.nuevo_nombre)
    SPARK_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: renombrar", f"{df_id} col:{column_name} -> {body.nuevo_nombre}")
    
    response_data = dc_spark.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", "operation": "renombrar", "df_id": df_id,
        "rows_after": response_data["rows_count"], "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


@app.post("/spark/{df_id}/extraer_numeros/{column_name}")
async def spark_extraer_numeros(
    df_id: str,
    column_name: str,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_spark_df(df_id)
    df_result = dc_spark.extraer_numeros_spark(df, column_name)
    SPARK_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: extraer_numeros", f"{df_id} col:{column_name}")
    
    response_data = dc_spark.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", "operation": "extraer_numeros", "df_id": df_id,
        "rows_after": response_data["rows_count"], "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


@app.post("/spark/{df_id}/separar_valores/{column_name}")
async def spark_separar_valores(
    df_id: str,
    column_name: str,
    body: SepararRequest,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_spark_df(df_id)
    df_result = dc_spark.separar_valores_spark(df, column_name, body.separador, f"{column_name}_new")
    SPARK_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: separar_valores", f"{df_id} col:{column_name} sep:{body.separador}")
    
    response_data = dc_spark.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", "operation": "separar_valores", "df_id": df_id,
        "rows_after": response_data["rows_count"], "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


@app.post("/spark/{df_id}/normalizar_texto/{column_name}")
async def spark_normalizar_texto(
    df_id: str,
    column_name: str,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_spark_df(df_id)
    df_result = dc_spark.normalizar_texto_spark(df, column_name)
    SPARK_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: normalizar_texto", f"{df_id} col:{column_name}")
    
    response_data = dc_spark.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", "operation": "normalizar_texto", "df_id": df_id,
        "rows_after": response_data["rows_count"], "columns": response_data["columns"],
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
    df = get_spark_df(df_id)
    df_result = dc_spark.remplazar_valor_spark(df, column_name, body.viejo_valor, body.nuevo_valor)
    SPARK_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: reemplazar_valor", f"{df_id} col:{column_name} {body.viejo_valor}->{body.nuevo_valor}")
    
    response_data = dc_spark.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", "operation": "reemplazar_valor", "df_id": df_id,
        "rows_after": response_data["rows_count"], "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


@app.post("/spark/{df_id}/eliminar_duplicados")
async def spark_eliminar_duplicados(
    df_id: str,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_spark_df(df_id)
    df_result = dc_spark.eliminar_duplicados_spark(df)
    SPARK_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: eliminar_duplicados", f"{df_id}")
    
    response_data = dc_spark.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", "operation": "eliminar_duplicados", "df_id": df_id,
        "rows_after": response_data["rows_count"], "columns": response_data["columns"],
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
    df = get_spark_df(df_id)
    df_result = dc_spark.rellenar_valores_spark(df, column_name, body.valor)
    SPARK_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: rellenar_nulos", f"{df_id} col:{column_name} valor:{body.valor}")
    
    response_data = dc_spark.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", "operation": "rellenar_nulos", "df_id": df_id,
        "rows_after": response_data["rows_count"], "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


@app.post("/spark/{df_id}/filtrar")
async def spark_filtrar(
    df_id: str,
    condicion: str,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_spark_df(df_id)
    df_result = dc_spark.filtrar_datos_spark(df, condicion)
    SPARK_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: filtrar", f"{df_id} condicion:{condicion}")
    
    response_data = dc_spark.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", "operation": "filtrar", "df_id": df_id,
        "rows_after": response_data["rows_count"], "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


@app.post("/spark/{df_id}/desdoblar/{column_name}")
async def spark_desdoblar(
    df_id: str,
    column_name: str,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_spark_df(df_id)
    df_result = dc_spark.desdoblar_columna_spark(df, column_name)
    SPARK_DF_CACHE[df_id] = df_result
    registrar_log(request.client.host, user, "N/A", "SPARK: desdoblar", f"{df_id} col:{column_name}")
    
    response_data = dc_spark.spark_df_to_api_response(df_result)
    return {
        "status": "cleaned", "operation": "desdoblar", "df_id": df_id,
        "rows_after": response_data["rows_count"], "columns": response_data["columns"],
        "preview": response_data["data_preview"]
    }


# Análisis Spark
@app.get("/spark/{df_id}/tipo_datos/{column_name}")
async def spark_tipo_datos(df_id: str, column_name: str, user: str = Depends(get_current_user)):
    df = get_spark_df(df_id)
    results = dc_spark.tipo_datos_spark(df, column_name)
    return {"status": "analysis", "operation": "tipo_datos", "results": results}


@app.get("/spark/{df_id}/cantidad_nulos/{column_name}")
async def spark_cantidad_nulos(df_id: str, column_name: str, user: str = Depends(get_current_user)):
    df = get_spark_df(df_id)
    results = dc_spark.cantidad_nulos_spark(df, column_name)
    return {"status": "analysis", "operation": "cantidad_nulos", "results": results}


@app.get("/spark/{df_id}/nulos_totales")
async def spark_nulos_totales(df_id: str, user: str = Depends(get_current_user)):
    df = get_spark_df(df_id)
    results = dc_spark.cantidad_nulos_total_spark(df)
    return {"status": "analysis", "operation": "nulos_totales", "results": results}


@app.get("/spark/{df_id}/detectar_patrones/{column_name}")
async def spark_detectar_patrones(df_id: str, column_name: str, user: str = Depends(get_current_user)):
    df = get_spark_df(df_id)
    results = dc_spark.detectar_patrones_spark(df, column_name)
    return {"status": "analysis", "operation": "detectar_patrones", "results": results}


@app.get("/spark/{df_id}/correlaciones")
async def spark_correlaciones(df_id: str, user: str = Depends(get_current_user)):
    df = get_spark_df(df_id)
    results = dc_spark.correlaciones_spark(df)
    return {"status": "analysis", "operation": "correlaciones", "results": results}


@app.post("/spark/{df_id}/save")
async def spark_save(
    df_id: str,
    body: SaveRequest,
    request: Request,
    user: str = Depends(get_current_user)
):
    df = get_spark_df(df_id)
    
    parts = body.filename.split('.')
    nombre_base = ".".join(parts[:-1])
    ext = f".{parts[-1]}" if len(parts) > 1 else ".csv"
    
    success, message = dc_spark.guardar_dataframe_spark(df, nombre_base, ext)
    
    if success:
        registrar_log(request.client.host, user, "N/A", "SPARK: save", f"{df_id} -> {message}")
        df.unpersist()
        del SPARK_DF_CACHE[df_id]
        return {"status": "saved", "path": message, "df_id": df_id}
    
    raise HTTPException(status_code=500, detail=message)
