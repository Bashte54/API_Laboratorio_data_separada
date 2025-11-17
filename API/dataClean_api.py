# dataCleanV3_API.py
import pandas as pd 
import numpy as np 
import re
from io import BytesIO, StringIO
from datetime import datetime
from pathlib import Path
import csv
from typing import Union, Dict, Any, Tuple

# =========================================================================
# 1. FUNCIÓN DE LECTURA (API-FRIENDLY)
# =========================================================================

def read_file_from_buffer(file_buffer: Union[BytesIO, StringIO], filename: str) -> pd.DataFrame:
    """
    Lee un archivo desde un buffer de memoria (recibido por FastAPI).
    """
    ext = filename.split('.')[-1].lower()
    
    try:
        if ext == "csv":
            # Intentar detectar el delimitador (se puede simplificar si se asume CSV estándar)
            file_buffer.seek(0)
            sample = file_buffer.read(4096).decode('utf-8', errors='ignore')
            
            try:
                # Intenta usar sniffer
                dialect = csv.Sniffer().sniff(sample, delimiters=[',', ';', '|', ':', '\t'])
                sep_detected = dialect.delimiter
            except Exception:
                # Fallback al delimitador más común
                sep_detected = ','
                
            file_buffer.seek(0)
            df = pd.read_csv(file_buffer, sep=sep_detected)

        elif ext == "json":
            file_buffer.seek(0)
            df = pd.read_json(file_buffer)

        elif ext in ["xls", "xlsx"]:
            file_buffer.seek(0)
            df = pd.read_excel(file_buffer)

        else:
            raise ValueError(f"Formato no soportado: .{ext}")

        return df

    except Exception as e:
        # Relanzamos el error para que FastAPI lo maneje
        raise IOError(f"Error al leer el archivo: {e}")

# =========================================================================
# 2. FUNCIONES DE LIMPIEZA (Puras - Devuelven DF modificado)
# =========================================================================

def eliminar_columna_api(df: pd.DataFrame, columna: str) -> pd.DataFrame:
    if columna not in df.columns:
        return df.copy() # No se puede eliminar, devuelve el original
    return df.drop(columns=[columna]).copy()

def transformar_columna_api(df: pd.DataFrame, columna: str, tipo: str) -> pd.DataFrame:
    """Tipos: 'int', 'float', 'str', 'bool', 'date'"""
    df_new = df.copy()
    
    try:
        if tipo == "date":
            df_new[columna] = pd.to_datetime(df_new[columna], errors='coerce')
        else:
            df_new[columna] = df_new[columna].astype(tipo)
            
    except Exception as e:
        raise ValueError(f"Error al convertir la columna a {tipo}: {e}")
        
    return df_new

def eliminar_nulos_api(df: pd.DataFrame, columna: str) -> pd.DataFrame:
    if columna not in df.columns:
        return df.copy()
    # Devuelve el nuevo DF sin las filas con nulos en la columna
    return df.dropna(subset=[columna]).copy()

def renombrar_columna_api(df: pd.DataFrame, columna_vieja: str, columna_nueva: str) -> pd.DataFrame:
    if columna_vieja not in df.columns:
        return df.copy()
    return df.rename(columns={columna_vieja: columna_nueva}).copy()

def extraer_numeros_api(df: pd.DataFrame, columna: str) -> pd.DataFrame:
    df_new = df.copy()
    
    def limpiar_valor(valor):
        valor = str(valor)
        match = re.search(r'(\d+(?:\.\d+)?)', valor)
        if match:
            num = float(match.group(1))
            if '%' in valor:
                num = num / 100
            return num
        return np.nan

    df_new[columna] = df_new[columna].apply(limpiar_valor)
    return df_new


def separar_valores_api(df: pd.DataFrame, columna: str, separador: str, nuevo_nombre: str) -> pd.DataFrame:
    if not df[columna].astype(str).str.contains(separador).any():
        raise ValueError(f"No se encontró el separador '{separador}' en la columna.")

    df_new = df.copy()
    
    # Dividimos y renombramos
    split_cols = df_new[columna].astype(str).str.split(separador, n=1, expand=True)
    
    # Si la división funciona, asignamos y eliminamos la temporal
    if split_cols.shape[1] == 2:
        df_new[columna] = split_cols[0]  # Parte izquierda reemplaza a la original
        df_new[nuevo_nombre] = split_cols[1] # Parte derecha en la nueva columna
    else:
        raise ValueError("Error al separar: el separador no produce dos partes.")
        
    return df_new

# =========================================================================
# 3. FUNCIONES DE ANÁLISIS (Puras - Devuelven un diccionario de resultados)
# =========================================================================

def tipo_datos_api(df: pd.DataFrame, columna: str) -> Dict[str, int]:
    if columna not in df.columns:
        raise ValueError(f"Columna '{columna}' no encontrada.")
        
    tipo_map = {
        int: "int", float: "float", str: "str", bool: "bool", type(None): "NoneType"
    }

    tipos = df[columna].apply(lambda x: tipo_map.get(type(x), str(type(x)))).value_counts()
    return tipos.to_dict()


def cantidad_nulos_api(df: pd.DataFrame, columna: str) -> int:
    if columna not in df.columns:
        raise ValueError(f"Columna '{columna}' no encontrada.")
    return int(df[columna].isnull().sum())


def detectar_patrones_api(df: pd.DataFrame, columna: str) -> Dict[str, Any]:
    if columna not in df.columns:
        raise ValueError(f"Columna '{columna}' no encontrada.")
        
    top_values = df[columna].value_counts().head(10)
    
    return {
        "unique_count": df[columna].nunique(),
        "total_count": len(df),
        "frequent_values": top_values.to_dict() 
    }


def correlaciones_api(df: pd.DataFrame) -> Dict[str, Any]:
    numeric_df = df.select_dtypes(include=[np.number])
    if numeric_df.empty:
        return {"error": "No hay columnas numéricas para correlación."}
    
    # Devuelve la matriz de correlación como diccionario de diccionarios
    return numeric_df.corr().to_dict()

# =========================================================================
# 4. FUNCIÓN DE GUARDADO (Adaptación al Servidor)
# =========================================================================

def guardar_dataframe_api(df: pd.DataFrame, nombre_base: str, ext: str) -> Tuple[bool, str]:
    """
    Guarda el DataFrame en el servidor. Devuelve éxito y la ruta.
    """
    base_dir = Path("data_limpia")
    # ... Lógica de creación de carpetas (puede ser opcional en un microservicio)
    
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        if ext == ".csv":
            ruta_guardado = base_dir / f"csv_limpia/{nombre_base}_limpio_{fecha}.csv"
            df.to_csv(ruta_guardado, index=False)
        # ... Lógica para JSON y Excel
        else:
            return False, f"Extensión {ext} no implementada para guardado API."

        return True, str(ruta_guardado)

    except Exception as e:
        return False, str(e)