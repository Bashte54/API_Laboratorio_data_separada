import pandas as pd 
import numpy as np 
from pathlib import Path
import os 
import tqdm
import csv
from rich.console import Console
from rich.table import Table
import re
from datetime import datetime
from tkinter import Tk, filedialog

console = Console()

# Funciones de selección de archivos

def read_subDir(base_path="."):
    base = Path(base_path)
    subdirs = [d for d in base.iterdir() if d.is_dir()]
    if not subdirs:
        console.print("[bold red]No se encontraron subcarpetas en el directorio base.[/bold red]")
        return None

    table = Table(title="Subcarpetas disponibles", show_lines=True)
    table.add_column("Número", justify="center", style="cyan")
    table.add_column("Nombre", style="green")
    for i, d in enumerate(subdirs, 1):
        table.add_row(str(i), d.name)
    console.print(table)

    while True:
        try:
            choice = int(input("Selecciona el número de la carpeta: "))
            if 1 <= choice <= len(subdirs):
                selected_dir = subdirs[choice - 1]
                console.print(f"\n[bold green] Carpeta seleccionada:[/bold green] {selected_dir.name}\n")
                return selected_dir
            else:
                console.print("[bold red] Número fuera de rango, intenta de nuevo.[/bold red]")
        except ValueError:
            console.print("[bold red] Entrada no válida, ingresa un número.[/bold red]")

def select_filesDir():
    selected_dir = read_subDir(".")
    if not selected_dir:
        return None

    files = [f for f in selected_dir.iterdir() if f.is_file()]
    if not files:
        console.print(f"[yellow] No hay archivos en {selected_dir.name}[/yellow]")
        return None

    table = Table(title=f"Archivos en '{selected_dir.name}'", show_lines=True)
    table.add_column("Número", justify="center", style="cyan")
    table.add_column("Archivo", style="magenta")
    for i, f in enumerate(files, 1):
        table.add_row(str(i), f.name)
    console.print(table)

    while True:
        try:
            choice = int(input("Selecciona el número del archivo (o 0 para salir): "))
            if choice == 0:
                console.print("[yellow]Saliendo...[/yellow]")
                return None
            if 1 <= choice <= len(files):
                selected_file = files[choice - 1]
                console.print(f"\n[bold green] Archivo seleccionado:[/bold green] {selected_file.name}")
                return selected_file
            else:
                console.print("[bold red] Número fuera de rango, intenta de nuevo.[/bold red]")
        except ValueError:
            console.print("[bold red] Entrada no válida, ingresa un número.[/bold red]") 


# Función para leer el archivo

def read_selected_file(selected_file):
    """
    Lee un archivo CSV, JSON o Excel con detección automática de delimitador
    y manejo robusto de errores.
    """

    ext = selected_file.suffix.lower()
    df = None

    try:
        if ext == ".csv":

            # =======================
            # 1. Intentar detectar delimitador
            # =======================
            try:
                with open(selected_file, 'r', encoding='utf-8', errors='ignore') as f:
                    sample = f.read(4096)
                    dialect = csv.Sniffer().sniff(sample, delimiters=[',', ';', '|', ':', '\t'])
                    sep_detected = dialect.delimiter
                console.print(f"[cyan]Delimitador detectado: '{sep_detected}'[/cyan]")
            except Exception:
                # Fallback cuando sniffer falla
                console.print("[yellow]No se pudo detectar el delimitador automáticamente. Probando delimitadores comunes...[/yellow]")
                possible_separators = [',', ';', '|', ':', '\t']
                best_sep = None
                max_cols = 1

                # =======================
                # Determinar mejor delimitador por número de columnas
                # =======================
                for sep in possible_separators:
                    try:
                        test_df = pd.read_csv(selected_file, sep=sep, nrows=5, engine='python')
                        if len(test_df.columns) > max_cols:
                            max_cols = len(test_df.columns)
                            best_sep = sep
                    except:
                        continue

                sep_detected = best_sep if best_sep else ','  
                console.print(f"[cyan]Delimitador seleccionado por fallback: '{sep_detected}'[/cyan]")

            # =======================
            # 2. Cargar el CSV real
            # =======================
            df = pd.read_csv(selected_file, sep=sep_detected, engine='python')

        elif ext == ".json":
            df = pd.read_json(selected_file)

        elif ext in [".xls", ".xlsx"]:
            df = pd.read_excel(selected_file)

        else:
            console.print(f"[red]Formato no soportado: {ext}[/red]")
            return None

    except Exception as e:
        console.print(f"[red]Error al leer el archivo: {e}[/red]")
        return None

    # Mostrar columnas
    table = Table(title=f"Columnas del archivo '{selected_file.name}'", show_lines=True)
    table.add_column("Número", justify="center", style="cyan")
    table.add_column("Nombre de columna", style="green")
    for i, col in enumerate(df.columns, 1):
        table.add_row(str(i), col)
    console.print(table)

    console.print(f"[green]DataFrame cargado con {len(df)} filas y {len(df.columns)} columnas[/green]")

    return df


# Funciones de limpieza por columna

def tipo_datos(df, columna):
    tipo_map = {
        int: "int",
        float: "float",
        str: "str",
        bool: "bool",
        type(None): "NoneType"
    }

    tipos = df[columna].apply(lambda x: tipo_map.get(type(x), str(type(x)))).value_counts()

    table = Table(title=f"Tipos de datos de la columna '{columna}'", show_lines=True)
    table.add_column("Tipo de dato", style="cyan")
    table.add_column("Cantidad", justify="center", style="green")

    for t, c in tipos.items():
        table.add_row(t, str(c))

    console.print(table)


def eliminar_columna(df, columna):
    df.drop(columns=[columna], inplace=True)
    console.print(f"[red]Columna '{columna}' eliminada[/red]")

def transformar_columna(df, columna):
    opciones = {"1":"int", "2":"float", "3":"str", "4":"bool", "5":"date"}
    console.print("Opciones de transformación: 1=int, 2=float, 3=str, 4=bool, 5=date")
    choice = input("Selecciona tipo de transformación: ")

    if choice in opciones:
        tipo = opciones[choice]
        try:
            if tipo == "date":
                df[columna] = pd.to_datetime(df[columna], errors='coerce')
                console.print(f"[green]Columna '{columna}' convertida a fecha (datetime)[/green]")
            else:
                df[columna] = df[columna].astype(tipo)
                console.print(f"[green]Columna '{columna}' convertida a {tipo}[/green]")
        except Exception as e:
            console.print(f"[red]Error al convertir la columna: {e}[/red]")


def cantidad_nulos(df, columna):
    nulos = df[columna].isnull().sum()
    console.print(f"[yellow]Columna '{columna}' tiene {nulos} datos nulos[/yellow]")

def eliminar_nulos(df, columna):
    antes = len(df)
    df.dropna(subset=[columna], inplace=True)
    despues = len(df)
    console.print(f"[red]Se eliminaron {antes - despues} filas con nulos en '{columna}'[/red]")

def renombrar_columna(df, columna):
    nuevo_nombre = input(f"Ingresa el nuevo nombre para la columna '{columna}': ")
    df.rename(columns={columna:nuevo_nombre}, inplace=True)
    console.print(f"[green]Columna renombrada a '{nuevo_nombre}'[/green]")

def mostrar_head(df, n=10):
    table = Table(title=f"Primeros {n} registros del DataFrame", show_lines=True)
    # Agregar columna para índice
    table.add_column("Índice", justify="center", style="magenta")
    # Agregar columnas del dataframe
    for col in df.columns:
        table.add_column(col, style="green")

    # Iterar por las primeras n filas
    for idx, row in df.head(n).iterrows():
        table.add_row(str(idx), *[str(row[col]) for col in df.columns])

    console.print(table)

def extraer_numeros(df, columna):
    """
    Extrae números (enteros, decimales o porcentajes) de una columna que contiene texto mezclado.
    Ejemplo:
        'Num:12'      -> 12
        'valor 3.5%'  -> 3.5
        'id_45_text'  -> 45
    """
    try:
        def limpiar_valor(valor):
            valor = str(valor)
            # Buscar el primer número (entero o decimal)
            match = re.search(r'(\d+(?:\.\d+)?)', valor)
            if match:
                num = float(match.group(1))
                # Si contiene '%', lo interpretamos como porcentaje
                if '%' in valor:
                    num = num / 100
                return num
            return np.nan

        df[columna] = df[columna].apply(limpiar_valor)

        console.print(f"[green]Números extraídos correctamente de la columna '{columna}'.[/green]")
        mostrar_head(df)

    except Exception as e:
        console.print(f"[red]Error al extraer números: {e}[/red]")


def separar_valores(df, columna):
    """
    Separa valores en una columna usando un separador indicado por el usuario.
    Ejemplo: '1:2' con separador ':' -> columna original = 1, nueva_columna = 2
    """
    separador = input("Ingresa el separador de los valores (por ejemplo ':' o '-'): ").strip()

    # Validamos que el separador esté presente
    if not df[columna].astype(str).str.contains(separador).any():
        console.print(f"[red]No se encontró el separador '{separador}' en los valores de la columna '{columna}'.[/red]")
        return

    try:
        # Dividimos en dos partes máximo (evita más columnas si hay múltiples separadores)
        df[['col_original', 'nueva_columna']] = df[columna].astype(str).str.split(separador, n=1, expand=True)
        
        # Reemplazamos la columna original por la parte izquierda
        df[columna] = df['col_original']
        df.drop(columns=['col_original'], inplace=True)

        console.print(f"[green]Columna '{columna}' separada correctamente. Nueva columna creada: 'nueva_columna'[/green]")
        mostrar_head(df)
    except Exception as e:
        console.print(f"[red]Error al separar valores: {e}[/red]")



def detectar_patrones(df, columna):
    """
    Detecta patrones básicos: valores únicos, frecuencia de los más comunes.
    """
    console.print(f"[cyan]Análisis de patrones para la columna '{columna}':[/cyan]")
    console.print(f"- Valores únicos: {df[columna].nunique()}")
    console.print(f"- Valores más frecuentes:")
    top_values = df[columna].value_counts().head(10)
    
    table = Table(show_lines=True)
    table.add_column("Valor", style="green")
    table.add_column("Frecuencia", justify="center", style="cyan")
    
    for val, freq in top_values.items():
        table.add_row(str(val), str(freq))
    
    console.print(table)


def correlaciones(df):
    """
    Muestra correlaciones entre columnas numéricas.
    """
    numeric_df = df.select_dtypes(include=[np.number])
    if numeric_df.empty:
        console.print("[yellow]No hay columnas numéricas para correlación[/yellow]")
        return
    
    corr = numeric_df.corr()
    table = Table(title="Matriz de correlación", show_lines=True)
    table.add_column("Columna", style="green")
    for col in corr.columns:
        table.add_column(col, justify="center", style="cyan")
    
    for idx, row in corr.iterrows():
        table.add_row(idx, *[f"{v:.2f}" for v in row])
    
    console.print(table)


def cargar_archivo():
    """
    Abre una ventana para seleccionar un archivo (CSV, Excel o JSON)
    y lo carga como un DataFrame de pandas.
    """
    console.print("[bold cyan]Selecciona el archivo que deseas cargar...[/bold cyan]")

    # Ocultar ventana principal de Tkinter
    root = Tk()
    root.withdraw()

    file_path = filedialog.askopenfilename(
        title="Selecciona un archivo de datos",
        filetypes=[
            ("Archivos CSV", "*.csv"),
            ("Archivos Excel", "*.xlsx *.xls"),
            ("Archivos JSON", "*.json"),
            ("Todos los archivos", "*.*")
        ]
    )

    if not file_path:
        console.print("[red]No se seleccionó ningún archivo.[/red]")
        return None, None

    console.print(f"[green]Archivo seleccionado:[/green] {file_path}")

    # Detectar tipo de archivo y leerlo
    try:
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        elif file_path.endswith((".xlsx", ".xls")):
            df = pd.read_excel(file_path)
        elif file_path.endswith(".json"):
            df = pd.read_json(file_path)
        else:
            console.print("[red]Formato no soportado.[/red]")
            return None, None

        console.print(f"[bold green]Archivo cargado correctamente.[/bold green]")
        console.print(f"[yellow]Filas:[/yellow] {df.shape[0]}  |  [yellow]Columnas:[/yellow] {df.shape[1]}")
        return df, Path(file_path)

    except Exception as e:
        console.print(f"[red]Error al leer el archivo:[/red] {e}")
        return None, None


#funcion para guardar Dataframe
def guardar_dataframe(df, archivo_original):
    """
    Guarda el DataFrame en 'data_limpia' dentro de la subcarpeta correspondiente
    según la extensión del archivo, agregando '_limpio' y la fecha al nombre.
    """
    base_dir = Path("data_limpia")
    base_dir.mkdir(exist_ok=True)

    csv_dir = base_dir / "csv_limpia"
    json_dir = base_dir / "json_limpia"
    excel_dir = base_dir / "excel_limpia"

    csv_dir.mkdir(exist_ok=True)
    json_dir.mkdir(exist_ok=True)
    excel_dir.mkdir(exist_ok=True)

    ext = archivo_original.suffix.lower()
    nombre_base = archivo_original.stem
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")

    try:
        if ext == ".csv":
            ruta_guardado = csv_dir / f"{nombre_base}_limpio_{fecha}.csv"
            df.to_csv(ruta_guardado, index=False)
        elif ext == ".json":
            ruta_guardado = json_dir / f"{nombre_base}_limpio_{fecha}.json"
            df.to_json(ruta_guardado, orient="records", indent=4)
        elif ext in [".xls", ".xlsx"]:
            ruta_guardado = excel_dir / f"{nombre_base}_limpio_{fecha}.xlsx"
            df.to_excel(ruta_guardado, index=False)
        else:
            console.print(f"[red]No se puede guardar archivo con extensión {ext}[/red]")
            return

        console.print(f"[green]Archivo guardado correctamente en: {ruta_guardado}[/green]")

    except Exception as e:
        console.print(f"[red]Error al guardar el archivo: {e}[/red]")



# Menú interactivo por columna
def menu_columna(df, columna):
    opciones = {
        "1": tipo_datos,
        "2": eliminar_columna,
        "3": transformar_columna,
        "4": cantidad_nulos,
        "5": eliminar_nulos,
        "6": renombrar_columna,
        "7": mostrar_head,
        "8": extraer_numeros,
        "9": separar_valores
    }

    while True:
        console.print(f"\nOpciones para la columna '{columna}':")
        console.print("1. Tipo de datos")
        console.print("2. Eliminar columna")
        console.print("3. Transformación de datos")
        console.print("4. Cantidad de datos nulos")
        console.print("5. Eliminar datos nulos")
        console.print("6. Renombrar columna")
        console.print("7. Mostrar primeros registros del DataFrame")
        console.print("8. Extraer números de texto")
        console.print("9. Separar valores en nueva columna")
        console.print("0. Volver al menú de columnas")

        choice = input("Selecciona una opción: ")
        if choice == "0":
            break
        elif choice in opciones:
            # Para la opción 7 no necesita columna específica
            if choice == "7":
                opciones[choice](df)
            else:
                opciones[choice](df, columna)
        else:
            console.print("[red]Opción no válida[/red]")

# Orquestador principal -> este orquesta a las funciones de limpieza basicas

'''def orquestador_lim(df, archivo_original):
    """
    Orquestador principal para limpieza y análisis básico del DataFrame.
    Incluye opciones de manipulación de columnas, detección de patrones,
    correlaciones y guardado manual.
    """
    while True:
        console.print(f"\n[bold blue]=== Archivo actual: {archivo_original.name} ===[/bold blue]")

        table = Table(title="Columnas disponibles", show_lines=True)
        table.add_column("Número", justify="center", style="cyan")
        table.add_column("Nombre de columna", style="green")

        for i, col in enumerate(df.columns, 1):
            table.add_row(str(i), col)
        console.print(table)

        console.print("\n[bold cyan]=== Menú principal ===[/bold cyan]")
        console.print("1. Seleccionar columna para limpieza")
        console.print("2. Detectar patrones en una columna")
        console.print("3. Mostrar correlaciones entre columnas numéricas")
        console.print("G. Guardar DataFrame limpio")
        console.print("0. Salir del orquestador")

        choice = input("Selecciona una opción: ").strip().lower()

        if choice == "0":
            console.print("[yellow]Saliendo del orquestador[/yellow]")
            break

        elif choice == "1":
            try:
                num = int(input("Selecciona el número de la columna: "))
                if 1 <= num <= len(df.columns):
                    columna = df.columns[num - 1]
                    menu_columna(df, columna)  # tu función ya existente
                else:
                    console.print("[red]Número fuera de rango[/red]")
            except ValueError:
                console.print("[red]Entrada no válida[/red]")

        elif choice == "2":
            try:
                num = int(input("Selecciona el número de la columna para detectar patrones: "))
                if 1 <= num <= len(df.columns):
                    columna = df.columns[num - 1]
                    detectar_patrones(df, columna)  # tu función ya existente
                else:
                    console.print("[red]Número fuera de rango[/red]")
            except ValueError:
                console.print("[red]Entrada no válida[/red]")

        elif choice == "3":
            correlaciones(df)  # tu función ya existente

        elif choice == "g":
            guardar_dataframe(df, archivo_original)

        else:
            console.print("[red]Opción no válida[/red]")


# ============================================
# 🔹 Ejecución principal del programa
# ============================================
if __name__ == "__main__":
    df, archivo = cargar_archivo()
    df = read_selected_file(archivo)
    if df is not None:
        orquestador_lim(df, archivo)
        console.print("[green]Proceso de limpieza finalizado[/green]")
    else:
        console.print("[red]No se cargó ningún archivo. Terminando programa.[/red]")
'''