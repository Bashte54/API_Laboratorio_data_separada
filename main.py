import dataClean2 as dc
from rich.table import Table


def orquestador_lim(df, archivo_original):
    while True:
        dc.console.print(f"\n[bold blue]=== Archivo actual: {archivo_original.name} ===[/bold blue]")

        # Mostrar columnas
        table = Table(title="Columnas disponibles", show_lines=True)
        table.add_column("Número", justify="center", style="cyan")
        table.add_column("Nombre de columna", style="green")

        for i, col in enumerate(df.columns, 1):
            table.add_row(str(i), col)
        dc.console.print(table)

        # Menú
        dc.console.print("\n[bold cyan]=== Menú principal ===[/bold cyan]")
        dc.console.print("1. Seleccionar columna para limpieza")
        dc.console.print("2. Detectar patrones en una columna")
        dc.console.print("3. Mostrar correlaciones entre columnas numéricas")
        dc.console.print("G. Guardar DataFrame limpio")
        dc.console.print("0. Salir del orquestador")

        choice = input("Selecciona una opción: ").strip().lower()

        if choice == "0":
            dc.console.print("[yellow]Saliendo del orquestador[/yellow]")
            break

        elif choice == "1":
            try:
                num = int(input("Número de columna: "))
                if 1 <= num <= len(df.columns):
                    columna = df.columns[num - 1]
                    dc.menu_columna(df, columna)
                else:
                    dc.console.print("[red]Número fuera de rango[/red]")
            except ValueError:
                dc.console.print("[red]Entrada no válida[/red]")

        elif choice == "2":
            num = int(input("Número de columna para detectar patrones: "))
            columna = df.columns[num - 1]
            dc.detectar_patrones(df, columna)

        elif choice == "3":
            dc.correlaciones(df)

        elif choice == "g":
            dc.guardar_dataframe(df, archivo_original)

        else:
            dc.console.print("[red]Opción no válida[/red]")


# ============================
#   MAIN PRINCIPAL
# ============================
if __name__ == "__main__":
    df, archivo = dc.cargar_archivo()
    df = dc.read_selected_file(archivo)
    if df is not None:
        orquestador_lim(df, archivo)
        dc.console.print("[green]Proceso de limpieza finalizado[/green]")
    else:
        dc.console.print("[red]No se cargó ningún archivo. Terminando programa.[/red]")
