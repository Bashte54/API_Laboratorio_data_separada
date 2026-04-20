source .venv/bin/activate

#ir a la carpeta donde este la app
cd <ubicacion api>

pip install -r requirements.txt
uvicorn <nombre_app>:app --reload
# uvicorn apiSpark:app --reload
