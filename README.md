source .venv/bin/activate

#ir a la carpeta de la api
cd <ubicacion api>

pip install -r requirements.txt

cd API

uvicorn <nombre_app>:app --reload
# uvicorn api_conbinada:app --reload
