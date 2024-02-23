from time import time
from fastapi import FastAPI, HTTPException, __version__
from fastapi.responses import HTMLResponse
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
import os 
import snowflake.connector
# Se crea una instancia de la aplicación FastAPI
app = FastAPI()


# HTML que se enviará como respuesta en la ruta "/"
html = f"""
<!DOCTYPE html>
<html>
    <head>
        <title>FastAPI</title>
        <link rel="icon" href="/static/Subte-logo.svg" type="image/x-icon" />
    </head>
    <body>
        <div class="bg-gray-200 p-4 rounded-lg shadow-lg">
            <!-- Agregar la imagen Subte-logo.svg desde la carpeta /static -->
            <img src="/static/Subte_logo.jpg" alt="Logo del Subte" style="max-width: 200px;">
            
            <h1>Hello, Bienvenidos a la API:</h1>
            <p>Pronóstico de demora en las llegadas y salidas de trenes del SUBTE de la ciudad de Buenos Aires.</p>
            <p>FastAPI@{__version__}</p>
            <p>Link para ver la API: <a href="/docs">/docs</a></p>
            <p>Realizado por Leo</p>
        </div>
    </body>
</html>
"""

# Parámetros de conexión a la base de datos

def conectar_snowflake():
    """Función para conectar a Snowflake"""
    conn = snowflake.connector.connect(
        user=os.environ['SNOWSQL_USER'],
        password=os.environ['SNOWSQL_PWD'],
        account=os.environ['SNOWSQL_ACCOUNT'],
        warehouse='COMPUTE_WH',
        database='SOURCE',
        schema='RAW'
    )
    return conn

def consulta_db_pronostico(conn):
    """Selecciona todos los registros de la tabla 'SUBTEDATA' y devuelve un DataFrame"""
    try:
        # Crear un cursor para ejecutar la consulta
        cur = conn.cursor()
        # Ejecutar la consulta SQL
        cur.execute("SELECT * FROM SUBTEDATA")
        # Obtener los resultados de la consulta
        rows = cur.fetchall()
        # Obtener los nombres de las columnas
        columns = [desc[0] for desc in cur.description]
        
        # Crear un DataFrame con los resultados y los nombres de las columnas
        df = pd.DataFrame(rows, columns=columns)
        # Cerrar el cursor
        cur.close()
        
        print("Datos de la tabla 'SUBTEDATA' obtenidos correctamente.")
        
        return df
    except Exception as e:
        print("Error durante la consulta a la base de datos:", e)
        return None



# Definición de la ruta principal "/"    
@app.get("/")
async def root():
    return HTMLResponse(html)


# Definición de la ruta "/get_pronostico ", que permite obtener todos los datos de la tabla pronósticos guardados en la BD.
@app.get('/get_pronostico', 
         summary="Obtiene la informacion", 
         description="""Pronostico:  retorna toda la tabla  del pronostico en un json."""
        )
  
def obtener_pronostico_data_subte():
    # Conectar a la base de datos
    conn = conectar_snowflake()
    
    # Consultar datos de pronostico desde la base de datos
    df_pronostico = consulta_db_pronostico(conn)
    
    # Filtrar datos según los parámetros de la solicitud
    resultado_loc = df_pronostico
    
    # Cerrar la conexión a la base de datos
    conn.dispose()
    return resultado_loc.to_dict(orient='records')


# Definición de la ruta "/get_pronostico/{linea}/{direccion}/{estacion}"
@app.get('/get_pronostico/{linea}/{direccion}/{estacion}', 
         summary="Obtiene la informacion de la demora  en una estación", 
         description="""Pronostico de la demora en llegada y salida de los trenes del subte de la Ciudad de Buienos aires.
         
         Ejemplo: linea_de_subte: LineaA, direccion_a (hacia donde): Plaza de Mayo, estación: San Pedrito"""
        )
  
def obtener_pronostico_delay_subte(linea_de_subte : str, direccion_a: str, estacion: str):
    # Conectar a la base de datos
    conn = conectar_snowflake()
    
    # Consultar datos de pronostico desde la base de datos
    df_pronostico = consulta_db_pronostico(conn)
    
    # Filtrar datos según los parámetros de la solicitud
    resultado_loc = df_pronostico.loc[(df_pronostico['Route_Id'] == linea_de_subte) & 
                                      (df_pronostico['Direction_to'] == direccion_a) & 
                                      (df_pronostico['stop_name'] == estacion), 
                                     ['arrival_delay', 'departure_delay']]
    
    # Cerrar la conexión a la base de datos
    conn.dispose()
    
    return resultado_loc
