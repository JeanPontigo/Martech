import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from sqlalchemy.engine import URL

# Desarrolla una nueva conexión a una base de datos
def crear_conexion():
    load_dotenv(override=True)

    conexion_sql = URL.create(
        drivername = "mysql+pymysql",
        username = os.getenv("MYSQL_USER"),
        password = os.getenv("MYSQL_PASSWORD"),
        host = os.getenv("MYSQL_HOST"),
        port = int(os.getenv("MYSQL_PORT")),
        database = os.getenv("MYSQL_DBNAME_MCETL"))

    inicio_conexion = create_engine(conexion_sql)

    print("\nConexión con la Base de Datos establecida")
    return inicio_conexion