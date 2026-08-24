import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from dotenv import load_dotenv


# Función para crear la conexión a la base de datos
def create_db_conn():
    load_dotenv()
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = os.getenv("MYSQL_PORT")
    MYSQL_DBNAME = os.getenv("MYSQL_DBNAME")

    connection_url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DBNAME}"
    engine = create_engine(connection_url)
    return engine


# Funciones generales
def build_url(endpoint):
    """Construye la URL base combinada con el endpoint proporcionado."""
    base_url = "https://www.ariztiaatunegocio.cl"
    return base_url + endpoint

def build_headers():
    """Construye los headers para la solicitud."""
    return {
        "Authorization": "Bearer f13f79qu6hcxc3a4lyfh2eatfatqd8u3",  # Reemplaza con tu token
        "Content-Type": "application/json"
    }
# smhqenytck4kuz7thm5m1m3q1b7qd16n
# utils.py
def normalizar_rut(rut):
    if pd.isnull(rut):
        return None
    return rut.replace('.', '').replace('-', '').strip().upper()


def ci_art():
    art = """
    =====================================================================================================
     _____           _                              _____      _       _ _ _                           
    /  __ \         | |                            |_   _|    | |     | | (_)                          
    | /  \/_   _ ___| |_ ___  _ __ ___   ___ _ __    | | _ __ | |_ ___| | |_  __ _  ___ _ __   ___ ___ 
    | |   | | | / __| __/ _ \| '_ ` _ \ / _ \ '__|   | || '_ \| __/ _ \ | | |/ _` |/ _ \ '_ \ / __/ _ \\
    | \__/\ |_| \__ \ || (_) | | | | | |  __/ |     _| || | | | ||  __/ | | | (_| |  __/ | | | (_|  __/
     \____/\__,_|___/\__\___/|_| |_| |_|\___|_|     \___/_| |_|\__\___|_|_|_|\__, |\___|_| |_|\___\___|
                                                                              __/ |                    
                                                                             |___/                     
                                ETL - Adobe Commerce - Ariztía a tu Negocior
    =====================================================================================================
                    -.-. .-. .. ... / .. -. -.-. .-.. ..- -.-- . / --. --- --. --- ..--..
    =====================================================================================================
    """
    print(art)