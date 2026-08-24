import logging
import logging
import os
import gzip
import os
import requests
from logging.handlers import TimedRotatingFileHandler
from dotenv import load_dotenv
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from slack_sdk import WebClient
from pathlib import Path
import gzip
import shutil
import re

# Cargar los datos provenientes del .env
load_dotenv()
TIMES = os.getenv("TIMES")
TIEMPO_ELIMINACION = os.getenv("TIEMPO_ELIMINACION")
webhook = os.getenv("SLACK_AVISOS")
tokendo = os.getenv("SLACK_TOKEN")
canal_id = os.getenv("CANAL_COMANDOS")
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
enlink = Path(os.getenv("GUARDADO", BASE_DIR / "archivos_generados")).resolve()

cliente = WebClient(token= tokendo)

# Archivos que se crearan dentro de Logs
carpeta_log = os.path.join(enlink / "log", "ETL.log")

# Filtración de palabras o llaves clave
class filtracion_palabras(logging.Filter):
    correo_electronico = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
    rut = re.compile(r"(?:\d{1,3}(?:\.\d{3}){2}|\d{2}\.\d{3}\.\d{2}|\d\.\d{3}\.\d{2})-[\dK]$")
    def filtrado(self, record):
        mensaje = record.getMessage()
        mensaje = self.correo_electronico.sub("????????????", mensaje)
        mensaje = self.rut.sub("????????????", mensaje)
        record.mensaje = mensaje
        record.args = ()
        return True

# Envia mensajes por slack mediante Handler
class envio_slack(logging.Handler):
    def __init__(self, WEB_ENLACE, level=logging.WARNING):
        super().__init__(level=level)
        self.WEB_ENLACE = WEB_ENLACE
        self.colors = {
            logging.INFO: "#0011ff",
            logging.DEBUG: "#36a64f",
            logging.WARNING: "#ffcc00",
            logging.ERROR: "#ff7b00",
            logging.CRITICAL: "#ff0000",}
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=1.5 , status_forcelist=[500, 501, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
    
    def emit(self, record):
        try:
            msg = self.format(record)
            color = self.colors.get(record.levelno, "#808080")
            payload = {"text": "Sistema de Monitoreo ETL: Error en el código",
                       "attachments": [{
                        "color": color,
                        "title": f"{record.levelname}: {record.module}",
                        "text": f"{msg}",
                        "footer": "Sistema de Notificaciones",
                        "ts": record.created
                    }]}
            requests.post(self.WEB_ENLACE, json= payload, timeout=15)
        except Exception as e:
            self.handleError(record)
            print(f"Error Interno de Slack, mensaje de error: {e}")
        except requests.exceptions.Timeout:
            print("Slack tardó mucho tiempo en responder.")

def rotacion_archivos():
    def nombrar(nombre_predeterminado):
        return nombre_predeterminado + ".gz"
    
    def rotacion(source, destino):
        with open(source, "rb") as funcion_inicial:
            with gzip.open(destino, "wb") as funcion_final:
                shutil.copyfileobj(funcion_inicial, funcion_final)
        os.remove(source)
    
    handler = TimedRotatingFileHandler(
        carpeta_log,
        when=TIMES,
        backupCount=TIEMPO_ELIMINACION,
        encoding="utf-8")
    
    handler.nombrar = nombrar
    handler.rotacion = rotacion
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s // %(message)s")
    hello = filtracion_palabras()
    handler.addFilter(hello)
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    return handler

# Configuramos los mensajes de slack
def funcion_slack():
    if not webhook:
        print("No hay ninguna información presente en los Webhook")
        return None
    
    handler = envio_slack(WEB_ENLACE=webhook, level=logging.WARNING)
    formatter = logging.Formatter("[%(levelname)s] : [%(name)s] - %(message)s")
    handler.setFormatter(formatter)
    return handler

# Se utiliza en main.py
def setup_logging():
    logger = logging.getLogger("etl")
    logger.setLevel(logging.INFO)
    
    if logger.handlers:
        return logger
    
    file_handler = rotacion_archivos()
    logger.addHandler(file_handler)

    slack_handler = funcion_slack()
    
    if slack_handler:
        logger.addHandler(slack_handler)
    logger.propagate = False
    return logger

# Sube la información de los archivos a un canal de slack
def enviar_slack(archivo= None, nombre= None, mensaje= None):
    
    cliente.files_upload_v2(
            channel=canal_id,
            file=archivo,
            title=nombre,
            initial_comment=mensaje,)