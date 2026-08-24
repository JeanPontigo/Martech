import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import os
import ssl
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Llamar Llaves
Correo = os.getenv("email")
Clave = os.getenv("contra")
Server = os.getenv("servidor")
Port = os.getenv("puerto_envio")
correo01 = os.getenv("correos_to_01")
correo02 = os.getenv("correos_to_02")

# Imagenes
LOGO_MARTECH = "./imagen/martech.gif"

def preparando_correo(imagen_url):
    hoy = datetime.now()
    formato = hoy.strftime("%Y-%m-%d")
    mensaje = MIMEMultipart('related')
    mensaje['From'] = Correo
    mensaje['To'] = [correo01, correo02]
    mensaje['Subject'] = f"Informe Semanal de ETL Mercado Carozzi [{formato}]"

    contenido_html = f"""
        <html>
        <body>
            <h2>Sistema de Informes Semanales</h2>
            <p>Resultado Promedio del Rendimiento del ETL de Mercado Carozzi:.</p>
            <p>---------------------------------------------------------------</p>
            <p>Body de Prueba</p>
            <p>---------------------------------------------------------------</p>
            <br>
            <p>Saludos Cordiales.</p>
            <img src="cid:logo_martech" alt="Logo Martech style="width:200px; border: 2px solid #333;">

        </body>
        </html>
        """

    mensaje.attach(MIMEText(contenido_html, "html"))
    try:
        with open(imagen_url, "rb") as fopo:
            imagen = MIMEImage(fopo.read())
        imagen.add_header("contenido-ID", "<imagen_logo>")
        mensaje.attach(imagen)
    except FileNotFoundError:
        print(f"Advertencia de Seguridad: No se encontró la imagen en la ruta: {imagen_url}")
        print("El correo se enviará sin la imagen incrustada.")
    return mensaje.as_string()

def enviar_correo():
    try:
        contexto = ssl.create_default_context()
        print("Intentando Conectar")
        
        with smtplib.SMTP(server, Port) as server:
            server.starttls(context=contexto)
            server.login(Correo, Clave)
            mensajero = preparando_correo(imagen_url=LOGO_MARTECH)
            server.sendmail(Correo, [correo01, correo02], mensajero)
            print("Correo Enviado con Exito")
    except smtplib.SMTPAuthenticationError:
        print("Error de Autenticación SMTP.")
    except smtplib.SMTPConnectError:
        print("Error de Conexión")
    except Exception as e:
        print("Error Inesperado")
        print(f"Mensaje de error: {e}")

if __name__ == "__main__":
    enviar_correo()