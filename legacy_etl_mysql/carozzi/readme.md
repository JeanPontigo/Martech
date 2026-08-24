# Sistema ETL para Mercado Carozzi

Implementa un sistema automatizado para la extracción, transformación y carga de los datos provenientes de siete endpoints de Magento / Adobe.

El objetivo principal del código es de disponer datos sensibles de un cliente especifico a una base de datos para satisfacer las diferentes necesidades
internas del usuario, organización y empresa.

## Herramientas Utilizadas para su Desarrollo
1. Python Versión 3.11 o superior
2. Postman
3. MySQL Shell
4. FileZilla
5. Visual Studio Code
6. FortiClient VPN
7. Slack

## Herramientas Necesarias para su uso
1. Python Versión 3.11 o superior
2. MySQL Shell
3. Visual Studio Code
4. FortiClient VPN
5. Slack

## Instrucciones
### Parte 1: *Creación de la Base de Datos*
Primero se requiere crear una nueva Base de Datos al nivel local mediante la aplicación *MySQL Shell*, al abrir debe escribir `\sql` y apretar enter:

`Type '\help' or '\?' for help; '\quit' to exit.`

`MySQL  JS >\sql`

`Switching to SQL mode... Commands end with ;`

`MySQL  SQL >`

Para establecer conexión con la base de datos local y debe ingresar sus credenciales de acceso:

`MySQL  SQL >\c [Nombre]@localhost`

`Creating a session to '[Nombre]@localhost'`

`Please provide the password for '[Nombre]@localhost':`

El acceso se considera exitoso cuando aparece el siguiente texto en pantalla:

`Fetching global names for auto-completion... Press ^C to stop.`

`Your MySQL connection id is 13 (X protocol)`

`Server version: [Número de Versión] MySQL Community Server - GPL`

`No default schema selected; type \use <schema> to set one.`

`MySQL  localhost:[Número de Puerto]0+ ssl  SQL >`

Para revisar el estado actual de la base de datos, debe ingresar el siguiente texto:

`MySQL  localhost:[Número de Puerto]0+ ssl  SQL >SHOW DATABASES;`

`+---------------------+`  
`| Database ---------- |`  
`+---------------------+`  
`| information_schema -|`  
`| mysql ------------- |`  
`| performance_schema -|`  
`| sakila ------------ |`  
`| sys --------------- |`  
`| world ------------- |`  
`+---------------------+`  
`6 rows in set (0.0077 sec)`

Para crear una nueva base de datos, debe ingresar el siguiente texto:

`MySQL  localhost:[Número de Puerto]0+ ssl  SQL >CREATE DATABASE [Nombre de la Base de Datos];`

Debe ahora volver a visualizar las bases de datos mediante `SHOW DATABASES!` para confirmar el exito del proceso:

`MySQL  localhost:[Número de Puerto]0+ ssl  SQL >SHOW DATABASES;`

`+---------------------+`  
`| Database ---------- |`  
`+---------------------+`  
`| Nueva Base de Datos |`  
`| information_schema -|`  
`| mysql ------------- |`  
`| performance_schema -|`  
`| sakila ------------ |`  
`| sys --------------- |`  
`| world ------------- |`  
`+---------------------+`  
`7 rows in set (0.0088 sec)`

Considerar que `MySQL Shell` solamente se utiliza para crear nuevas Bases de Datos.

A continuación debe entrar a la aplicación `DBeaver` y seleccionar la siguiente secuencia de botones: *CTRL +  Mayúscula + N* o en caso de presentar problemas debe buscar el boton con el texto: *Nueva Conexión* y seleccionar `MySQL`. Debe ingresar los datos solicitados y para confirmar la estabilidad de la conexión con la base de datos, debe buscar la opción *_Probar Conexión ..._*

### Parte 2: *Instalación de Librerías*
En la siguiente Fase se debe extraer la carpeta *Código ETL MC* y abrir el código usando el programa `Visual Studio Code`, al entrar debe crear una nueva terminal y escribir los siguientes comandos para crear un nuevo entorno virtual:

`pip install virtualenv`

`python -m venv [Default]`

`[Default]/Scripts/Activate`

Para instalar las librerías, debe escribir en la terminal:

`pip install -r requirements.txt`

### Parte 3: *Archivo .env*
Dentro del .env, debe ingresar los siguientes datos:

_**Enlaces página web:**_  
**MC_URL:** Contiene el URL con la página web requerida.  
**SLACK_AVISOS:** Contiene un URL que conecta con el canal de avisos en Slack.  
**MC_TOKEN:** Token necesario para descargar la información proveniente de los EndPoints  
**SLACK_TOKEN:** Token necesario para hacer funcionar los avisos del bot.  
**CANAL_COMANDOS:** ID del canal de slack.  

_**Configuración de los Logs:**_ 
**TIMES:** Tiempo en formato de texto donde se resetea el almacenamiento de los logs.  
**TIEMPO_ELIMINACION:** Tiempo estimado en días para eliminar los archivos Logs del sistema.  

_**Conexión con la Base de Datos**_  
**MYSQL_USER:** Nombre de la cuenta de usuario de MySQL.  
**MYSQL_PASSWORD:** Contraseña del usuario en MySQL.  
**MYSQL_HOST:** Dirección del Host.  
**MYSQL_PORT:** Número de puerto.  
**MYSQL_DBNAME_MCETL:** Nombre de la Base de Datos creada anteriormente en MySQL.  

_**Guardados Adicionales:**_  
**GUARDADO:** Es una dirección que especifica en donde guardar los datos creados durante el funcionamiento del código.

### Parte 4: *Utilización del Código*

Al terminar de configurar los pasos anteriores, se tiene que dirigir al archivo main.py y compilar el código al colocar el boton de play.