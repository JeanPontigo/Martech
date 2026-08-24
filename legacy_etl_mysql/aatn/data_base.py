import os
#from dotenv import load_dotenv
#from sqlalchemy import create_engine
#from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, text
import pandas as pd
import sqlalchemy
from tqdm import tqdm
from utils import create_db_conn

def run_setup_ariztia():
    conn = create_db_conn()

    # Paso 1: Eliminar relaciones y tablas existentes
    with conn.connect() as con:
        print("Eliminando relaciones y tablas existentes...")
        foreign_keys = [
            'fk_order_id', 'fk_sku', 'fk_rut', 'fk_centro', 'fk_info_or', 'fk_id_sap'
        ]
        for fk in foreign_keys:
            try:
                con.execute(text(f'ALTER TABLE ariztia_grid DROP FOREIGN KEY {fk};'))
            except Exception as e:
                print(f"No se pudo eliminar la clave foránea {fk}: {e}")
        
        #tables = ['ariztia_grid2', 'ariztia_orders2', 'ariztia_products', 'ariztia_clients_approved2', 'cd', 'ariztia_metrics'
        tables = ['ariztia_grid', 'ariztia_orders', 'ariztia_products', 'ariztia_clients', 'cd', 'ariztia_clients_pending', 'info_orders', 'ariztia_metrics', 'grid_sap', 'orders_sap']
        for table in tables:
            try:
                con.execute(text(f'DROP TABLE IF EXISTS {table};'))
            except Exception as e:
                print(f"Error eliminando tabla {table}: {e}")

    # Paso 2: Crear y cargar las tablas
    csv_files = {
        "info_orders": ["order_id", "payment_method", "customer_group", "client_group"],
        "cd": ["center", "description", "zona"],
        "ariztia_clients": [
            "entity_id", "id_adobe", "id_sap", "usu_rut", "centro", 
            "region", "active", "razon_social", "contacto", "celular", 
            "email", "created_at", "last_conection", "last_updated"],
        "ariztia_orders": [
            "order_id", "order_sap", "id_sap_client", "id_destinatario", 
            "fecha_unificada", "fecha_despacho", "fecha_compra", "hora_compra", 
            "rut", "direccion", "comuna", "venta_neta", "venta_bruta", "valor_envio", 
            "monto_dscto", "total_final", "nombre_cupon", "status", "last_updated_at"],
        "ariztia_grid": [
            "order_id", "order_sap", "fecha_unificada", "fecha_compra", 
            "hora_compra", "sku", "marca", "precio", "cantidad", "venta_neta", "venta_bruta", "status"],
        "ariztia_products": [
            "id", "sku", "name", "created_at", "updated_at", "weight_per_box", "image_url",
            "content_measurement_unit", "category", "subcategory", "brand", 
            "units_per_box", "average_weight_per_box", "box_size", "storage_description"],
        "ariztia_clients_pending": ["id_sap", "id_adobe", "razon_social", "rut", "celular", "fecha_solicitud", "email", "contacto"],
        "ariztia_metrics": ['last_update', 'total_clients', 'total_orders', 'total_products'],
        "grid_sap":['id_sap', 'id_sap_client', 'sku', 'id_sap_sku', 'fecha_pedido', 'fecha_creacion', 'fecha_entrega','unidades_facturadas_adobe', 'kilos_facturados_sap', 'unidades_facturadas_sap', 'total_facturado'],
        "orders_sap":['id_sap', 'id_sap_client', 'fecha_pedido', 'fecha_creacion', 'fecha_entrega', 'total_facturado']
    }

    # Lista de columnas que originalmente son `DATETIME`
    datetime_columns = {
        "ariztia_orders": ["last_updated_at"],
        "ariztia_clients": ["last_updated"]
    }
    for table_name, columns in csv_files.items():
        csv_path = os.path.join("data", f"{table_name}.csv")
        print(f"Cargando {table_name}.csv en la tabla {table_name}...")

        total_rows = sum(1 for row in open(csv_path, encoding='utf-8')) - 1
        with tqdm(total=total_rows, unit="rows") as pbar:
            for chunk in pd.read_csv(csv_path, chunksize=30000, names=columns, header=0):
                # Convertir columnas `DATETIME`
                if table_name in datetime_columns:
                    for col in datetime_columns[table_name]:
                        if col in chunk.columns:
                            chunk[col] = pd.to_datetime(chunk[col], errors="coerce")  # Convierte correctamente
                            chunk[col] = chunk[col].where(pd.notnull(chunk[col]), None)  # Reemplaza NaT con None

                # Definir tipos de datos: solo `DATETIME` como `VARCHAR(255)`
                dtype_mapping = {
                    col: sqlalchemy.types.DATETIME() for col in datetime_columns.get(table_name, [])
                }

                chunk.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists="append",
                    index=False,
                    dtype=dtype_mapping  # Solo cambia los `DATETIME`
                )
                pbar.update(len(chunk))


    # Paso 3: Ajustar tipos de datos antes de establecer claves
    with conn.connect() as con:
        print("Ajustando tipos de datos...")

        # Modificar columnas de la tabla de CD
        con.execute(text('ALTER TABLE cd MODIFY COLUMN center VARCHAR(50);'))
        con.execute(text('ALTER TABLE cd MODIFY COLUMN description VARCHAR(255);'))
        con.execute(text('ALTER TABLE cd MODIFY COLUMN zona VARCHAR(50);'))
        con.execute(text('CREATE INDEX idx_center ON cd(center);'))
        
        # Modificar columnas de la tabla de clientes
        con.execute(text('ALTER TABLE ariztia_clients MODIFY COLUMN entity_id VARCHAR(255);'))
        con.execute(text('ALTER TABLE ariztia_clients MODIFY COLUMN centro VARCHAR(255);'))
        con.execute(text('ALTER TABLE ariztia_clients MODIFY COLUMN id_sap VARCHAR(255);'))
        con.execute(text('ALTER TABLE ariztia_clients MODIFY COLUMN usu_rut VARCHAR(255);'))
        con.execute(text('ALTER TABLE ariztia_clients MODIFY COLUMN active TINYINT(1);'))  # Para almacenar booleanos
        con.execute(text('ALTER TABLE ariztia_clients MODIFY COLUMN created_at DATETIME;'))
        con.execute(text('ALTER TABLE ariztia_clients MODIFY COLUMN last_conection DATETIME;'))
        con.execute(text('ALTER TABLE ariztia_clients MODIFY COLUMN last_updated DATETIME;'))
        con.execute(text('CREATE INDEX idx_rut ON ariztia_clients(usu_rut);')) 

        # Modificar columnas de la tabla de ordenes
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN order_id BIGINT;'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN order_sap VARCHAR(255);'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN id_sap_client VARCHAR(255);'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN id_destinatario BIGINT;'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN fecha_unificada DATETIME;'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN fecha_despacho DATE;'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN fecha_compra DATE;'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN hora_compra TIME;'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN rut VARCHAR(255);'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN direccion TEXT;'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN comuna TEXT;'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN venta_neta BIGINT;'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN venta_bruta BIGINT;'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN valor_envio BIGINT;'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN monto_dscto BIGINT;'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN total_final BIGINT;'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN nombre_cupon VARCHAR(255);'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN status VARCHAR(255);'))
        con.execute(text('ALTER TABLE ariztia_orders MODIFY COLUMN last_updated_at DATETIME NULL;'))

        # Modificar columnas de la tabla de products
        con.execute(text('ALTER TABLE ariztia_products MODIFY COLUMN sku VARCHAR(255);'))  # Mantener como identificador clave
        con.execute(text('ALTER TABLE ariztia_products MODIFY COLUMN name TEXT;'))
        con.execute(text('ALTER TABLE ariztia_products MODIFY COLUMN created_at DATETIME;'))
        con.execute(text('ALTER TABLE ariztia_products MODIFY COLUMN updated_at DATETIME;'))
        con.execute(text('ALTER TABLE ariztia_products MODIFY COLUMN weight_per_box TEXT;'))
        con.execute(text('ALTER TABLE ariztia_products MODIFY COLUMN image_url TEXT;'))
        con.execute(text('ALTER TABLE ariztia_products MODIFY COLUMN content_measurement_unit TEXT;'))
        con.execute(text('ALTER TABLE ariztia_products MODIFY COLUMN category TEXT;'))
        con.execute(text('ALTER TABLE ariztia_products MODIFY COLUMN subcategory TEXT;'))
        con.execute(text('ALTER TABLE ariztia_products MODIFY COLUMN brand VARCHAR(255);'))  # Ajustar como texto corto
        con.execute(text('ALTER TABLE ariztia_products MODIFY COLUMN units_per_box BIGINT;'))
        con.execute(text('ALTER TABLE ariztia_products MODIFY COLUMN average_weight_per_box TEXT;'))
        con.execute(text('ALTER TABLE ariztia_products MODIFY COLUMN box_size TEXT;'))
        con.execute(text('ALTER TABLE ariztia_products MODIFY COLUMN storage_description TEXT;'))

        # Modificar columnas de la tabla de grid
        con.execute(text('ALTER TABLE ariztia_grid MODIFY COLUMN order_id BIGINT;'))
        con.execute(text('ALTER TABLE ariztia_grid MODIFY COLUMN order_sap VARCHAR(255);'))
        con.execute(text('ALTER TABLE ariztia_grid MODIFY COLUMN fecha_unificada DATETIME;'))
        con.execute(text('ALTER TABLE ariztia_grid MODIFY COLUMN fecha_compra DATE;'))
        con.execute(text('ALTER TABLE ariztia_grid MODIFY COLUMN hora_compra TIME;'))
        con.execute(text('ALTER TABLE ariztia_grid MODIFY COLUMN sku VARCHAR(255);'))  # SKU como identificador
        con.execute(text('ALTER TABLE ariztia_grid MODIFY COLUMN marca TEXT;'))
        con.execute(text('ALTER TABLE ariztia_grid MODIFY COLUMN precio BIGINT;'))
        con.execute(text('ALTER TABLE ariztia_grid MODIFY COLUMN cantidad BIGINT;'))
        con.execute(text('ALTER TABLE ariztia_grid MODIFY COLUMN venta_neta BIGINT;'))
        con.execute(text('ALTER TABLE ariztia_grid MODIFY COLUMN venta_bruta BIGINT;'))
        con.execute(text('ALTER TABLE ariztia_grid MODIFY COLUMN status VARCHAR(255);'))

        # Modificar columnas de la tabla de informacion extra de ordenes
        con.execute(text('ALTER TABLE info_orders MODIFY COLUMN order_id BIGINT;'))
        con.execute(text('ALTER TABLE info_orders MODIFY COLUMN payment_method VARCHAR(255);'))
        con.execute(text('ALTER TABLE info_orders MODIFY COLUMN customer_group BIGINT;'))
        con.execute(text('ALTER TABLE info_orders MODIFY COLUMN client_group VARCHAR(255);'))

        # Modificar columnas de la tabla de clientes pendientes
        con.execute(text('ALTER TABLE ariztia_clients_pending MODIFY COLUMN id_sap TEXT NULL;'))
        con.execute(text('ALTER TABLE ariztia_clients_pending MODIFY COLUMN id_adobe TEXT NULL;'))
        con.execute(text('ALTER TABLE ariztia_clients_pending MODIFY COLUMN razon_social TEXT NULL;'))
        con.execute(text('ALTER TABLE ariztia_clients_pending MODIFY COLUMN rut TEXT NULL;'))
        con.execute(text('ALTER TABLE ariztia_clients_pending MODIFY COLUMN celular TEXT NULL;'))
        con.execute(text('ALTER TABLE ariztia_clients_pending MODIFY COLUMN fecha_solicitud DATETIME NULL;'))
        con.execute(text('ALTER TABLE ariztia_clients_pending MODIFY COLUMN email TEXT NULL;'))
        con.execute(text('ALTER TABLE ariztia_clients_pending MODIFY COLUMN contacto TEXT NULL;'))

        # Modificar columnas de la tabla de metricas
        con.execute(text('ALTER TABLE ariztia_metrics MODIFY COLUMN last_update DATETIME;'))
        con.execute(text('ALTER TABLE ariztia_metrics MODIFY COLUMN total_clients INT;'))
        con.execute(text('ALTER TABLE ariztia_metrics MODIFY COLUMN total_orders INT;'))
        con.execute(text('ALTER TABLE ariztia_metrics MODIFY COLUMN total_products INT;'))

        # Modificar columnas de la tabla de grid_sap
        con.execute(text('ALTER TABLE grid_sap MODIFY COLUMN id_sap VARCHAR(255);'))
        con.execute(text('ALTER TABLE grid_sap MODIFY COLUMN id_sap_client VARCHAR(255);'))
        con.execute(text('ALTER TABLE grid_sap MODIFY COLUMN sku VARCHAR(255);'))
        con.execute(text('ALTER TABLE grid_sap MODIFY COLUMN id_sap_sku VARCHAR(255);'))
        con.execute(text('ALTER TABLE grid_sap MODIFY COLUMN fecha_pedido DATETIME;'))
        con.execute(text('ALTER TABLE grid_sap MODIFY COLUMN fecha_creacion DATETIME;'))
        con.execute(text('ALTER TABLE grid_sap MODIFY COLUMN fecha_entrega DATETIME;'))
        con.execute(text('ALTER TABLE grid_sap MODIFY COLUMN unidades_facturadas_adobe INT;'))
        con.execute(text('ALTER TABLE grid_sap MODIFY COLUMN kilos_facturados_sap INT;'))
        con.execute(text('ALTER TABLE grid_sap MODIFY COLUMN unidades_facturadas_sap INT;'))
        con.execute(text('ALTER TABLE grid_sap MODIFY COLUMN total_facturado INT;'))

        # Modificar columnas de la tabla de orders_sap
        con.execute(text('ALTER TABLE orders_sap MODIFY COLUMN id_sap VARCHAR(255);'))
        con.execute(text('ALTER TABLE orders_sap MODIFY COLUMN id_sap_client VARCHAR(255);'))
        con.execute(text('ALTER TABLE orders_sap MODIFY COLUMN fecha_pedido DATETIME;'))
        con.execute(text('ALTER TABLE orders_sap MODIFY COLUMN fecha_creacion DATETIME;'))
        con.execute(text('ALTER TABLE orders_sap MODIFY COLUMN fecha_entrega DATETIME;'))
        con.execute(text('ALTER TABLE orders_sap MODIFY COLUMN total_facturado INT;'))
        con.execute(text('CREATE INDEX idx_sap_id ON orders_sap(id_sap);')) 

    # Paso 4: Crear claves primarias y relaciones entre tablas
    with conn.connect() as con:
        print("Creando claves primarias y relaciones entre tablas...")

        # Claves primarias
        con.execute(text('ALTER TABLE cd ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;'))
        con.execute(text('ALTER TABLE ariztia_clients ADD PRIMARY KEY (entity_id);'))
        con.execute(text('ALTER TABLE ariztia_orders ADD PRIMARY KEY (order_id);'))
        con.execute(text('ALTER TABLE ariztia_products ADD PRIMARY KEY (sku);'))
        con.execute(text('ALTER TABLE ariztia_grid ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;'))
        con.execute(text('ALTER TABLE ariztia_clients_pending ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;'))
        con.execute(text('ALTER TABLE info_orders ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;'))
        con.execute(text('ALTER TABLE ariztia_metrics ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;'))
        con.execute(text('ALTER TABLE grid_sap ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;'))
        con.execute(text('ALTER TABLE orders_sap ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;'))

        # Claves foráneas
        con.execute(text('ALTER TABLE ariztia_clients ADD CONSTRAINT fk_centro FOREIGN KEY (centro) REFERENCES cd(center);'))
        con.execute(text('ALTER TABLE ariztia_orders ADD CONSTRAINT fk_rut FOREIGN KEY (rut) REFERENCES ariztia_clients(usu_rut);'))
        con.execute(text('ALTER TABLE ariztia_grid ADD CONSTRAINT fk_order_id FOREIGN KEY (order_id) REFERENCES ariztia_orders(order_id);'))
        con.execute(text('ALTER TABLE ariztia_grid ADD CONSTRAINT fk_sku FOREIGN KEY (sku) REFERENCES ariztia_products(sku);'))
        con.execute(text('ALTER TABLE info_orders ADD CONSTRAINT fk_info_or FOREIGN KEY (order_id) REFERENCES ariztia_orders(order_id);'))
        con.execute(text('ALTER TABLE grid_sap ADD CONSTRAINT fk_id_sap FOREIGN KEY (id_sap) REFERENCES orders_sap(id_sap);'))

        print("Relaciones creadas correctamente.")

def save_to_csv(table_name, csv_file_name, engine, chunk_size, verbose=False, exclude_columns=None):
    """
    Exporta una tabla de la base de datos a un archivo CSV en la carpeta 'data', con opción de excluir columnas específicas.
    Args:
        table_name (str): Nombre de la tabla en la base de datos.
        csv_file_name (str): Nombre del archivo CSV de salida.
        engine (sqlalchemy.engine.base.Engine): Conexión a la base de datos.
        chunk_size (int): Tamaño del chunk para leer y escribir.
        verbose (bool): Si es True, imprime mensajes detallados.
        exclude_columns (list): Lista de columnas a excluir de la exportación.
    """
    # Asegurar que la carpeta 'data' exista
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)
    # Definir la ruta completa del archivo CSV en la carpeta 'data'
    csv_file_path = os.path.join(data_dir, csv_file_name)
    try:
        for i, chunk in enumerate(pd.read_sql_table(table_name, con=engine, chunksize=chunk_size)):
            # Excluir columnas si se especifican
            if exclude_columns:
                chunk = chunk.drop(columns=exclude_columns, errors='ignore')
            # Configurar modo de escritura y encabezado
            mode = 'w' if i == 0 else 'a'
            header = i == 0
            chunk.to_csv(csv_file_path, mode=mode, header=header, index=False)
            if verbose:
                print(f"Chunk {i+1} exportado con éxito a {csv_file_path}.")
        print(f"Tabla '{table_name}' exportada correctamente a '{csv_file_path}'.")
    except Exception as e:
        print(f"Error al exportar la tabla '{table_name}': {e}")