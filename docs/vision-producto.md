# Visión de Producto — Proyecto MarTech (Plataforma ATLAS)

**Equipo:** Jean Carlos Pontigo · Rodrigo Urbina · Jaime Vergara
**Cliente:** Martech — empresa chilena de marketing y analítica B2B
**Versión:** 1.0

---

## 1. Para quién es el producto

- **Equipo de datos y operaciones de Martech**, que hoy mantiene a mano los procesos de carga de datos.
- **Áreas comerciales y de marketing de los tenants PF, Carozzi y Ariztía**, que necesitan reportes confiables y diarios sobre ventas, clientes y campañas.
- **Dirección de Martech**, que requiere una plataforma auditable y conforme a la normativa chilena de protección de datos (Ley 21.719).

## 2. Problema que resuelve

Los datos de los tenants se extraen con scripts Python escritos a mano (`legacy_etl_mysql/`) que cargan a un **MySQL local**:

- El servidor se queda sin espacio y no escala con el volumen de órdenes y eventos.
- No hay trazabilidad: si una carga falla a mitad de camino, no se sabe qué quedó cargado ni qué falta.
- Las credenciales y la lógica están acopladas a scripts por tenant, difíciles de mantener y de auditar.
- Los reportes se construyen sobre datos sin capas de calidad, lo que genera desconfianza en las cifras que ve el negocio.

## 3. Propuesta de valor

Migrar la plataforma a **Google Cloud** con arquitectura **medallion** sobre BigQuery:

- **Bronze:** ingesta cruda inmutable (JSON tal como llega de la fuente) con bitácora de estado por ejecución.
- **Silver:** datos filtrados, limpiados y enriquecidos con dbt (modelos incrementales con `merge`).
- **Gold:** tablas a nivel de negocio listas para consumo (KPIs diarios, customer 360, performance de campañas y productos).
- Extracción diaria automatizada con **Cloud Run Jobs** por tenant, orquestación por **Pub/Sub** y visualización en **Looker Studio**.

El negocio pasa de "scripts que alguien corre" a "una plataforma que corre sola todos los días y deja evidencia de lo que hizo".

## 4. Objetivos medibles

| # | Objetivo | Meta |
|---|----------|------|
| O1 | Ejecución diaria completa del pipeline antes de las 06:00 (hora Chile) | 95% de los días del mes |
| O2 | Tenants migrados a la plataforma (PF, Carozzi, Ariztía) | 3 de 3 con extracción diaria operativa |
| O3 | Trazabilidad de cada ejecución (estado, conteos, errores) registrada en BigQuery | 100% de las ejecuciones |
| O4 | Dashboards de Looker Studio consumiendo capa Gold | Al menos 1 dashboard por tenant |
| O5 | Cero credenciales en el código versionado (uso de variables de entorno / Secret Manager) | 0 secretos en el repositorio |

## 5. Alcance y límites

**Dentro del alcance:** extracción desde Magento (ecommerce), Mailup (email) y GA4 (eventos web); modelado medallion en BigQuery con dbt; dashboards base en Looker Studio; documentación técnica y de operación.

**Fuera del alcance (fase actual):** enmascaramiento de datos personales a nivel de columna (planificado como mejora para cumplimiento de Ley 21.719); Row-Level Security por tenant; ingesta en tiempo real (el diseño es batch diario).
