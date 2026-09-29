# Proyecto MarTech — Plataforma de datos ATLAS

## 1. Descripción

**MarTech** es la migración de la plataforma de datos de la empresa Martech (empresa chilena de marketing automation B2B) desde procesos ETL manuales en Python que cargaban a un MySQL local, hacia una plataforma moderna en **Google Cloud Platform**, escalable, auditable y alineada con la normativa chilena de protección de datos (Ley 21.719).

El pipeline diario extrae datos de las fuentes de cada cliente (tenant) — Magento (e-commerce), Mailup (email marketing) y GA4 (analítica web) — los ingesta en crudo a BigQuery (capa Bronze), los transforma con dbt siguiendo la arquitectura medallion (Staging → Silver → Gold) y los expone en dashboards de Looker Studio para el área comercial.

**Tenants:** PF (Productos Fernández), Carozzi y Ariztía.

## 2. Tecnologías

| Capa | Tecnología |
|---|---|
| Extracción | Python 3.11, pandas, requests, SQLAlchemy |
| Orquestación | Cloud Scheduler + Cloud Run (Jobs) |
| Mensajería | Cloud Pub/Sub |
| Data Warehouse | BigQuery (datasets `bronze`, `silver`, `gold`) |
| Transformación | dbt Cloud (proyecto `martech_atlas`) |
| Fuentes | Magento REST API, Mailup, GA4 (export nativo) |
| Visualización | Looker Studio |
| Contenedores | Docker, docker-compose |
| Legacy (reemplazado) | Python + MySQL local |

Ver `docs/arquitectura.md` para el detalle.

## 3. Ejecución local

### Requisitos previos

- Docker y docker-compose instalados.
- Python 3.11+ y [dbt](https://docs.getdbt.com/) (solo para correr transformaciones).
- Archivo `.env` creado a partir de `.env.example` (ver sección 4).
- Credenciales de GCP con acceso a BigQuery (`GOOGLE_APPLICATION_CREDENTIALS`).

### 3.1 Levantar los extractores con docker-compose

```bash
# 1. Copiar y completar variables de entorno (sin valores reales en el repo)
cp .env.example .env

# 2. Construir las imágenes de los 3 pipelines
docker-compose build

# 3. Ejecutar extracción completa de un tenant
docker-compose run --rm pipeline-pf
docker-compose run --rm pipeline-mc
docker-compose run --rm pipeline-aatn

# 4. Ejecutar solo una entidad (ej: solo órdenes de PF)
docker-compose run --rm pipeline-pf python3 main_gcp.py --entity orders
```

Cada servicio escribe en crudo a `bronze.ecommerce` / `bronze.email_mailup` en BigQuery y publica un mensaje al tópico Pub/Sub correspondiente (`pipeline-pf`, `pipeline-carozzi`, `pipeline-ariztia`).

### 3.2 Correr las transformaciones dbt

```bash
# Instalar dependencias de dbt
dbt deps

# Cargar seeds (ej: comunas_regiones)
dbt seed

# Ejecutar todos los modelos (staging → silver → gold)
dbt run

# Ejecutar solo una capa
dbt run --select staging
dbt run --select silver
dbt run --select gold

# Validar
dbt test
```

El perfil de conexión es `martech_atlas` (ver `dbt_project.yml`).

## 4. Variables de entorno

Todas las credenciales se configuran por variables de entorno; **ningún secreto se versiona en el repo**. La lista completa de nombres está en [`.env.example`](./.env.example) (sin valores). Ejemplo de uso:

```bash
cp .env.example .env
# editar .env con los valores reales (no commitear)
```

> Nota: el despliegue productivo debe migrar estos valores a **Secret Manager** de GCP.

## 5. Integrantes y roles

| Integrante | Rol |
|---|---|
| Jaime Vergara | Líder técnico — pipelines de extracción en GCP, repositorio y despliegue |
| Rodrigo Urbina | Modelado y transformación de datos (dbt: staging/silver/gold) |
| Jean Carlos Pontigo | Documentación, QA y validación de datos |

*(Distribución de referencia del equipo; el aporte detallado por persona consta en el historial de commits del repositorio.)*

## 6. Metodología

El proyecto se gestionó con **Scrum**: sprints cortos, backlog priorizado por valor de negocio y ceremonias de planificación, revisión y retrospectiva. Ver justificación completa en [`docs/metodologia.md`](./docs/metodologia.md).

## 7. Arquitectura

Pipeline diario (04:00 hora de Chile):

```
Cloud Scheduler → Cloud Run (extractores por tenant) → BigQuery Bronze
    → Pub/Sub → Cloud Function → dbt Cloud (Silver/Gold) → Looker Studio
```

Detalle, diagramas y decisiones en [`docs/arquitectura.md`](./docs/arquitectura.md). Modelo de datos en [`docs/modelo-datos.md`](./docs/modelo-datos.md).

## 8. Documentación adicional

- [`docs/arquitectura.md`](./docs/arquitectura.md) — arquitectura y diagrama del pipeline
- [`docs/modelo-datos.md`](./docs/modelo-datos.md) — diagrama entidad-relación
- [`docs/diagramas-uml.md`](./docs/diagramas-uml.md) — diagramas de componentes y secuencia
- [`docs/requisitos-no-funcionales.md`](./docs/requisitos-no-funcionales.md) — RNF
- [`docs/manual-tecnico.md`](./docs/manual-tecnico.md) — despliegue y operación
- [`docs/plan-de-pruebas.md`](./docs/plan-de-pruebas.md) — plan de pruebas
- [`docs/innovacion.md`](./docs/innovacion.md) — innovación vs. sistema legacy
- [`docs/metodologia.md`](./docs/metodologia.md) — justificación de Scrum
- [`docs/alcance-mvp.md`](./docs/alcance-mvp.md) — alcance del MVP
