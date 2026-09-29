# Diagramas UML — Proyecto MarTech (ATLAS)

## 1. Diagrama de componentes

```mermaid
flowchart TB
    subgraph EXT["Extracción (Cloud Run Jobs)"]
        PFJ[pf-job<br/>main_gcp.py]
        MCJ[mc-job<br/>main_gcp_carozzi.py]
        AATNJ[aatn-job<br/>main_gcp_ariztia.py]
        UTILS[utils_gcp_*.py<br/>BigQuery + Pub/Sub + watermarks]
    end
    subgraph BQ["BigQuery: martech-data-platform-atlas"]
        BRONZE[(bronze.*)]
        OPS[(pipeline_state<br/>pipeline_logs)]
        SILVER[(silver.*)]
        GOLD[(gold.*)]
    end
    subgraph ORQ["Orquestación"]
        SCHED[Cloud Scheduler]
        PS[Pub/Sub<br/>3 tópicos]
        CF[Cloud Function]
    end
    subgraph TRF["Transformación"]
        DBT[dbt Cloud<br/>staging → silver → gold]
    end
    subgraph SRC["Fuentes externas"]
        MAG[Magento REST]
        MAIL[Mailup]
        GA4S[GA4 export]
    end
    subgraph BI["Consumo"]
        LS[Looker Studio]
    end

    SCHED --> PFJ & MCJ & AATNJ
    PFJ & MCJ & AATNJ --> UTILS
    UTILS --> BRONZE & OPS
    MAG --> PFJ & MCJ & AATNJ
    MAIL --> BRONZE
    GA4S --> SILVER
    UTILS --> PS --> CF --> DBT
    BRONZE --> DBT --> SILVER --> GOLD --> LS
```

## 2. Diagrama de secuencia — ejecución diaria

```mermaid
sequenceDiagram
    participant S as Cloud Scheduler<br/>(04:00 America/Santiago)
    participant R as Cloud Run Job<br/>(extractor tenant)
    participant M as Magento REST API
    participant B as BigQuery<br/>(bronze + state/logs)
    participant P as Pub/Sub
    participant F as Cloud Function
    participant D as dbt Cloud
    participant L as Looker Studio

    S->>R: dispara job diario
    R->>B: lee watermark (pipeline_state)
    R->>M: GET entidades (delta: nuevos + actualizados)
    M-->>R: JSON paginado
    R->>B: WRITE_APPEND a bronze.ecommerce
    R->>B: actualiza pipeline_state + pipeline_logs
    R->>P: publica mensaje (tenant, estado)
    P->>F: entrega mensaje
    F->>D: dispara corrida martech_atlas
    D->>B: staging (views) → silver (merge) → gold (tables)
    D-->>F: resultado OK/error
    L->>B: dashboards consultan gold
```

## 3. Diagrama de secuencia — manejo de error en extracción

```mermaid
sequenceDiagram
    participant R as Extractor
    participant M as Magento API
    participant B as BigQuery (pipeline_logs)

    R->>M: GET página N
    M-->>R: 429 / timeout
    R->>R: retry con backoff + throttling
    M-->>R: 200 OK
    R->>B: append parcial a bronze
    R->>B: log estado=error (entidad, página)
    Note over R,B: el watermark NO avanza en entidades fallidas;<br/>la próxima corrida reintenta el delta pendiente
```
