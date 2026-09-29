# Retrospectivas — Proyecto MarTech (Plataforma ATLAS)

**Equipo:** Jean Carlos Pontigo · Rodrigo Urbina · Jaime Vergara
**Formato:** qué salió bien / qué mejorar / acciones comprometidas.

---

## Retrospectiva Sprint 1 — Fundaciones e ingesta PF

### Qué salió bien
- Levantamos el repositorio con una estructura clara por tenant (`pipelines/`, `legacy_etl_mysql/`, `models/`) que nos acompañó todo el proyecto.
- La primera extracción de Magento a `bronze.ecommerce` funcionó al tercer intento y validamos conteos contra la fuente.
- Definir desde el inicio las tablas `pipeline_state` y `pipeline_logs` nos dio trazabilidad; fue una de las mejores decisiones técnicas.

### Qué mejorar
- Subestimamos el tiempo de pelear con credenciales y permisos de GCP; perdimos casi dos días en configuración.
- Los commits quedaron repartidos en más de una identidad de Git por integrante, lo que después complicó la auditoría de aportes.

### Acciones
- [ ] Documentar el setup de GCP (proyecto, service accounts, permisos) para no repetir la configuración a ciegas. → **JV**
- [ ] Cada integrante fija `user.name` y `user.email` de su cuenta GitHub en todos sus entornos. → **Todos**

---

## Retrospectiva Sprint 2 — Transformación dbt y tenant Carozzi

### Qué salió bien
- El modelado medallion en dbt quedó sólido: staging como vistas, Silver incremental con `merge` y Gold como tablas de negocio.
- Incorporamos el tenant Carozzi (email vía Mailup) reutilizando gran parte del extractor de PF.

### Qué mejorar
- **Incidente Cloudflare:** las llamadas salientes hacia Cloud Run fueron bloqueadas por Cloudflare y lo descubrimos tarde, porque asumimos que era un error de nuestro código. Nos faltó un checklist de diagnóstico de red antes de culpar al código.
- La Cloud Function que debía disparar dbt tras cada extracción resultó poco confiable como trigger; el diseño asumía que "siempre se ejecuta" y no era así.
- Duplicamos demasiada lógica entre tenants (~64% del código es idéntico), lo que ya empezó a generar bugs divergentes.

### Acciones
- [ ] Agregar reintentos con backoff y registro del error real (HTTP status + cuerpo) en todos los extractores, para diagnosticar bloqueos de red sin adivinar. → **RU**
- [ ] Rediseñar el trigger post-extracción: mensaje Pub/Sub con payload JSON (tenant, entidad, estado, conteos) y no disparar dbt si la extracción falló. → **JV**
- [ ] Extraer la lógica común de los tres pipelines a un paquete compartido. → **JP**

---

## Retrospectiva Sprint 3 — Tenant Ariztía, Gold y visualización

### Qué salió bien
- El tercer tenant (Ariztía) se integró mucho más rápido que los dos primeros: la curva de aprendizaje rindió frutos.
- Los modelos Gold (`customer_360`, `kpi_orders_daily`, `campaign_performance`) empezaron a responder las preguntas reales del negocio.

### Qué mejorar
- **Incidente `.env`:** detectamos que el `Dockerfile` de un pipeline copiaba el `.env` dentro de la imagen. Fue un descuido grave tratándose de credenciales; el `.dockerignore` debió existir desde el sprint 1.
- El pipeline de Carozzi sigue dependiendo en parte de un proceso transitorio que corre desde un VPS, fuera de la plataforma. Es deuda técnica conocida y documentada, pero hay que cerrarla.
- Los dashboards avanzaron más lento de lo planeado porque priorizamos estabilizar la ingesta.

### Acciones
- [ ] Agregar `.dockerignore` en todos los Dockerfiles, reconstruir imágenes y **rotar todas las credenciales** que pudieron quedar expuestas; migrar a Secret Manager. → **JP**
- [ ] Plan de migración del proceso transitorio de Carozzi (VPS → Cloud Run) con fecha comprometida. → **JV**
- [ ] Completar un dashboard por tenant sobre capa Gold antes del cierre. → **RU**

---

**Seguimiento:** las acciones pendientes se revisan al inicio de cada sprint planning. Las marcadas como deuda de seguridad (credenciales, `.env`) tienen prioridad sobre funcionalidad nueva.
