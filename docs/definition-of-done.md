# Definition of Done — Proyecto MarTech (Plataforma ATLAS)

**Equipo:** Jean Carlos Pontigo · Rodrigo Urbina · Jaime Vergara
**Aprobado por el equipo.** Una historia de usuario se considera terminada solo si cumple **todos** estos criterios.

---

## 1. Código

- [ ] El código está en el repositorio `JeanPontigo/Martech`, en la rama `main`, con commits atribuidos al autor (nombre y correo de GitHub configurados).
- [ ] **Cero secretos en el código:** ninguna password, token ni API key hardcodeada; todo secreto va por variables de entorno o Secret Manager.
- [ ] El `Dockerfile` del pipeline usa `.dockerignore` (excluye `.env`, `__pycache__`, `*.pyc`).
- [ ] Sin código duplicado evitable entre tenants: la lógica común vive en `utils_*` compartidos.
- [ ] Otro integrante revisó el cambio (revisión cruzada) antes de darlo por terminado.

## 2. Datos y modelos

- [ ] Los modelos dbt compilan: `dbt build` termina sin errores en el entorno de desarrollo.
- [ ] Los modelos incrementales declaran `unique_key` (compuesta con `tenant_id` donde corresponda) y no generan duplicados en re-ejecuciones.
- [ ] Los modelos nuevos están declarados en `sources.yml` / `schema.yml` según corresponda.
- [ ] Los conteos del modelo cuadran contra la fuente (Bronze) en una muestra de validación.

## 3. Pruebas

- [ ] El extractor se probó contra la fuente real (o un sandbox) y maneja el caso de error: registra el fallo en `pipeline_logs` sin avanzar el watermark.
- [ ] Los modelos críticos tienen al menos tests `unique` y `not_null` en las claves, y `dbt test` pasa.
- [ ] La imagen Docker se construye y el `main` del tenant ejecuta de punta a punta en un entorno limpio.

## 4. Documentación

- [ ] El README del repositorio refleja el cambio (si aplica): qué hace el componente y cómo ejecutarlo.
- [ ] Cambios de arquitectura o de modelo de datos quedan registrados en `docs/`.

## 5. Despliegue y operación

- [ ] El pipeline desplegado en Cloud Run ejecuta la carga diaria completa y registra su estado en `pipeline_state`.
- [ ] El mensaje Pub/Sub posterior a la extracción incluye tenant, entidad, estado y conteos.
- [ ] El responsable verifica al día siguiente que la ejecución programada (04:00 hora Chile) terminó correctamente.
