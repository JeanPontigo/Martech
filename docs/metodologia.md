# Metodología — Por qué Scrum en el Proyecto MarTech

## 1. Decisión

El proyecto se gestionó con **Scrum**, marco ágil de trabajo por sprints con backlog priorizado, ceremonias y roles definidos.

## 2. Justificación

1. **Requisitos evolutivos**: al inicio no se conocía el detalle de las APIs de cada tenant (Magento con customizaciones distintas en PF, Carozzi y Ariztía, más Mailup y GA4). Un plan en cascada habría exigido congelar requisitos imposibles de conocer; Scrum permitió descubrir la complejidad por iteraciones y ajustar el diseño (ej. el overlay de status de Ariztía surgió de un hallazgo en un sprint, no del análisis inicial).
2. **Entrega temprana de valor**: cada sprint dejaba un incremento funcional (primero Bronze de un tenant, luego silver, luego gold), de modo que el avance era verificable por el docente y el equipo desde las primeras semanas, en lugar de un "big bang" al final.
3. **Riesgo técnico alto**: la migración MySQL → BigQuery + dbt implicaba tecnologías nuevas para el equipo. Los sprints cortos acotaron el riesgo: si una decisión (ej. estrategia incremental) fallaba, se detectaba en días, no en meses.
4. **Equipo pequeño y co-ubicado (3 personas)**: Scrum funciona bien con equipos de 3–9; la comunicación diaria informal sustituyó burocracia innecesaria.
5. **Necesidad de retrospectiva**: el proyecto tuvo incidentes reales (bloqueo de Cloudflare a Cloud Run, `.env` en Docker, Cloud Function poco confiable); las retrospectivas dieron el espacio para convertirlos en mejoras de proceso.

## 3. Roles

| Rol Scrum | Responsable | Función en el proyecto |
|---|---|---|
| Product Owner | Jean Carlos Pontigo | Prioriza el backlog según valor para Martech; valida cada incremento |
| Scrum Master | Rodrigo Urbina | Facilita ceremonias, remueve bloqueos (accesos GCP, APIs) |
| Development Team | Los 3 integrantes | Extracción (pipelines), modelado (dbt), documentación y QA |

## 4. Eventos y cadencia

| Evento | Cadencia | Propósito en el proyecto |
|---|---|---|
| Sprint Planning | Cada 2 semanas | Seleccionar historias del Product Backlog (ej. "Bronze de Carozzi") |
| Daily standup | Diario (breve) | Sincronizar avances y bloqueos entre los 3 |
| Sprint Review | Fin de sprint | Demostrar el incremento (ej. modelos silver corriendo en BigQuery) |
| Retrospective | Fin de sprint | Mejorar proceso (ej. "no más secretos en imágenes Docker") |

## 5. Artefactos

- **Product Vision**: plataforma de datos escalable y auditable para Martech (ver `docs/alcance-mvp.md`).
- **Product Backlog**: épicas por capa (extracción Bronze → staging → silver → gold → dashboards), priorizadas por dependencia técnica.
- **Sprint Backlog**: tareas del sprint en curso con responsable.
- **Definition of Done**: el incremento se considera terminado cuando: código en `main`, `dbt compile` sin errores, datos validados contra la fuente y documentado en este repo.

## 6. Adaptaciones pragmáticas

Siendo un equipo de 3 estudiantes con práctica profesional en paralelo, se adoptó un **Scrum liviano**: dailies asincrónicas por chat cuando no había clase presencial y sprints de 2 semanas alineados a las entregas de la asignatura (Fase 1, Fase 2). Lo esencial — backlog priorizado, incremento demostrable y retrospectiva— se mantuvo siempre.
