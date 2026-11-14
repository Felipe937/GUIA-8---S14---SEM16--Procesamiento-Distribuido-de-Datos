# Metodología del Proyecto: Procesamiento Distribuido de Datos

## 1. Análisis: identificación del problema y definición de requerimientos

La etapa inicial consistió en comprender el problema del procesamiento distribuido de datos y definir los requerimientos del sistema. El objetivo era implementar un sistema que pudiera procesar datasets de diferentes tamaños (10,000, 100,000 y 500,000 registros) calculando ingresos por región, top productos y ingresos por mes.

**Evidencias relacionadas:**
- `fase1_preparacion_datos.py`: Script de generación y preparación de datos del dataset distribuido.
- `requeriments.txt`: Dependencias del proyecto (pandas, matplotlib, multiprocessing).
- `data/ventas_pequeno.csv` (5,000 registros) y `data/ventas_grande.csv` (1,000,000 de registros): Conjuntos de datos de entrada.

**Definición de requerimientos:**
- Procesar datasets con columnas: región, categoría, cantidad, total_venta, fecha.
- Calcular tres operaciones: ingresos por región, top productos por cantidad, ingresos por mes.
- Comparar ejecución secuencial vs paralelo con 4 workers.
- Validar que los resultados sean idénticos entre ambos enfoques.
- Medir tiempos de ejecución para diferentes volúmenes de datos.
## 2. Diseño: representación de la arquitectura y selección de herramientas
## 3. Implementación: procesamiento de datos y consolidación de resultados
## 4. Experimentación: pruebas con distintos volúmenes de datos
## 5. Evaluación: comparación de tiempos, validación de resultados y análisis de limitaciones
## 6. Documentación: registro de evidencias, conclusiones y referencias

La etapa final consistió en documentar todo el proceso, los resultados obtenidos y las conclusiones derivadas, asegurando que todas las evidencias estén debidamente registradas y tracables.

**Evidencias generadas y archivadas:**
- `resultados_fase4/tabla_rendimiento.csv`: Datos numéricos de rendimiento.
- `resultados_fase4/entorno_pruebas.json`: Características del hardware y entorno.
- `resultados_fase4/comparativa_rendimiento.png`: Representación gráfica de los resultados.
- `ANALISIS_FASE5.md`: Análisis detallado de los 5 cuestionarios de resultados.
- `CONCLUSIONES_FASE6.md`: Conclusiones finales del proyecto.

**Procedimientos de documentación:**
1. Todos los resultados numéricos fueron registrados en `tabla_rendimiento.csv` con promedios y desviaciones estándar de 3 repeticiones por configuración.
2. Las características del entorno fueron registradas automáticamente en `entorno_pruebas.json`.
3. Los gráficos comparativos fueron generados con matplotlib y guardados en `comparativa_rendimiento.png`.
4. Los análisis por preguntas fueron documentados en `ANALISIS_FASE5.md`.
5. Las conclusiones del proyecto fueron resumidas en `CONCLUSIONES_FASE6.md`.

La etapa de evaluación involucró el análisis detallado de los resultados obtenidos, la comparación de tiempos de ejecución y el identificación de limitaciones del enfoque utilizado.

**Evidencias relacionadas:**
- `ANALISIS_FASE5.md`: Documento completo con el análisis de los resultados de la Fase 5.
- `CONCLUSIONES_FASE6.md`: Conclusiones del proyecto basadas en los resultados.
- `resultados_fase4/tabla_rendimiento.csv`: Datos numéricos de las mediciones.

**Aspectos evaluados:**

1. **Comparación de tiempos:** Se compararon los tiempos de ejecución secuencial vs paralelo para los tres volúmenes de datos, observando cómo el speedup varía de 0.38x (10k) a 2.90x (100k) a estimación de 6.77x (500k).

2. **Validación de resultados:** Se verificó que los resultados secuenciales y paralelos sean idénticos para 10,000 y 100,000 registros. Para 500,000 registros, la validación no fue posible debido al límite de tiempo del entorno.

3. **Análisis de limitaciones:**
   - **Overhead de coordinación:** Para volúmenes pequeños, la overhead de particionamiento, transferencia (pickle) y coordinación de workers supera el beneficio del cómputo paralelo.
   - **Paralelismo fijo en 4 workers:** No hay balanceo dinámico ni ajuste automático basado en la carga de trabajo.
   - **Comunicación en un solo nodo:** Los tiempos de transferencia miden serialización en memoria (pickle), no tráfico de red. En un clúster real, los costos serían sustancialmente mayores.
   - **Tiempo secuencial no medido para 500,000:** El entorno de prueba no pudo medir directamente el tiempo secuencial para 500,000 registros (excedió los 30 segundos).

4. **Diferenciación importante:** `multiprocessing` ejecuta procesos locales en un solo equipo con 16 núcleos lógicos, compartiendo memoria limitada y comunicándose mediante serialización en memoria. Esto simula el procesamiento distribuido para fines de evaluación, pero no representa un clúster distribuido real donde los workers residirían en máquinas diferentes y la comunicación tendría latencia de red significativa.

Esta etapa consistió en ejecutar las pruebas de rendimiento con los tres volúmenes de datos especificados y registrar los tiempos de cada proceso.

**Evidencias relacionadas:**
- `resultados_fase4/tabla_rendimiento.csv`: Tabla con los tiempos medidos para 10,000, 100,000 y 500,000 registros.
- `resultados_fase4/comparativa_rendimiento.png`: Gráfico comparativo de tiempos y speedup por volumen de datos.
- `resultados_fase4/entorno_pruebas.json`: Características del entorno donde se ejecutaron las pruebas.

**Resultados de experimentación:**

| Volumen | Secuencial (s) | Paralelo (s) | Speedup | Validación |
|---------|---------------|--------------|---------|------------|
| 10,000 | 0.5684 ± 0.0638 | 1.4881 ± 0.3059 | 0.38x | ✅ Idénticos |
| 100,000 | 6.2098 ± 0.2006 | 2.1414 ± 0.1517 | 2.90x | ✅ Idénticos |
| 500,000 | 31.0490* (extrap.) | 4.5890 | 6.77x* | ⚠️ No validado* |

*Nota sobre 500,000 registros: el tiempo secuencial superó el límite de medición del entorno (más de 30 segundos). El valor de 31.0490 s es una extrapolación basada en la tendencia observada (10k: 0.57 s, 100k: 6.21 s). El procesamiento paralelo y la consolidación sí se midieron directamente. Por esta razón, la validación de resultados idénticos para 500,000 registros no se pudo completar directamente y aparece como "No validado" en la tabla.

**Hallazgos clave:**
- Para 10,000 registros: el paralelo fue más lento debido a la overhead de coordinación.
- Para 100,000 registros: el paralelo alcanzó speedup de 2.90x, volviendo beneficioso el paralelismo.
- Para 500,000 registros: el speedup estimado es de 6.77x, pero debe interpretarse con cautela por la falta de validación directa del secuencial.

La fase de implementación involucró la codificación de las funciones de procesamiento, la generación de datasets y la ejecución de pruebas de rendimiento. Se reutilizó la lógica de la Fase 3 sin modificar sus funciones core.

**Evidencias relacionadas:**
- `fase3_procesamiento_distribuido.py`: Implementación del procesamiento distribuido simulado con las funciones `procesar_particion()`, `consolidar_resultados()` y `ejecucion_secuencial()`.
- `fase4_pruebas_rendimiento.py`: Script de pruebas de rendimiento que ejecuta las pruebas con los volúmenes 10,000, 100,000 y 500,000 registros.
- `fase1_preparacion_datos.py`: Generación del dataset distribuido utilizado en las pruebas.
- `fase2_generar_diagrama.py`: Script auxiliar para diagramación.

**Proceso de implementación:**
1. Generación del dataset utilizando `generar_dataset(n)` con los volúmenes deseados.
2. Particionamiento del dataset en 4 particiones equilibradas: `particiones = [df.iloc[i::NUM_WORKERS].reset_index(drop=True) for i in range(NUM_WORKERS)]`.
3. Ejecución paralela usando `Pool(NUM_WORKERS).map(procesar_particion, datos_part)`.
4. Consolidación de resultados parciales con `consolidar_resultados(resultados_parciales)`.
5. Ejecución secuencial de referencia con `ejecucion_secuencial(df)`.
6. Comparación de resultados mediante `comparar_resultados(resultado_seq, resultado_par)`.
7. Almacenamiento de resultados en `resultados_fase4/tabla_rendimiento.csv`.

En esta etapa se diseñó la arquitectura del sistema basado en multiprocessing de Python para simular nodos distribuidos en un solo equipo. Se decidió usar un modelo de un coordinador que particiona los datos y 4 workers que procesan de forma independiente.

**Evidencias relacionadas:**
- `fase2_generar_diagrama.py`: Generación del diagrama Mermaid de la arquitectura distribuida.
- `fase3_procesamiento_distribuido.py`: Implementación del modelo Map-Reduce simulado con 4 workers.
- `resultados_fase4/entorno_pruebas.json`: Características del hardware (Windows 10, 16 núcleos lógicos, Python 3.11.9).

**Arquitectura diseñada:**
- **Coordinador Principal**: Lee el dataset, lo particiona en N bloques equilibrados y distribuye a los workers.
- **Workers (4)**: Procesan de forma independiente sus particiones calculando ingresos por región, top productos e ingresos por mes.
- **Consolidador Central**: Recibe los resultados parciales de los workers y los suma para obtener los totales globales.
- **Validador**: Compara los resultados de la ejecución secuencial con los paralelos para verificar que sean idénticos.

El uso de `multiprocessing` debe aclararse: ejecuta procesos locales en el mismo equipo y simula el procesamiento distribuido, pero no representa un clúster distribuido real donde los workers residirían en máquinas diferentes y la comunicación ocurriría mediante protocolos de red.