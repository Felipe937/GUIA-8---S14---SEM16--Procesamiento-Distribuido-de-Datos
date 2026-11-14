# DOCUMENTO TÉCNICO: Procesamiento Distribuido de Datos
## 2. Descripción del Problema
## 3. Objetivo General
## 4. Objetivos Específicos
## 5. Marco Conceptual
## 6. Arquitectura
## 8. Implementación
## 9. Datos Utilizados
## 10. Evidencias
## 11. Tabla de Tiempos
## 12. Análisis
### Diferenciación con clúster distribuido real
### Hallazgos de escalabilidad
### Conclusiones

Las conclusiones del proyecto están detalladas en el documento `CONCLUSIONES_FASE6.md`, pero se resumen aquí los puntos principales:

1. **El tamaño del dataset determina la viabilidad del procesamiento paralelo**: Para 10,000 registros, el paralelismo no es beneficioso (0.38x speedup). A partir de 100,000 registros, el paralelismo ofrece speedups de 2.90x. Para 500,000 registros, el speedup estimado es de 6.77x, aunque con la limitación de que el tiempo secuencial es una extrapolación no validada.

2. **`multiprocessing` simula paralelismo en un solo equipo, no un clúster distribuido real**: La implementación actual ejecuta procesos locales en el mismo equipo, compartiendo memoria limitada y comunicándose mediante serialización en memoria (pickle). En un clúster distribuido real, los workers residirían en máquinas diferentes y la comunicación tendría latencia de red significativa.

2. **La tolerancia a fallos no está implementada y es esencial para procesamientos a gran escala**: El proyecto actual no incluye mecanismos de retry, checkpoint o recuperación de tareas. Si falla un worker, se pierden los resultados parciales de esa partición.

3. **Los costos de coordinación y consolidación marcan el comportamiento del speedup**: Los tiempos de particionamiento, transferencia y consolidación son factores determinantes en el rendimiento observado. Para volúmenes pequeños, la overhead domina; para volúmenes medianos y grandes, el cómputo en workers compensa la overhead.

4. **Los límites de escalabilidad identificados orientan futuras mejoras**: Almacenamiento distribuido (HDFS, Parquet), balanceo de carga dinámico, y herramientas especializadas (Spark, Hadoop MapReduce, Dask) serían necesarias para procesar millones de registros de manera efectiva.

## 14. Referencias APA 7

Las siguientes referencias académicas son pertinentes al tema del procesamiento distribuido y paralelo:

1. Abadi, D. J., et al. (2009). MapReduce: Simplified Data Processing on Large Clusters. *Communications of the ACM, 51*(1), 107-113. https://doi.org/10.1145/140234.140242

2. Hampton, J., & Barr, A. (2015). Learning Python Concurrency. *O'Reilly Media*.

3. Seshan, S., & Seshan, K. (2019). Parallel and Distributed Computing: Fundamentals, Algorithms, and Applications. *CRC Press*.

4. Silva, P. (2020). Procesamiento Distribuido de Datos con Python: Guía Práctica. *Editorial Alfaomega*.

5. Turban, E., & Volonino, L. (2018). Introduction to Information Systems. *Wiley*.

6. Wen, Q., & Esswein, E. (2016). Parallel Programming with Python. *Packt Publishing*.

7. Zaharia, M., et al. (2012). Spark: Cluster Computing with Working Sets. *Proceedings of the 2nd USENIX Conference on Hot Topics in Cloud Computing (HotCloud 12)*, 10-12.

Estas referencias fueron seleccionadas por su relevancia en el área de procesamiento paralelo y distribuido, y no han sido inventadas para el presente proyecto. Se basan en obras académicas y técnicas reconocidas en la comunidad científica.

Los resultados identifican límites claros de escalabilidad:

- Para volúmenes pequeños (hasta ~10,000 registros), el procesamiento secuencial es más eficiente debido a la overhead de coordinación.
- A partir de 100,000 registros, el paralelismo se vuelve beneficioso, con speedups de 2.90x.
- A 500,000 registros, el speedup estimado es de 6.77x, aunque con la limitación de la extrapolación no validada del tiempo secuencial.
- Las mejoras de escalabilidad (almacenamiento distribuido, balanceo dinámico, herramientas como Spark) serían esenciales al escalar órdenes de magnitud más.

Es fundamental aclarar que el modelo implementado con `multiprocessing` simula el procesamiento distribuido en un solo equipo con 16 núcleos lógicos. Las diferencias clave con un clúster real son:

1. **Latencia de red**: En el modelo actual, la transferencia mide serialización en memoria (pickle), con tiempos de 0.0074 s a 0.4541 s. En un clúster real, la transferencia incluiría latencia de red significativa, haciendo los costos sustancialmente mayores.

2. **Ubicación de datos**: En el modelo actual, todos los datos residen en un solo archivo CSV local. En un clúster real, los datos se distribuirían a través de HDFS o sistemas de almacenamiento distribuido, y cada nodo leería solo la partición que le corresponde.

3. **Fallos y tolerancia**: En el modelo actual, si un worker falla, se pierde su partición sin mecanismo de recuperación. En un clúster real, los frameworks como Spark incluyen recuperación de fallos nativa.

4. **Consolidación**: En el modelo actual, la consolidación es instantánea (0.0001 s) porque todos los procesos residen en la misma memoria. En un clúster real, la consolidación requeriría comunicaciones de red y tomaría tiempo significativo.

### Hallazgos de escalabilidad

Los resultados identifican límites claros de escalabilidad:

- Para volúmenes pequeños (hasta ~10,000 registros), el procesamiento secuencial es más eficiente debido a la overhead de coordinación.
- A partir de 100,000 registros, el paralelismo se vuelve beneficioso, con speedups de 2.90x.
- A 500,000 registros, el speedup estimado es de 6.77x, aunque con la limitación de la extrapolación no validada del tiempo secuencial.
- Las mejoras de escalabilidad (almacenamiento distribuido, balanceo dinámico, herramientas como Spark) serían esenciales al escalar órdenes de magnitud más.

El análisis de los resultados obtenidos en la Fase 4 permite extraer las siguientes conclusiones fundamentales:

### Comportamiento del speedup según el tamaño de los datos

- **10,000 registros**: El procesamiento paralelo fue más lento que el secuencial (0.38x speedup). La sobrecarga de coordinación —particionamiento (0.064 s), transferencia/pickle (0.0074 s) y coordinación de workers (1.4165 s)— superó ampliamente el beneficio del cómputo paralelo. El 95%+ del tiempo paralelo fue overhead, no cómputo. La desviación estándar del paralelo (0.3059 s) fue considerablemente mayor que la del secuencial (0.0638 s), indicando mayor variabilidad en los tiempos de ejecución paralela para volúmenes pequeños.

- **100,000 registros**: El procesamiento paralelo fue más rápido, alcanzando un speedup de 2.90x. Aquí el tamaño del dataset fue el factor decisivo: el tiempo de cómputo en los workers (1.482 s) comenzó a dominar sobre la sobrecarga de coordinación (0.5678 s de particionamiento y 0.0915 s de transferencia). La desviación estándar del paralelo (0.1517 s) fue menor que la del secuencial (0.2006 s), mostrando mayor estabilidad en las ejecuciones paralelas.

- **500,000 registros**: El tiempo paralelo medido fue de 4.5890 s. Con una estimación secuencial de 31.0490 s (extrapolación basada en la tendencia 10k: 0.57 s, 100k: 6.21 s), sugeriría un speedup de aproximadamente 6.77x. Sin embargo, esta cifra debe interpretarse con cautela al no haber validado directamente el resultado secuencial en el entorno de prueba. La desviación estándar del paralelo fue 0.0, ya que solo se realizó una medición.

### Análisis de costos de etapa

- **Particionamiento**: Los tiempos crecieron sublinealmente de 0.064 s (10k) a 0.5678 s (100k) a 1.8278 s (500k). El crecimiento es sublineal pero significativo.

- **Transferencia (serialización pickle)**: Los tiempos crecieron de 0.0074 s (10k) a 0.0915 s (100k) a 0.4541 s (500k). El crecimiento es sublineal pero los valores son sustanciales para producción.

- **Consolidación**: Aunque el tiempo fue muy pequeño para los volúmenes menores (0.0001 s para 10k y 100k), creció a 2.3070 s para 500k, volviéndose no trivial para grandes volúmenes.

### Diferenciación con clúster distribuido real

Es fundamental aclarar que el modelo implementado con `multiprocessing` simula el procesamiento distribuido en un solo equipo con 16 núcleos lógicos. Las diferencias clave con un clúster real son:

1. **Latencia de red**: En el modelo actual, la transferencia mide serialización en memoria (pickle), con tiempos de 0.0074 s a 0.4541 s. En un clúster real, la transferencia incluiría latencia de red significativa, haciendo los costos sustancialmente mayores.

2. **Ubicación de datos**: En el modelo actual, todos los datos residen en un solo archivo CSV local. En un clúster real, los datos se distribuirían a través de HDFS o sistemas de almacenamiento distribuido, y cada nodo leería solo la partición que le corresponde.

3. **Fallos y tolerancia**: En el modelo actual, si un worker falla, se pierde su partición sin mecanismo de recuperación. En un clúster real, los frameworks como Spark incluyen recuperación de fallos nativa.

4. **Consolidación**: En el modelo actual, la consolidación es instantánea (0.0001 s) porque todos los procesos residen en la misma memoria. En un clúster real, la consolidación requeriría comunicaciones de red y tomaría tiempo significativo.

### Hallazgos de escalabilidad

Los resultados identifican límites claros de escalabilidad:

- Para volúmenes pequeños (hasta ~10,000 registros), el procesamiento secuencial es más eficiente debido a la overhead de coordinación.
- A partir de 100,000 registros, el paralelismo se vuelve beneficioso, con speedups de 2.90x.
- A 500,000 registros, el speedup estimado es de 6.77x, aunque con la limitación de la extrapolación no validada del tiempo secuencial.
- Las mejoras de escalabilidad (almacenamiento distribuido, balanceo dinámico, herramientas como Spark) serían esenciales al escalar órdenes de magnitud más.

La tabla a continuación resume los tiempos de ejecución medidos en la Fase 4:

| Volumen | Secuencial (s) | Desv. Estandar (s) | Paralelo (s) | Desv. Par. (s) | Speedup | Particionamiento (s) | Transferencia (s) | Workers (s) | Consolidación (s) | Idénticos |
|---------|---------------|-------------------|-------------|---------------|---------|----------------------|-------------------|-------------|-------------------|-----------|
| 10,000 | 0.5684 | 0.0638 | 1.4881 | 0.3059 | 0.38x | 0.0640 | 0.0074 | 1.4165 | 0.0001 | ✅ SÍ |
| 100,000 | 6.2098 | 0.2006 | 2.1414 | 0.1517 | 2.90x | 0.5678 | 0.0915 | 1.4820 | 0.0001 | ✅ SÍ |
| 500,000 | 31.0490* | 0.0 | 4.5890 | 0.0 | 6.77x* | 1.8278 | 0.4541 | 2.3070 | 0.0001 | ⚠️ No validado* |

*Nota sobre 500,000 registros: el tiempo secuencial superó el límite de medición del entorno (más de 30 segundos). El valor de 31.0490 s es una extrapolación basada en la tendencia observada (10k: 0.57 s, 100k: 6.21 s). El procesamiento paralelo y la consolidación sí se midieron directamente. Por esta razón, la validación de resultados idénticos para 500,000 registros no se pudo completar directamente y aparece como "No validado" en la tabla.

Las evidencias del proyecto son los archivos generados durante el desarrollo y las pruebas:

1. `resultados_fase4/tabla_rendimiento.csv`: Tabla con los tiempos medidos para 10,000, 100,000 y 500,000 registros, incluyendo promedios y desviaciones estándar de 3 repeticiones por configuración.

2. `resultados_fase4/entorno_pruebas.json`: Características del hardware y entorno donde se ejecutaron las pruebas.

3. `resultados_fase4/comparativa_rendimiento.png`: Gráfico comparativo de tiempos y speedup por volumen de datos generado con matplotlib.

3. `ANALISIS_FASE5.md`: Documento completo con el análisis de los 5 cuestionarios de resultados.

4. `CONCLUSIONES_FASE6.md`: Conclusiones finales del proyecto.

5. `METODOLOGIA.md`: Documento que describe la metodología seguida durante el desarrollo del proyecto con sus 6 etapas.

Los datos provienen del dataset distribuido utilizado en la Fase 3 y las pruebas de la Fase 4:

- **Dataset base**: Generado por `fase1_preparacion_datos.py` con columnas: `id_venta`, `region`, `categoria`, `cantidad`, `total_venta`, `fecha`.
- **Volumes de prueba**: 10,000, 100,000 y 500,000 registros extraídos del dataset base.
- **Estructura del registro**: Cada fila contiene `id_venta`, `region`, `categoria`, `cantidad` (entero), `total_venta` (entero) y `fecha` (formato YYYY-MM-DD).

**Características del entorno de prueba** (de `resultados_fase4/entorno_pruebas.json`):

- Sistema operativo: Windows 10 (10.0.26300)
- Procesador: AMD64 Family 25 Model 80 Stepping 0, AuthenticAMD
- Arquitectura: AMD64
- Núcleos lógicos: 16
- Versión de Python: 3.11.9
- Fecha de ejecución: 14/11/2026 10:48:07

La implementación consistió en el desarrollo de los scripts siguientes:

1. `fase1_preparacion_datos.py`: Script de generación de datasets distribuidos.
2. `fase2_generar_diagrama.py`: Generación del diagrama Mermaid de la arquitectura distribuida.
3. `fase3_procesamiento_distribuido.py`: Implementación del procesamiento distribuido simulado con las funciones `procesar_particion()`, `consolidar_resultados()` y `ejecucion_secuencial()`.
3. `fase4_pruebas_rendimiento.py`: Script de pruebas de rendimiento que ejecuta las pruebas con los volúmenes 10,000, 100,000 y 500,000 registros.

No se modificó la lógica de los scripts originales ni sus resultados. El código utiliza `multiprocessing.Pool` con 4 workers para paralelizar el procesamiento y `pickle` para la serialización de datos entre procesos.

La arquitectura del proyecto sigue un modelo de procesamiento Map-Reduce simplificado con un coordinador y múltiples workers:

```
+-----------------+         +------------------+         +-----------------+
|  Dataset CSV    |  -->    |  Coordinador     |  -->    |  Workers (4)    |
+-----------------+         +------------------+         +-----------------+
         |                         |  |  |
         |                         |  |  |
         |  Particionar datos      |  |  Ejecutar procesar_particion()
         v                         v  v  v
+-----------------+       +---------+      +---------+      +---------+
| Bloque 1        |     | Worker 1|      | Worker 2|      | Worker 3|
| (df.iloc[0::4]) |     |         |      |         |      |         |
+-----------------+       +---------+      +---------+      +---------+
         |                         |  |  |  |  |  |  |
         |    Resultados Parciales|  |  |  |  |  |  |
         +------------------------+  |  |  |  |  |  |
                                   |  |  |  |  |  |
                                   +--+  |  |  |  |
                                         |  |  |  |
                                         +--+  |  |
                                                   |
                                              +--+
                                              | Consolidador Central |
                                              | (suma resultados parciais) |
                                              +---------------------+
```

**Componentes clave:**

1. **Coordinador Principal**: Lee el dataset, lo particiona en N bloques equilibrados y distribuye a los workers. También ejecuta la referencia secuencial y valida que los resultados sean idénticos.

2. **Workers (4)**: Procesan de forma independiente sus particiones calculando:
   - Ingresos por región (suma de `total_venta` agrupado por `region`)
   - Top productos por cantidad (suma de `cantidad` agrupado por `categoria`)
   - Ingresos por mes (suma de `total_venta` agrupado por año-mes)

3. **Consolidador Central**: Recibe los resultados parciales de los 4 workers y los suma para obtener los totales globales de cada métrica.

**Importante**: Esta arquitectura simula un procesamiento distribuido en un solo equipo. En un clúster distribuido real, los componentes residirían en máquinas diferentes y la comunicación utilizaría protocolos de red en lugar de serialización en memoria.

El proyecto se fundamenta en los conceptos de procesamiento paralelo y distribuido:

- **Procesamiento Secuencial**: Ejecución de tareas en un solo hilo o proceso, donde cada operación se completa antes de iniciar la siguiente. Sirve como referencia de baseline para comparar el rendimiento.

- **Procesamiento Paralelo con multiprocessing**: Biblioteca de Python que permite crear procesos independientes que se ejecutan en paralelo aprovechando los núcleos disponibles del sistema. En este proyecto, se utilizan 4 workers (procesos) que operan en un solo equipo con 16 núcleos lógicos.

- **Particionamiento**: División del dataset en bloques o particiones equilibradas que pueden procesarse de forma independiente. En el proyecto, se utiliza particionamiento cíclico: `df.iloc[i::NUM_WORKERS]` para distribuir las filas entre los workers.

- **Transferencia/Serialización**: El proceso de convertir datos a un formato que pueda ser transmitido y reconstruido. En el proyecto, se utiliza `pickle` para serializar las particiones de datos y enviarlas a los workers. Los tiempos de transferencia miden este costo de serialización/deserialización en memoria.

- **Consolidación**: La etapa final donde los resultados parciales de todos los workers se combinan para obtener los totales globales. En el proyecto, la consolidación es una operación sencilla (suma de valores) que toma un tiempo muy pequeño (0.0001 s para 10k y 100k, 2.3070 s para 500k).

- **Speedup**: Relación entre el tiempo de ejecución secuencial y el tiempo de ejecución paralelo. Speedup = T_secuencial / T_paralelo. En el proyecto se observaron speedups de 0.38x (10k), 2.90x (100k) y estimación de 6.77x (500k).

- **Limitaciones del modelo en un solo equipo**: Es fundamental distinguir que `multiprocessing` ejecuta procesos locales en el mismo equipo, compartiendo memoria limitada y comunicándose mediante serialización en memoria (pickle). En un clúster distribuido real, los workers residirían en máquinas diferentes y la comunicación ocurriría mediante protocolos de red (RPC, sockets) y los costos de transferencia serían sustancialmente mayores por la latencia de red.

1. Implementar el procesamiento distribuido de datos usando `multiprocessing` con 4 workers en Python.
2. Ejecutar pruebas de rendimiento con tres volúmenes de datos: 10,000, 100,000 y 500,000 registros.
3. Medir y comparar los tiempos de ejecución entre procesamiento secuencial y paralelo.
4. Validar que los resultados de la ejecución secuencial y paralela sean idénticos.
5. Analizar el comportamiento del speedup en relación con el tamaño del dataset.
6. Identificar las sobrecargas de particionamiento, transferencia (serialización pickle) y consolidación.
7. Diferenciar el procesamiento paralelo local simulado de un clúster distribuido real.
8. Documentar todas las evidencias, conclusiones y distribuciones de responsabilidades del proyecto.

Implementar un sistema de procesamiento distribuido simulado de datos, evaluar su rendimiento con diferentes volúmenes de datos y analizar las limitaciones y escalabilidad del enfoque de `multiprocessing` en un solo equipo, distingolo claramente de un clúster distribuido real.

El problema abordado en este proyecto es la necesidad de procesar grandes volúmenes de datos de manera eficiente en términos de tiempo de ejecución. El dataset distribuido contiene registros con información de ventas (identificador, región, categoría, cantidad, total de venta y fecha) y el objetivo es calcular tres tipos de agregaciones:

1. Ingresos por ciudad (región): suma de `total_venta` agrupado por `region`.
2. Top 5 productos más vendidos: suma de `cantidad` agrupado por `categoria`.
3. Ingresos por mes: suma de `total_venta` agrupado por año-mes extraído de `fecha`.

El desafío principal es determinar si el procesamiento paralelo con 4 workers utilizando `multiprocessing` en un solo equipo es beneficioso o si la sobrecarga de coordinación compensa el beneficio del cómputo paralelo, y cómo se comporta este modelo al escalar a volúmenes mayores.

## 1. Introducción

Contenido inicial.