# Conclusiones del Proyecto: Procesamiento Distribuido de Datos

## Conclusión 1: El tamaño del dataset determina la viabilidad del procesamiento paralelo

El análisis demuestra que el procesamiento paralelo con `multiprocessing` no es beneficioso por sí mismo; su efectividad depende críticamente del volumen de datos a procesar. Para 10,000 registros, el tiempo de procesamiento paralelo fue de 1.4881 s frente a 0.5684 s del secuencial, resultando en un speedup de 0.38x. Este resultado indica que la sobrecarga de coordinación —particionamiento (0.064 s), transferencia/pickle (0.0074 s) y coordinación de workers (1.4165 s)— superó ampliamente el beneficio del cómputo paralelo en volúmenes pequeños. Los datos confirman que el 95%+ del tiempo paralelo fue overhead, no cómputo.

Para 100,000 registros, el escenario cambió sustancialmente: el secuencial requirió 6.2098 s y el paralelo 2.1414 s, logrando un speedup de 2.90x. Aquí el tamaño del dataset fue el factor decisivo: el tiempo de cómputo en los workers comenzó a dominar sobre la sobrecarga de coordinación. Para 500,000 registros, el tiempo paralelo medido fue de 4.5890 s, con una estimación secuencial de 31.0490 s (extrapolado), lo que sugeriría un speedup de 6.77x, aunque esta cifra debe interpretarse con cautela al no haber validado directamente el resultado secuencial.

**Evidencia:** `resultados_fase4/tabla_rendimiento.csv` muestra los tiempos reales para los tres volúmenes, y `entorno_pruebas.json` confirma el hardware (Windows 10, 16 núcleos lógicos, Python 3.11.9).
# Conclusiones del Proyecto: Procesamiento Distribuido de Datos

## Conclusión 1: El tamaño del dataset determina la viabilidad del procesamiento paralelo

El análisis demuestra que el procesamiento paralelo con `multiprocessing` no es beneficioso por sí mismo; su efectividad depende críticamente del volumen de datos a procesar. Para 10,000 registros, el tiempo de procesamiento paralelo fue de 1.4881 s frente a 0.5684 s del secuencial, resultando en un speedup de 0.38x. Este resultado indica que la sobrecarga de coordinación —particionamiento (0.064 s), transferencia/pickle (0.0074 s) y coordinación de workers (1.4165 s)— superó ampliamente el beneficio del cómputo paralelo en volúmenes pequeños. Los datos confirman que el 95%+ del tiempo paralelo fue overhead, no cómputo.

Para 100,000 registros, el escenario cambió sustancialmente: el secuencial requirió 6.2098 s y el paralelo 2.1414 s, logrando un speedup de 2.90x. Aquí el tamaño del dataset fue el factor decisivo: el tiempo de cómputo en los workers comenzó a dominar sobre la sobrecarga de coordinación (1.482 s de cómputo vs. 0.5678 s de particionamiento y 0.0915 s de transferencia). Para 500,000 registros, el tiempo paralelo medido fue de 4.5890 s, con una estimación secuencial de 31.0490 s (extrapolado), lo que sugeriría un speedup de 6.77x, aunque esta cifra debe interpretarse con cautela al no haber validado directamente el resultado secuencial en el entorno de prueba.

**Evidencia:** `resultados_fase4/tabla_rendimiento.csv` muestra los tiempos reales para los tres volúmenes, y `entorno_pruebas.json` confirma el hardware (Windows 10, 16 núcleos lógicos, Python 3.11.9).
## Conclusión 3: La tolerancia a fallos no está implementada y es esencial para procesamientos a gran escala

El proyecto actual no incluye mecanismos de tolerancia a fallos. Si uno de los 4 workers fallara antes de entregar sus resultados, las consecuencias serían la pérdida de los resultados parciales de esa partición y un resultado global incompleto o incorrecto. No existiría validación posible sin reejecutar todo el proceso. En el framework `multiprocessing` tal como está implementado, no hay manejo integrado de fallos: si un proceso crashea, la excepción es no capturada y se pierde esa partición.

Para escalar a millones de registros, la tolerancia a fallos se vuelve crítica. La probabilidad de que algún proceso falle durante una ejecución prolongada aumenta con el tiempo y el volumen de datos. Los mecanismos que permitirían mejorar esta situación incluyen:

- **Reintentos (retries):** Al detectar que un worker no responde dentro de un tiempo límite, reenviar la tarea partición en particular. Se podría wrapper la función `procesar_particion` para capturar excepciones y reintentarlas automáticamente.
- **Checkpoints (puntos de control):** Durante el procesamiento, guardar los resultados parciales en disco (o en memoria intermedia) periódicamente. Si falla un worker, la ejecución podría reanudarse desde el último checkpoint en lugar de comenzar desde cero. Esto es especialmente importante para datasets grandes donde el tiempo de re-cómputo sería significativo.
- **Recuperación de tareas:** Al reejecutar el proceso, distribuir nuevamente las particiones entre los workers restantes y cualquier worker reingresante, asegurando que cada partición tenga un worker disponible. Se podría implementar un mecanismo de "marcación" de particiones completadas para evitarlas en reintentos futuros.

Estos mecanismos son inherentes a frameworks de procesamiento distribuido real como Apache Spark, que incluye recuperación de fallos nativa: si una task falla, Spark la reintenta automáticamente en otro executor, y los resultados parciales se acumulan en un checkpoint maestro.

**Evidencia:** El análisis de la Pregunta 4 en `ANALISIS_FASE5.md` describe estos mecanismos teóricamente, y el estado actual del código confirma que no hay implementación de retry, checkpoint o recovery. El resultado devalidación `resultados_identicos` para 500,000 registros aparece como "No validado" precisamente por esta limitación.
## Conclusión 5: Los límites de escalabilidad identificados orientan futuras mejoras

Los resultados del proyecto identifican límites claros de escalabilidad que, si se superan, permitirían procesar millones de registros de manera efectiva:

1. **Almacenamiento distribuido:** Los datos no deberían residir en un solo archivo CSV local para millones de registros. Utilizar HDFS o almacenamiento en columnas (Parquet) permitiría que cada worker lea solo la partición que le corresponde, reduciendo el I/O y eliminando el cuello de botella de un único punto de lectura.

2. **Balanceo de carga dinámico:** La partición actual usa reparto cíclico (`df.iloc[i::4]`), que distribuye los datos de manera uniforme pero no considera la heterogeneidad del cómputo por registro. Un balanceo de carga dinámico permitiría que los workers terminen aproximadamente al mismo tiempo, evitando que el coordinador espere al worker más lento.

3. **Herramientas especializadas:** Pasar de `multiprocessing` a Apache Spark, Hadoop MapReduce o Dask proporcionaría tolerancia a fallos nativa, particionamiento automático de datos y ejecución en múltiples nodos de un clúster. La API en Python de Spark (PySpark) sería el camino natural para escalar el código actual con mínimos cambios estructurales.

4. **Procesamiento por lotes (batching):** Dividir el dataset en lotes más pequeños que los workers puedan procesar de forma incremental permitiría un mejor aprovechamiento de los 16 núcleos lógicos y reduciría el tiempo de espera del coordinador.

Estas mejoras no serían necesarias para los volúmenes probados (10k, 100k, 500k) en el entorno actual, pero se vuelven esenciales al escalar órdenes de magnitud más, tal como indican las conclusiones sobre la relación entre tamaño del dataset y viabilidad del paralelismo.

**Evidencia:** Las discusiones en `ANALISIS_FASE5.md` (Pregunta 5) y las limitaciones identificadas en las respuestas anteriores fundamentan estas orientaciones de mejora, siempre basadas en los datos reales obtenidos y reconociendo las restricciones del entorno actual.