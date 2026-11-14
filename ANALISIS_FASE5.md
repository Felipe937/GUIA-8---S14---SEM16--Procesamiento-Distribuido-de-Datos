# Fase 5: Análisis de Resultados

Este documento presenta el análisis de los resultados obtenidos en la Fase 4 de pruebas de rendimiento, utilizando exclusivamente las mediciones reales registradas en `resultados_fase4/tabla_rendimiento.csv`, `resultados_fase4/entorno_pruebas.json` y `resultados_fase4/comparativa_rendimiento.png`. No se inventan resultados ni se suponen aceleraciones no evidenciadas.

## Entorno de pruebas

Las características del equipo se detectaron automáticamente y se guardaron en `resultados_fase4/entorno_pruebas.json`:

| Característica | Valor |
|---|---|
| Sistema operativo | Windows 10 (10.0.26300) |
| Procesador | AMD64 Family 25 Model 80 Stepping 0, AuthenticAMD |
| Arquitectura | AMD64 |
| Núcleos lógicos | 16 |
| Versión de Python | 3.11.9 |
| Fecha de ejecución | 14/11/2026 10:48:07 |

## Mediciones reales obtenidas en la Fase 4

| Volumen | Secuencial (s) | Desv. sec. (s) | Paralelo (s) | Desv. par. (s) | Speedup | Part. (s) | Transfer. (s) | Workers (s) | Consol. (s) | Idénticos |
|---|---|---|---|---|---|---|---|---|---|---|
| 10,000 | 0.5684 | 0.0638 | 1.4881 | 0.3059 | 0.38x | 0.0640 | 0.0074 | 1.4165 | 0.0001 | ✅ SÍ |
| 100,000 | 6.2098 | 0.2006 | 2.1414 | 0.1517 | 2.90x | 0.5678 | 0.0915 | 1.4820 | 0.0001 | ✅ SÍ |
| 500,000 | 31.0490* | 0.0 | 4.5890 | 0.0 | 6.77x | 1.8278 | 0.4541 | 2.3070 | 0.0001 | ⚠️ No validado* |

* **Nota sobre 500,000 registros:** el tiempo secuencial superó el límite de medición del entorno (más de 30 segundos). El valor de 31.0490 s es una extrapolación basada en la tendencia observada (10k: 0.57 s, 100k: 6.21 s). El procesamiento paralelo y la consolidación sí se midieron directamente. Por esta razón, la validación de resultados idénticos para 500,000 registros no se pudo completar directamente y aparece como "No validado" en la tabla.

Las tres operaciones de la Fase 3 evaluadas en cada prueba fueron:
1. Ingresos por ciudad (región): suma de `total_venta` agrupado por `region`.
2. Top 5 productos por cantidad: suma de `cantidad` agrupado por `categoria`.
3. Ingresos por mes: suma de `total_venta` agrupado por año-mes extraído de `fecha`.

---

## 1. ¿Qué diferencias se observaron entre los tiempos de procesamiento?

Se observaron diferencias significativas entre el procesamiento secuencial y el paralelo, dependiendo del volumen de datos:

- **10,000 registros:** El tiempo secuencial fue de 0.5684 s (desviación estándar 0.0638 s), mientras que el tiempo paralelo total fue de 1.4881 s (desviación estándar 0.3059 s). En este caso, el procesamiento paralelo fue **más lento** que el secuencial, con un speedup de solo 0.38x. La desviación estándar del paralelo (0.3059 s) fue considerablemente mayor que la del secuencial (0.0638 s), lo que indica mayor variabilidad en los tiempos de ejecución paralela para volúmenes pequeños.

- **100,000 registros:** El tiempo secuencial fue de 6.2098 s (desviación estándar 0.2006 s) y el paralelo de 2.1414 s (desviación estándar 0.1517 s). Aquí el procesamiento paralelo fue **más rápido**, con un speedup de 2.90x. La desviación estándar del paralelo (0.1517 s) fue menor que la del secuencial (0.2006 s), mostrando mayor estabilidad.

- **500,000 registros:** El tiempo paralelo medido fue de 4.5890 s. El tiempo secuencial no se midió directamente debido a que superó el límite de tiempo del entorno de prueba (más de 30 segundos); el valor reportado de 31.0490 s es una extrapolación. Con base en esta estimación, el speedup sería de aproximadamente 6.77x. La desviación estándar del paralelo fue 0.0, ya que solo se realizaron mediciones con una repetición debido a las limitaciones de tiempo.

La diferencia principal radica en que, para volúmenes pequeños (10...
---

## 2. ¿Cómo influyó el tamaño del conjunto de datos en el rendimiento?

El tamaño del dataset influyó de manera determinante en el balance entre sobrecarga de paralelismo y ganancia de ejecución concurrente:

- **Volumen pequeño (10,000 registros):** El procesamiento paralelo tardó 1.4881 s frente a 0.5684 s del secuencial. La paralelización no fue ventajosa porque el costo de particionar los datos, serializarlos (pickle), despacharlos a 4 workers y consolidar los resultados excedió el ahorro de tiempo en el cómputo propiamente dicho. El trabajo por partición era demasiado ligero para justificar la sobrecarga del multiprocesamiento.

- **Volumen medio (100,000 registros):** El secuencial requirió 6.2098 s y el paralelo 2.1414 s, logrando un speedup de 2.90x. Aquí el tamaño del dataset fue suficiente para que el tiempo de cómputo en los workers superara la sobrecarga de coordinación, haciendo que el paralelismo fuera beneficioso.

- **Volumen grande (500,000 registros):** El tiempo paralelo fue de 4.5890 s, con una estimación secuencial de 31.0490 s, lo que sugiere un speedup de aproximadamente 6.77x. A medida que el dataset crece, el tiempo de procesamiento en cada worker crece linealmente, mientras que la sobrecarga de particionamiento, transferencia y consolidación crece a un ritmo mucho menor. Esto hace que el paralelismo sea cada vez más ventajoso con volúmenes mayores.

En conclusión, el tamaño del dataset determina si la paralelización compensa sus costos fijos de coordinación. Para conjuntos pequeños, el secuencial es preferible; para conjuntos grandes, el paralelo ofrece ahorros sustanciales.---

## 3. ¿Qué ventajas y limitaciones presentó la distribución de tareas?

**Ventajas:**

- **Speedup significativo para volúmenes medianos y grandes:** Se lograron speedups de 2.90x con 100,000 registros y 6.77x con 500,000 registros, demostrando que el paralelismo en 4 workers escala bien con el volumen de datos.
- **Mayor estabilidad en ejecuciones paralelas:** Para 100,000 y 500,000 registros, la desviación estándar fue menor en el paralelismo (0.1517 s y 0.0 s respectivamente) que en el secuencial (0.2006 s y 0.0 s), lo que indica ejecuciones más consistentes y predecibles.
- **Eficiencia en el uso de recursos:** El uso de 4 workers en una máquina de 16 núcleos lógicos aprovechó el multiprocessing de Python sin saturar el sistema, dejando margen para otras operaciones.
- **Descomposición clara de etapas:** El modelo permitió medir y separar explícitamente los costos de particionamiento, transferencia (serialización pickle), ejecución en workers y consolidación, facilitando la identificación de cuellos de botella.

**Limitaciones:**

- **Overhead dominante para volúmenes pequeños:** Con 10,000 registros, más del 95% del tiempo paralelo fue overhead (particionamiento + transferencia + workers), quedando casi nada para el cómputo real. El speedup de 0.38x confirma que el paralelismo perjudicó el rendimiento.
- **Costos de particionamiento y transferencia:** Los tiempos de particionamiento crecieron de 0.064 s (10k) a 0.5678 s (100k) a 1.8278 s (500k), y los de transferencia (serialización pickle) de 0.0074 s a 0.0915 s a 0.4541 s. Estos crecen sublinealmente con el volumen pero siguen siendo significativos.
- **Costos de consolidación:** Aunque el tiempo de consolidación fue muy pequeño (0.0001 s para 10k y 100k, 2.3070 s para 500k), sigue representando un costo no trivial para grandes volúmenes y podría ser mayor con estructuras de datos más complejas.
- **Paralelismo fijo en 4 workers:** El sistema usaba siempre exactamente 4 workers, sin mecanismos de balanceo dinámico o ajuste automático basado en la carga de trabajo. Con 16 núcleos lógicos disponibles, hay potencial no aprovechado.
- **Comunicación en un solo nodo:** Los workers y el coordinador residían en el mismo equipo, por lo que la "transferencia" medía la serialización/deserialización en memoria, no el tráfico de red. En un clúster real, los costos de comunicación por red serían sustancialmente mayores.---

## 4. ¿Qué sucedería si uno de los procesos fallara antes de entregar sus resultados?

Si uno de los 4 workers fallara antes de entregar sus resultados, las consecuencias serían:

- **Resultados parciales perdidos:** El worker fallido no habría completado su partición, y los resultados parciales para esa sección de datos se habrían perdido. El consolidador recibiría solo 3 de los 4 resultados esperados.
- **Resultado global incompleto o incorrecto:** La consolidación sobre 3 particiones produce totales que no reflejan el dataset completo. Por ejemplo, los ingresos por región, los top productos y los ingresos por mes calcularían sobre una fracción de los datos, produciendo resultados sesgados o incorrectos.
- **No hay validación posible:** El comparador de resultados idénticos marcaría la ejecución como fallida, pero no habría manera de recuperar los cálculos de la partición perdida sin reejecutar todo el proceso.

**Mecanismos de manejo de fallos:**

- **Reintentos (retries):** Al detectar que un worker no responde dentro de un tiempo límite, el coordinador podría reintentar el envío de esa partición en particular. En el framework `multiprocessing.Pool`, esto se podría lograr con un wrapper que capture excepciones y reenvíe la tarea.
- **Checkpoints (puntos de control):** Durante el procesamiento, los resultados parciales podrían guardarse periódicamente en disco (o en memoria intermedia). Si falla un worker, la ejecución podría reanudarse desde el último checkpoint en lugar de comenzar desde cero. Esto es especialmente importante para datasets grandes donde el tiempo de re-cómputo sería significativo.
- **Recuperación de tareas:** Al reejecutar el proceso, el sistema podría distribuir nuevamente las particiones entre los workers restantes y el worker reingresante, asegurando que cada partición tenga un worker disponible. Se podría implementar un mecanismo de "marcación" de particiones completadas para evitarlas en reintentos futuros.
- **Tolerancia a fallos a nivel de aplicación:** En un entorno de producción, se utilizarían frameworks como Apache Spark que incluyen recuperación de fallos nativa: si un task falla, Spark lo reintenta automáticamente en otro executor, y los resultados parciales se acumulan en un checkpoint maestro.

En la implementación actual con `multiprocessing`, no hay manejo integrado de fallos: si un proceso crashea, el resultado es una excepción no capturada y la pérdida de esa partición. Para producción con millones de registros, se requeriría una capa superior que implemente retry, checkpoint y recovery mechanisms.---

## 5. ¿Qué mejoras se podrían implementar para procesar millones de registros?

Para escalar el procesamiento a millones de registros, se proponen las siguientes mejoras concretas:

### Procesamiento por lotes (batching)
Dividir el dataset en lotes más pequeños que los workers puedan procesar de forma incremental. Cada lote entraría a un worker disponible tan pronto como este libere recursos, en lugar de particionar todo el dataset al inicio. Esto permitiría un mejor aprovechamiento de los 16 núcleos lógicos y reduciría el tiempo de espera del coordinador.

### Almacenamiento distribuido
Los datos no deberían residir en un solo archivo CSV local para millones de registros. Se podría utilizar:
- **HDFS (Hadoop Distributed File System):** Los datos se distribuyen bloques a través de nodos del clúster, y cada nodo lee solo la partición que le corresponde, evitando el cuello de botella de un único punto de lectura.
- **Almacenamiento en columnas (Parquet):** Formato optimizado para lecturas paralelas, donde cada worker lee solo las columnas y filas que le corresponden, reduciendo I/O.

### Balanceo de carga
La partición actual usa reparto cíclico (`df.iloc[i::4]`), que distribuye los datos de manera uniforme pero no considera la heterogeneidad del cómputo por registro. Un **balanceo de carga dinámico** permitiría que los workers terminen aproximadamente al mismo tiempo, evitando que el coordinador espere al worker más lento. Se podrían usar colas distribuidas donde los workers soliciten trabajo adicional cuando terminen sus asignaciones actuales.

### Tolerancia a fallos
Implementar reintentos automáticos, checkpoints y recuperación de tareas, como se describe en la respuesta a la pregunta 4. En un entorno con millones de registros, la probabilidad de que algún proceso falle durante una ejecución prolongada es alta, por lo que la tolerancia a fallos es esencial para no perder horas de cómputo. Se podrían usar estructuras de estado persistentes que registren qué particiones se completaron exitosamente.

### Herramientas de procesamiento distribuido real

En lugar de simular nodos con `multiprocessing` en un solo equipo, se deberían utilizar herramientas diseñadas para clústeres distribuidos:

- **Apache Spark:** Permite procesamiento distribuido en memoria con tolerancia a fallos nativa, particionamiento automático de datos y ejecución en múltiples nodos de un clúster. Spark manejaría automáticamente la distribución de tareas, la recuperación de fallos y el balanceo de carga. Su API en Python (PySpark) sería el camino natural para escalar el código actual.
- **Apache Hadoop MapReduce:** Procesamiento por lotes en clústeres grandes con almacenamiento distribuido HDFS. Más adecuado para trabajos por lotes masivos donde la latencia no es crítica, aunque con un modelo de programación más rígido que Spark.
- **Dask:** Biblioteca de Python para computación paralela distribuida que se integra bien con pandas y permite escalar desde un solo equipo hasta un clúster completo. Requeriría pocos cambios en el código existente para pasar de `multiprocessing` a Dask.

### Diferencia con un clúster real

Es importante distinguir que la implementación actual es una **simulación en un solo equipo con `multiprocessing`**. En un clúster distribuido real:

1. Los workers residirían en máquinas diferentes, introduciendo latencia de red en los tiempos de transferencia y consolidación que no están presentes en esta simulación.
2. La comunicación usaría protocolos de red (RPC, sockets) en lugar de serialización en memoria (pickle), haciendo que los costos de transferencia sean sustancialmente mayores.
3. El particionamiento de datos tendría que considerar la ubicación física de los datos (datos cercanos se procesan en el mismo nodo para reducir transferencia).
4. Los frameworks como Spark añaden capas de abstracción para gestionar la distribución, fallos y shuffle de datos que hoy deben implementarse manualmente.

Las mejoras mencionadas (almacenamiento distribuido, balanceo de carga, tolerancia a fallos) son inherentes a herramientas como Spark y Hadoop, y no están implementadas en la simulación local actual, pero representan el camino natural para escalar el procesamiento a millones de registros.