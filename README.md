# Proyecto Procesamiento Distribuido de Datos - Fase 1

## Estructura del proyecto

```
GUIA-8/
│
├── data/            # Conjuntos de datos generados
│   ├── ventas_pequeno.csv   (5,000 registros)
│   └── ventas_grande.csv    (1,000,000 de registros)
│
├── src/
│   └── generar_datos.py  # Script de generación de datos
│
├── requirements.txt
└── README.md
```

## Cómo ejecutar la Fase 1

1. Asegúrate de tener **Python 3.8+** instalado.
2. Instala las dependencias:

   ```bash
   pip install -r requirements.txt
   ```

3. Ejecuta el script de generación:

   ```bash
   python src/generar_datos.py
   ```

4. Los archivos CSV se crearán en la carpeta `data/`.
5. Revisa la salida en consola para validar los registros, tamaño en MB, ausencia de nulos/duplicados y muestra de datos.

## Resultados esperados

- **`data/ventas_pequeno.csv`**: 5 000 registros para pruebas rápidas.
- **`data/ventas_grande.csv`**: 1 000 000 de registros para evaluar escalabilidad.
- Validación automática imprime en consola:
  - Cantidad exacta de registros y peso en MB por archivo.
  - Confirmación de que no hay valores nulos ni `id_venta` duplicados.
  - Las primeras 5 filas de `ventas_pequeno.csv`.

---

## Fase 2: Diseño de la Arquitectura Distribuida

En esta fase se diseña la arquitectura para el procesamiento distribuido de los 50,000 registros del `dataset_distribuido.csv`. El diagrama a continuación muestra la distribución de tareas y la integración de resultados.

### Diagrama Mermaid

```mermaid
graph TB
    %% Nodos y conexiones de la arquitectura distribuida
    D[dataset_distribuido.csv\\n(50,000 registros)]
    class D source
    
    C[Coordinador Principal]
    class C coordinator
    D --> C
    
    %% Mecanismo de particionamiento
    C_part[PARTITIONING\\nN bloques equilibrados]
    C --> C_part
    
    C1[Bloque 1\\nWorker 1]
    C2[Bloque 2\\nWorker 2]
    C3[Bloque 3\\nWorker 3]
    C4[Bloque 4\\nWorker 4]
    class C1 C2 C3 C4 processor
    C_part --> C1
    C_part --> C2
    C_part --> C3
    C_part --> C4
    
    %% Nodos de procesamiento paralelo
    W1[Worker 1\\nProcesamiento Independiente]
    W2[Worker 2\\nProcesamiento Independiente]
    W3[Worker 3\\nProcesamiento Independiente]
    W4[Worker 4\\nProcesamiento Independiente]
    class W1 W2 W3 W4 processor
    W1 -.->|Partición A| W2
    W2 -.->|Partición B| W3
    W3 -.->|Partición C| W4
    
    %% Etapa de consolidación (Reduce)
    R[Consolador Central\\nREDUCE]
    class R reducer
    W1 --> R
    W2 --> R
    W3 --> R
    W4 --> R
    
    %% Generación de reportes y visualizaciones
    G1[CSV Consolidado]
    G2[JSON Consolidado]
    G3[Gráficos Categoría]
    G4[Gráficos Región]
    G5[Comparativa Tiempos]
    R --> G1
    R --> G2
    G1 --> G3
    G2 --> G4
    G3 --> G5
    G4 --> G5
    
    %% Registro de tiempos y speedup
    M1[Tiempo Lectura (ms)]
    M2[Speedup Observado]
    G5 --> M1
    G5 --> M2
```

### Componentes del Diagrama

1. **Fuente de Datos** (`dataset_distribuido.csv`): Contiene 50,000 transacciones con columnas `id_transacción`, `fecha`, `categoría`, `región`, `estado`, `cantidad`, `precio_unitario`, `total_venta`, `tiempo_procesamiento_ms`.

2. **Mecanismo de Particionamiento** (`Coordinador Principal`): Divide los registros en N bloques equilibrados (por chunks de filas o por región) y despacha cada partición a los workers.

3. **Nodos de Procesamiento** (`Worker 1 a Worker 4`): Cuatro procesos trabajadores paralelos usando `multiprocessing` en Python que procesan y agregan localmente cada partición de forma independiente.

4. **Etapa de Consolidación** (`Consolador Central`): Fase tipo *Reduce* que recolecta los resultados parciales de todos los workers y los unifica en un resultado global.

5. **Generación de Reportes**: Exportación de datos consolidados en CSV/JSON y generación de gráficos de resumen (métricas por categoría/región y comparativa de tiempos).

6. **Registro de Tiempos y Speedup**: Medición de métricas (`registro_tiempos.csv`) que incluye tiempo de lectura, particionamiento, ejecución paralela por nodo y consolidación, comparado contra ejecución secuencial para calcular la ganancia de velocidad (*speedup*).

### Script de Generación de Diagrama

El script `src/fase2_generar_diagrama.py` genera:
- `diagrama_mermaid.mmd`: Código fuente en formato Mermaid
- `arquitectura_distribuida.png`: Imagen en alta resolución del diagrama

Ejecutar: `python src/fase2_generar_diagrama.py`

### Próximos pasos

- Ejecutar el script de generación de diagrama
- Verificar la imagen PNG resultante
- Insertar el diagrama en el documento de entrega
- Ejecutar el procesamiento distribuido real sobre los 50,000 registros