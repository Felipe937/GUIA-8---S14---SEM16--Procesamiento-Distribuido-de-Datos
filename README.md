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