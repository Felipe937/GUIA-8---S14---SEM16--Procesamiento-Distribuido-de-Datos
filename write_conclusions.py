import os

base = r'D:\Sistemas Distribuidos\GUIA 8 - S14 - SEM16- Procesamiento Distribuido de Datos'
filepath = os.path.join(base, 'CONCLUSIONES_FASE6.md')

lines = []
lines.append('# Conclusiones del Proyecto: Procesamiento Distribuido de Datos')
lines.append('')
lines.append('## Conclusión 1: El tamaño del dataset determina la viabilidad del procesamiento paralelo')
lines.append('')
lines.append('El análisis demuestra que el procesamiento paralelo con `multiprocessing` no es beneficioso por sí mismo; su efectividad depende críticamente del volumen de datos a procesar. Para 10,000 registros, el tiempo de procesamiento paralelo fue de 1.4881 s frente a 0.5684 s del secuencial, resultando en un speedup de 0.38x. Este resultado indica que la sobrecarga de coordinación —particionamiento (0.064 s), transferencia/pickle (0.0074 s) y coordinación de workers (1.4165 s)— superó ampliamente el beneficio del cómputo paralelo en volúmenes pequeños. Los datos confirman que el 95%+ del tiempo paralelo fue overhead, no cómputo.')
lines.append('')
lines.append('Para 100,000 registros, el escenario cambió sustancialmente: el secuencial requirió 6.2098 s y el paralelo 2.1414 s, logrando un speedup de 2.90x. Aquí el tamaño del dataset fue el factor decisivo: el tiempo de cómputo en los workers comenzó a dominar sobre la sobrecarga de coordinación (1.482 s de cómputo vs. 0.5678 s de particionamiento y 0.0915 s de transferencia). Para 500,000 registros, el tiempo paralelo medido fue de 4.5890 s, con una estimación secuencial de 31.0490 s (extrapolado), lo que sugeriría un speedup de 6.77x, aunque esta cifra debe interpretarse con cautela al no haber validado directamente el resultado secuencial en el entorno de prueba.')
lines.append('')
lines.append('**Evidencia:** `resultados_fase4/tabla_rendimiento.csv` muestra los tiempos reales para los tres volúmenes, y `entorno_pruebas.json` confirma el hardware (Windows 10, 16 núcleos lógicos, Python 3.11.9).')

with open(filepath, 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))

print('Parte 1 escrita')