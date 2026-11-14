"""
Script Fase 3: Procesamiento distribuido Map-Reduce
====================================================

Este script simula un procesamiento distribuido de datos utilizando
multiprocessing con 4 procesos trabajadores (workers) que actúan como
nodos independientes en un solo equipo.

Divide el dataset en 4 particiones equilibradas, cada worker calcula
de forma aislada:
  - Total de ingresos por ciudad (región)
  - Cantidad total vendida por producto (categoría)
  - Ingresos totales por mes

El proceso principal consolida los resultados parciales, ejecuta una
secuencial de control y valida que la suma de las particiones coincida
exactamente con el total global.
"""

import os
import json
import time
from collections import defaultdict, Counter
import pandas as pd
from multiprocessing import Pool
from datetime import datetime

# ---------------------------------------------------------------------------
# RUTAS Y CONSTANTES
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, 'data', 'dataset_distribuido.csv')
RESULTADOS_DIR = os.path.join(BASE_DIR, 'resultados_fase3')
NUM_WORKERS = 4

os.makedirs(RESULTADOS_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# FUNCIONES AUXILIARES
# ---------------------------------------------------------------------------

def extraer_mes(fecha_str):
    """Extrae el año-mes (YYYY-MM) de una fecha en formato YYYY-MM-DD."""
    try:
        dt = datetime.strptime(str(fecha_str), "%Y-%m-%d")
        return dt.strftime("%Y-%m")
    except (ValueError, TypeError):
        return "Desconocido"


def procesar_particion(filas):
    """
    Procesa una partición de datos y calcula:
      - Ingresos por región
      - Cantidad vendida por categoría (producto)
      - Ingresos por mes

    Parameters
    ----------
    filas : list[dict]
        Lista de registros (diccionarios) de la partición.

    Returns
    -------
    dict
        Resultados parciales de la partición.
    """
    ingresos_por_region = defaultdict(float)
    contador_productos = Counter()
    ingresos_por_mes = defaultdict(float)

    for fila in filas:
        try:
            region = fila['region']
            categoria = fila['categoria']
            cantidad = int(fila['cantidad'])
            total_venta = int(fila['total_venta'])
            fecha = fila['fecha']

            ingresos_por_region[region] += total_venta
            contador_productos[categoria] += cantidad
            mes = extraer_mes(fecha)
            ingresos_por_mes[mes] += total_venta
        except Exception:
            continue

    return {
        'ingresos_por_region': dict(ingresos_por_region),
        'top_productos': contador_productos.most_common(),
        'ingresos_por_mes': dict(ingresos_por_mes),
        'total_registros': len(filas),
    }


def consolidar_resultados(resultados_parciales):
    """
    Consolida los resultados parciales de los 4 workers en totales globales.

    Parameters
    ----------
    resultados_parciales : list[dict]
        Lista de diccionarios con los resultados de cada partición.

    Returns
    -------
    dict
        Consolidación global con ingresos por región, top productos y
        ingresos por mes.
    """
    ingresos_por_region_global = defaultdict(float)
    contador_productos_global = Counter()
    ingresos_por_mes_global = defaultdict(float)

    for resultado in resultados_parciales:
        for region, monto in resultado['ingresos_por_region'].items():
            ingresos_por_region_global[region] += monto
        for producto, cantidad in resultado['top_productos']:
            contador_productos_global[producto] += cantidad
        for mes, monto in resultado['ingresos_por_mes'].items():
            ingresos_por_mes_global[mes] += monto

    top_productos_definitivos = contador_productos_global.most_common(5)

    return {
        'ingresos_por_region': dict(ingresos_por_region_global),
        'top_productos_globales': top_productos_definitivos,
        'ingresos_por_mes': dict(ingresos_por_mes_global),
    }


def ejecucion_secuencial(df):
    """
    Ejecuta el mismo procesamiento de forma secuencial (un solo hilo)
    para usarlo como ejecución de control y validación.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame completo del dataset.

    Returns
    -------
    dict
        Resultados globales calculados de forma secuencial.
    """
    ingresos_por_region = defaultdict(float)
    contador_productos = Counter()
    ingresos_por_mes = defaultdict(float)

    for _, fila in df.iterrows():
        region = fila['region']
        categoria = fila['categoria']
        cantidad = int(fila['cantidad'])
        total_venta = int(fila['total_venta'])
        fecha = fila['fecha']

        ingresos_por_region[region] += total_venta
        contador_productos[categoria] += cantidad
        mes = extraer_mes(fecha)
        ingresos_por_mes[mes] += total_venta

    return {
        'ingresos_por_region': dict(ingresos_por_region),
        'top_productos': contador_productos.most_common(5),
        'ingresos_por_mes': dict(ingresos_por_mes),
    }


# ---------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("FASE 3 - PROCESAMIENTO DISTRIBUIDO (simulado con multiprocessing)")
    print("=" * 70)

    # 1. Cargar dataset
    print(f"\n[1/6] Cargando dataset desde: {DATA_PATH}")
    df = pd.read_csv(DATA_PATH)
    total_registros = len(df)
    print(f"      Registros cargados: {total_registros:,}")
    print(f"      Columnas: {list(df.columns)}")

    # 2. Dividir en 4 particiones equilibradas
    print(f"\n[2/6] Dividiendo dataset en {NUM_WORKERS} particiones equilibradas...")
    particiones = [df.iloc[i::NUM_WORKERS].reset_index(drop=True)
                   for i in range(NUM_WORKERS)]
    datos_particiones = [p.to_dict('records') for p in particiones]
    for i, p in enumerate(datos_particiones):
        print(f"      Partición {i+1}: {len(p):,} registros")

    # 3. Procesar en paralelo con 4 workers
    print(f"\n[3/6] Ejecutando procesamiento paralelo con {NUM_WORKERS} workers...")
    tiempo_inicio_paralelo = time.perf_counter()

    with Pool(NUM_WORKERS) as pool:
        resultados_parciales = pool.map(procesar_particion, datos_particiones)

    tiempo_fin_paralelo = time.perf_counter()
    tiempo_paralelo = tiempo_fin_paralelo - tiempo_inicio_paralelo
    print(f"      Tiempo paralelo: {tiempo_paralelo:.4f} segundos")

    # Mostrar resultados individuales de cada partición
    print("\n      --- Resultados individuales por partición ---")
    for i, resultado in enumerate(resultados_parciales):
        print(f"      Partición {i+1} ({resultado['total_registros']:,} registros):")
        print(f"        - Ingresos por región: {len(resultado['ingresos_por_region'])} regiones")
        print(f"        - Productos únicos: {len(resultado['top_productos'])}")
        print(f"        - Meses: {len(resultado['ingresos_por_mes'])}")

    # 4. Consolidar resultados en el proceso principal
    print("\n[4/6] Consolidando resultados globales (proceso principal)...")
    consolidado = consolidar_resultados(resultados_parciales)

    # 5. Ejecución secuencial de control
    print("\n[5/6] Ejecutando procesamiento secuencial de control...")
    tiempo_inicio_secuencial = time.perf_counter()
    resultado_secuencial = ejecucion_secuencial(df)
    tiempo_fin_secuencial = time.perf_counter()
    tiempo_secuencial = tiempo_fin_secuencial - tiempo_inicio_secuencial
    print(f"      Tiempo secuencial: {tiempo_secuencial:.4f} segundos")

    # 6. Validación: suma de particiones vs total global vs secuencial
    print("\n[6/6] Validando consistencia de resultados...")

    # Validar ingresos por región
    suma_particiones_region = defaultdict(float)
    for r in resultados_parciales:
        for region, monto in r['ingresos_por_region'].items():
            suma_particiones_region[region] += monto

    region_coincide = True
    for region, monto_global in consolidado['ingresos_por_region'].items():
        if abs(suma_particiones_region[region] - monto_global) > 0.01:
            region_coincide = False
            break

    # Validar productos (comparar top 5 de consolidado vs secuencial)
    productos_global_consolidado = consolidado['top_productos_globales']
    productos_global_secuencial = resultado_secuencial['top_productos']
    productos_coinciden = (productos_global_consolidado == productos_global_secuencial)

    # Validar ingresos por mes
    suma_particiones_mes = defaultdict(float)
    for r in resultados_parciales:
        for mes, monto in r['ingresos_por_mes'].items():
            suma_particiones_mes[mes] += monto

    mes_coincide = True
    for mes, monto_global in consolidado['ingresos_por_mes'].items():
        if abs(suma_particiones_mes[mes] - monto_global) > 0.01:
            mes_coincide = False
            break

    # Validar vs secuencial (ingresos por región)
    vs_secuencial_region = True
    for region, monto in consolidado['ingresos_por_region'].items():
        if abs(monto - resultado_secuencial['ingresos_por_region'].get(region, 0)) > 0.01:
            vs_secuencial_region = False
            break

    # Validar vs secuencial (ingresos por mes)
    vs_secuencial_mes = True
    for mes, monto in consolidado['ingresos_por_mes'].items():
        if abs(monto - resultado_secuencial['ingresos_por_mes'].get(mes, 0)) > 0.01:
            vs_secuencial_mes = False
            break

    print(f"      Suma de particiones == Total global (región):  [{'SÍ' if region_coincide else 'NO'}]")
    print(f"      Top 5 productos == Secuencial:                 [{'SÍ' if productos_coinciden else 'NO'}]")
    print(f"      Suma de particiones == Total global (mes):     [{'SÍ' if mes_coincide else 'NO'}]")
    print(f"      Global == Secuencial (región):                 [{'SÍ' if vs_secuencial_region else 'NO'}]")
    print(f"      Global == Secuencial (mes):                    [{'SÍ' if vs_secuencial_mes else 'NO'}]")

    # -------------------------------------------------------------------
    # GUARDAR RESULTADOS EN CSV
    # -------------------------------------------------------------------

    # 6a. Resultados individuales de cada partición
    filas_particion = []
    for i, r in enumerate(resultados_parciales):
        filas_particion.append({
            'particion': f'Partición {i+1}',
            'total_registros': r['total_registros'],
            'num_regiones': len(r['ingresos_por_region']),
            'num_productos': len(r['top_productos']),
            'num_meses': len(r['ingresos_por_mes']),
        })

    # Detalle de ingresos por región por partición
    detalle_region_particion = []
    for i, r in enumerate(resultados_parciales):
        for region, monto in sorted(r['ingresos_por_region'].items()):
            detalle_region_particion.append({
                'particion': f'Partición {i+1}',
                'region': region,
                'ingresos': round(monto, 2),
            })

    # Detalle de productos por partición
    detalle_producto_particion = []
    for i, r in enumerate(resultados_parciales):
        for producto, cantidad in r['top_productos']:
            detalle_producto_particion.append({
                'particion': f'Partición {i+1}',
                'producto': producto,
                'cantidad': cantidad,
            })

    # Detalle de ingresos por mes por partición
    detalle_mes_particion = []
    for i, r in enumerate(resultados_parciales):
        for mes, monto in sorted(r['ingresos_por_mes'].items()):
            detalle_mes_particion.append({
                'particion': f'Partición {i+1}',
                'mes': mes,
                'ingresos': round(monto, 2),
            })

    pd.DataFrame(filas_particion).to_csv(
        os.path.join(RESULTADOS_DIR, 'resultados_por_particion.csv'), index=False)
    pd.DataFrame(detalle_region_particion).to_csv(
        os.path.join(RESULTADOS_DIR, 'ingresos_por_region_por_particion.csv'), index=False)
    pd.DataFrame(detalle_producto_particion).to_csv(
        os.path.join(RESULTADOS_DIR, 'productos_por_particion.csv'), index=False)
    pd.DataFrame(detalle_mes_particion).to_csv(
        os.path.join(RESULTADOS_DIR, 'ingresos_por_mes_por_particion.csv'), index=False)

    # 6b. Consolidación final
    # Ingresos por ciudad (región) - consolidado global
    df_ingresos_ciudad = pd.DataFrame([
        {'region': region, 'ingresos_totales': round(monto, 2)}
        for region, monto in sorted(consolidado['ingresos_por_region'].items(),
                                    key=lambda x: x[1], reverse=True)
    ])
    df_ingresos_ciudad.to_csv(
        os.path.join(RESULTADOS_DIR, 'ingresos_por_ciudad.csv'), index=False)

    # Top 5 productos más vendidos por cantidad - consolidado global
    df_top_productos = pd.DataFrame([
        {'producto': producto, 'cantidad_total': cantidad}
        for producto, cantidad in consolidado['top_productos_globales']
    ])
    df_top_productos.to_csv(
        os.path.join(RESULTADOS_DIR, 'top_productos.csv'), index=False)

    # Ingresos por mes - consolidado global
    df_ingresos_mes = pd.DataFrame([
        {'mes': mes, 'ingresos_totales': round(monto, 2)}
        for mes, monto in sorted(consolidado['ingresos_por_mes'].items())
    ])
    df_ingresos_mes.to_csv(
        os.path.join(RESULTADOS_DIR, 'ingresos_por_mes.csv'), index=False)

    # 6c. Validación comparativa
    filas_validacion = []
    for region in sorted(set(list(suma_particiones_region.keys()) +
                              list(consolidado['ingresos_por_region'].keys()))):
        filas_validacion.append({
            'tipo': 'region',
            'nombre': region,
            'suma_particiones': round(suma_particiones_region[region], 2),
            'total_global': round(consolidado['ingresos_por_region'].get(region, 0), 2),
            'total_secuencial': round(resultado_secuencial['ingresos_por_region'].get(region, 0), 2),
            'diferencia_global_secuencial': round(
                consolidado['ingresos_por_region'].get(region, 0) -
                resultado_secuencial['ingresos_por_region'].get(region, 0), 2),
        })
    for mes in sorted(set(list(suma_particiones_mes.keys()) +
                         list(consolidado['ingresos_por_mes'].keys()))):
        filas_validacion.append({
            'tipo': 'mes',
            'nombre': mes,
            'suma_particiones': round(suma_particiones_mes[mes], 2),
            'total_global': round(consolidado['ingresos_por_mes'].get(mes, 0), 2),
            'total_secuencial': round(resultado_secuencial['ingresos_por_mes'].get(mes, 0), 2),
            'diferencia_global_secuencial': round(
                consolidado['ingresos_por_mes'].get(mes, 0) -
                resultado_secuencial['ingresos_por_mes'].get(mes, 0), 2),
        })
    pd.DataFrame(filas_validacion).to_csv(
        os.path.join(RESULTADOS_DIR, 'validacion_particion_vs_global.csv'), index=False)

    # 6d. Resumen de ejecución en texto
    resumen = []
    resumen.append("=" * 70)
    resumen.append("RESUMEN DE EJECUCIÓN - FASE 3 (Procesamiento Distribuido Simulado)")
    resumen.append("=" * 70)
    resumen.append("")
    resumen.append(f"Dataset: {DATA_PATH}")
    resumen.append(f"Total de registros: {total_registros:,}")
    resumen.append(f"Número de workers: {NUM_WORKERS}")
    resumen.append("")
    resumen.append("--- TIEMPOS DE EJECUCIÓN ---")
    resumen.append(f"  Paralelo (4 workers):  {tiempo_paralelo:.4f} segundos")
    resumen.append(f"  Secuencial (control):  {tiempo_secuencial:.4f} segundos")
    if tiempo_paralelo > 0:
        speedup = tiempo_secuencial / tiempo_paralelo
        resumen.append(f"  Speedup observado:     {speedup:.2f}x")
    resumen.append("")
    resumen.append("--- INGRESOS POR CIUDAD (GLOBAL) ---")
    for region, monto in sorted(consolidado['ingresos_por_region'].items(),
                                key=lambda x: x[1], reverse=True):
        resumen.append(f"  {region}: {monto:,.2f} COP")
    resumen.append("")
    resumen.append("--- TOP 5 PRODUCTOS MÁS VENDIDOS (GLOBAL) ---")
    for posicion, (producto, cantidad) in enumerate(consolidado['top_productos_globales'], 1):
        resumen.append(f"  {posicion}. {producto}: {cantidad:,} unidades")
    resumen.append("")
    resumen.append("--- INGRESOS POR MES (GLOBAL) ---")
    for mes, monto in sorted(consolidado['ingresos_por_mes'].items()):
        resumen.append(f"  {mes}: {monto:,.2f} COP")
    resumen.append("")
    resumen.append("--- VALIDACIÓN ---")
    resumen.append(f"  Suma de particiones == Total global (región):  {'SÍ' if region_coincide else 'NO'}")
    resumen.append(f"  Top 5 productos == Secuencial:                 {'SÍ' if productos_coinciden else 'NO'}")
    resumen.append(f"  Suma de particiones == Total global (mes):     {'SÍ' if mes_coincide else 'NO'}")
    resumen.append(f"  Global == Secuencial (región):                 {'SÍ' if vs_secuencial_region else 'NO'}")
    resumen.append(f"  Global == Secuencial (mes):                    {'SÍ' if vs_secuencial_mes else 'NO'}")
    resumen.append("")
    resumen.append("NOTA: Este es un procesamiento simulado en un solo equipo con")
    resumen.append("procesos independientes (multiprocessing), NO un clúster distribuido real.")
    resumen.append("=" * 70)

    resumen_texto = "\n".join(resumen)
    with open(os.path.join(RESULTADOS_DIR, 'resumen_ejecucion.txt'), 'w', encoding='utf-8') as f:
        f.write(resumen_texto)

    # -------------------------------------------------------------------
    # IMPRIMIR RESUMEN EN CONSOLA
    # -------------------------------------------------------------------
    print("\n" + resumen_texto)

    print(f"\n>>> Todos los resultados guardados en: {RESULTADOS_DIR}/")
    print("   - resultados_por_particion.csv")
    print("   - ingresos_por_region_por_particion.csv")
    print("   - productos_por_particion.csv")
    print("   - ingresos_por_mes_por_particion.csv")
    print("   - ingresos_por_ciudad.csv (global)")
    print("   - top_productos.csv (global)")
    print("   - ingresos_por_mes.csv (global)")
    print("   - validacion_particion_vs_global.csv")
    print("   - resumen_ejecucion.txt")


# ---------------------------------------------------------------------------
# PUNTO DE ENTRADA
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()