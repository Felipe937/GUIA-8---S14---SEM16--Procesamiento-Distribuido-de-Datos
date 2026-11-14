"""Script Fase 3: Procesamiento distribuido Map-Reduce"""
import os
import json
from collections import defaultdict, Counter
import pandas as pd
from multiprocessing import Pool
from datetime import datetime

DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'dataset_distribuido.csv')
RESULTADOS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resultados_fase3')
NUM_WORKERS = 4
os.makedirs(RESULTADOS_DIR, exist_ok=True)


def extraer_mes(fecha_str):
    try:
        dt = datetime.strptime(fecha_str, "%Y-%m-%d")
        return dt.strftime("%Y-%m")
    except ValueError:
        return "Desconocido"


def procesar_particion(filas):
    ingresos_por_region = defaultdict(float)
    contador_productos = Counter()
    ingresos_por_mes = defaultdict(float)
    for fila in filas:
        try:
            region = fila['region']
            categoria = fila['categoria']
            cantidad = int(fila['cantidad'])
            precio_unitario = int(fila['precio_unitario'])
            total_venta = int(fila['total_venta'])
            fecha = fila['fecha']
            ingresos_por_region[region] += total_venta
            contador_productos[categoria] += cantidad
            mes = extraer_mes(fecha)
            ingresos_por_mes[mes] += total_venta
        except: continue
    return {
        'ingresos_por_region': dict(ingresos_por_region),
        'top_productos': contador_productos.most_common(5),
        'ingresos_por_mes': dict(ingresos_por_mes),
        'total_registros': len(filas)
    }


def consolidar_resultados(resultados_parciales):
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


def ejecucion_securecuencial(df):
    ingresos_por_region = defaultdict(float)
    contador_productos = Counter()
    ingresos_por_mes = defaultdict(float)
    for _, fila in df.iterrows():
        region = fila['region']
        categoria = fila['categoria']
        cantidad = int(fila['cantidad'])
        precio_unitario = int(fila['precio_unitario'])
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