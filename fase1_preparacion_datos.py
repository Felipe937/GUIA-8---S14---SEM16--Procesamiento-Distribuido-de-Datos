"""
Script: fase1_preparacion_datos.py
Descripción: Genera el dataset distribuido de 50,000 registros para la Fase 1
y siguientes fases del proyecto de procesamiento distribuido de datos.
"""

import os
import random
import numpy as np
import pandas as pd
from datetime import datetime


# ---------------------------------------------------------------------------
# CONFIGURACIÓN DE CONSTANTES
# ---------------------------------------------------------------------------

# Categorías de productos
CATEGORIAS = [
    "Electrónica", "Ropa", "Alimentos", "Hogar", "Deportes",
    "Juegos", "Automotriz", "Belleza", "Muebles", "Oficina"
]

# Regiones de Colombia
REGIONES = [
    "Bogotá", "Medellín", "Cali", "Norte", "Sur", "Oriente", "Occidente"
]

# Estados de transacción
ESTADOS = [
    "Pendiente", "Completada", "Cancelada"
]


# ---------------------------------------------------------------------------
# FUNCIONES AUXILIARES
# ---------------------------------------------------------------------------

def generar_fecha_aleatoria() -> str:
    """Genera una fecha aleatoria en formato YYYY-MM-DD en el año 2024."""
    mes = random.randint(1, 12)
    dia = random.randint(1, 28)
    return datetime(2024, mes, dia).strftime("%Y-%m-%d")


def generar_dataset(n_registros: int = 50_000) -> pd.DataFrame:
    """
    Genera un DataFrame con `n_registros` registros de transacciones.
    
    Columnas:
    - id_transacción: identificador único consecutivo TRX-000001, TRX-000002, ...
    - fecha: fechas aleatorias distribuidas a lo largo de los 12 meses del año (2024)
    - categoría: categoría del producto (10 categorías técnicas)
    - región: región colombiana (Bogotá, Medellín, Cali, Norte, Sur, Oriente, Occidente)
    - estado: estado de la transacción (Pendiente, Completada, Cancelada)
    - cantidad: número entero aleatorio (1-150 unidades por venta)
    - precio_unitario: precio en pesos colombianos (COP) aleatorio
    - total_venta: cantidad × precio_unitario
    - tiempo_procesamiento_ms: tiempo de procesamiento simulado en ms (1-500)
    """
    # 1️⃣ IDs consecutivos (únicos y ordenados)
    ids = [f"TRX-{i+1:06d}" for i in range(n_registros)]

    # 2️⃣ Fechas aleatorias distribuidas en los 12 meses
    fechas = [generar_fecha_aleatoria() for _ in range(n_registros)]

    # 3️⃣ Categorías, regiones y estados aleatorios
    categorias = [random.choice(CATEGORIAS) for _ in range(n_registros)]
    regiones = [random.choice(REGIONES) for _ in range(n_registros)]
    estados = [random.choice(ESTADOS) for _ in range(n_registros)]

    # 4️⃣ Cantidades enteras entre 1 y 150
    cantidades = [random.randint(1, 150) for _ in range(n_registros)]

    # 5️⃣ Precios unitarios aleatorios en COP (10,000 - 2,000,000)
    precios_unitarios = [random.randint(10_000, 2_000_000) for _ in range(n_registros)]

    # 6️⃣ Totales (cantidad × precio_unitario)
    totales = [c * p for c, p in zip(cantidades, precios_unitarios)]

    # 7️⃣ Tiempos de procesamiento simulados (1-500 ms)
    tiempos = [random.randint(1, 500) for _ in range(n_registros)]

    # Ensamblar en DataFrame
    df = pd.DataFrame({
        "id_transaccion": ids,
        "fecha": fechas,
        "categoria": categorias,
        "region": regiones,
        "estado": estados,
        "cantidad": cantidades,
        "precio_unitario": precios_unitarios,
        "total_venta": totales,
        "tiempo_procesamiento_ms": tiempos
    })

    # Tipos de datos explícitos
    df["cantidad"] = df["cantidad"].astype(int)
    df["precio_unitario"] = df["precio_unitario"].astype(int)
    df["total_venta"] = df["total_venta"].astype(int)
    df["tiempo_procesamiento_ms"] = df["tiempo_procesamiento_ms"].astype(int)

    return df


def validar_y_guardar(df: pd.DataFrame, ruta_salida: str, nombre_archivo: str):
    """
    Guarda el DataFrame en CSV, calcula el tamaño en disco y muestra un
    resumen de validación en consola.
    """
    # Guardar en CSV (sin índice)
    df.to_csv(ruta_salida, index=False)

    # Tamaño en bytes y MB
    tamaño_bytes = os.path.getsize(ruta_salida)
    tamaño_mb = tamaño_bytes / (1024 * 1024)

    # Validaciones
    total_registros = len(df)
    tiene_nulos = df.isnull().any().any()
    id_duplicados = df["id_transaccion"].duplicated().any()

    # --- Salida en consola ---
    print(f"--- Validación para {nombre_archivo} ---")
    print(f"Registros generados: {total_registros}")
    print(f"Peso en disco: {tamaño_mb:.2f} MB")
    print(f"Valores nulos: {'No' if not tiene_nulos else 'Sí'}")
    print(f"id_transacción duplicados: {'Sí' if id_duplicados else 'No'}")
    print(f"Columnas: {list(df.columns)}")
    print()


# ---------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# ---------------------------------------------------------------------------

def main():
    # Directorio del proyecto (un nivel arriba donde está el script)
    proyecto_dir = os.path.dirname(os.path.abspath(__file__))
    carpeta_datos = os.path.join(proyecto_dir, 'data')
    os.makedirs(carpeta_datos, exist_ok=True)

    # Ruta de salida
    ruta_dataset = os.path.join(carpeta_datos, "dataset_distribuido.csv")

    # --- Generar conjunto de 50,000 registros ---
    df = generar_dataset(50_000)
    validar_y_guardar(df, ruta_dataset, "dataset_distribuido.csv")

    print("✅ Proceso completado. Dataset guardado en la carpeta `data/`.")
    print(f"   - {ruta_dataset}: 50,000 registros")


# ---------------------------------------------------------------------------
# PUNTO DE ENTRADA
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()