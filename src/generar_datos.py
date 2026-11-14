import os
import random
import numpy as np
import pandas as pd
from datetime import datetime

# Ciudades reales de Colombia con pesos realistas
CIUDADES = ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena", "Bucaramanga", "Pereira", "Santa Marta", "Manizales", "Cúcuta"]
CIUDAD_PESOS = [0.30, 0.15, 0.10, 0.08, 0.05, 0.05, 0.05, 0.03, 0.03, 0.01]

# Catálogo de 10 productos tecnológicos/oficina con precios en COP
PRODUCTOS = ["Laptop", "Mouse", "Teclado", "Monitor", "Impresora", "Tablet", "Cargador", "USB", "Software Ofi", "Headphones"]
PRECIOS_PRODUCTO = {"Laptop": 2500000, "Mouse": 85000, "Teclado": 120000, "Monitor": 800000, "Impresora": 1200000, "Tablet": 1500000, "Cargador": 75000, "USB": 35000, "Software Ofi": 250000, "Headphones": 350000}

MESES_PESOS = {1: 0.06, 2: 0.06, 3: 0.07, 4: 0.07, 5: 0.13, 6: 0.13, 7: 0.07, 8: 0.07, 9: 0.07, 10: 0.07, 11: 0.12, 12: 0.12}

def generar_fecha_aleatoria():
    mes = random.choices(population=list(MESES_PESOS.keys()), weights=list(MESES_PESOS.values()))[0]
    dia = random.randint(1, 28)
    return datetime(2024, mes, dia).strftime("%Y-%m-%d")

def generar_registro(n_registros):
    ids = [f"VTA-{i+1:07d}" for i in range(n_registros)]
    fechas = [generar_fecha_aleatoria() for _ in range(n_registros)]
    ciudades = random.choices(CIUDADES, weights=CIUDAD_PESOS, k=n_registros)
    productos = random.choices(PRODUCTOS, k=n_registros)
    precios = [PRECIOS_PRODUCTO[p] for p in productos]
    cantidades = [random.randint(1, 15) for _ in range(n_registros)]
    df = pd.DataFrame({"id_venta": ids, "fecha": fechas, "ciudad": ciudades, "producto": productos, "cantidad": cantidades, "precio_unitario": precios})
    df["cantidad"] = df["cantidad"].astype(int)
    df["precio_unitario"] = df["precio_unitario"].astype(int)
    return df

def validar_y_guardar(df, ruta_salida, nombre_archivo):
    df.to_csv(ruta_salida, index=False)
    tamaño_bytes = os.path.getsize(ruta_salida)
    tamaño_mb = tamaño_bytes / (1024 * 1024)
    total_registros = len(df)
    tiene_nulos = df.isnull().any().any()
    id_duplicados = df["id_venta"].duplicated().any()
    print(f"--- Validación para {nombre_archivo} ---")
    print(f"Registros generados: {total_registros}")
    print(f"Peso en disco: {tamaño_mb:.2f} MB")
    print(f"Valores nulos: {'No' if not tiene_nulos else 'Sí'}")
    print(f"id_venta duplicados: {'Sí' if id_duplicados else 'No'}")
    print("Primeras 5 filas de muestra:")
    print(df.head().to_string(index=False))
    print()

def main():
    # Directorio del proyecto (un nivel arriba de `src/`)
    proyecto_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    carpeta_datos = os.path.join(proyecto_dir, 'data')
    os.makedirs(carpeta_datos, exist_ok=True)
    ruta_pequeno = os.path.join(carpeta_datos, "ventas_pequeno.csv")
    ruta_grande = os.path.join(carpeta_datos, "ventas_grande.csv")
    df_pequeno = generar_registro(5_000)
    validar_y_guardar(df_pequeno, ruta_pequeno, "ventas_pequeno.csv")
    df_grande = generar_registro(1_000_000)
    validar_y_guardar(df_grande, ruta_grande, "ventas_grande.csv")
    print("✅ Proceso completado. Archivos guardados en la carpeta `data/`.")
    print(f"   - {ruta_pequeno}: 5 000 registros")
    print(f"   - {ruta_grande}: 1 000 000 de registros")

if __name__ == "__main__":
    main()
