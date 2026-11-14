"""
Script Fase 4: Pruebas de Rendimiento
=====================================

Compara procesamiento secuencial vs paralelo (4 workers de multiprocessing)
para diferentes volúmenes de datos: 10,000, 100,000 y 500,000 registros.

Reutiliza EXACTAMENTE las operaciones de la Fase 3 sin modificarlas:
- extraer_mes()
- procesar_particion()  (ingresos por ciudad, productos, ingresos por mes)
- consolidar_resultados()
- ejecucion_secuencial()

Y la generación de datos de la Fase 1:
- generar_dataset(n)

Cada configuración (volumen × modalidad) se mide 3 veces.
Se calcula promedio y desviación estándar.
Se verifica que resultados secuencial y paralelo sean idénticos.
"""

import os
import sys
import json
import time
import pickle
import platform
import statistics
import multiprocessing
from datetime import datetime

import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from multiprocessing import Pool

# ---------------------------------------------------------------------------
# IMPORTAR FUNCIONES DE FASES ANTERIORES (sin modificar su lógica)
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from fase3_procesamiento_distribuido import (
    extraer_mes,
    procesar_particion,
    consolidar_resultados,
    ejecucion_secuencial,
)

from fase1_preparacion_datos import generar_dataset

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------

RESULTADOS_DIR = os.path.join(BASE_DIR, "resultados_fase4")
DEFAULT_VOLUMENES = [10_000, 100_000, 500_000]
DEFAULT_NUM_REPETICIONES = 3
NUM_WORKERS = 4

VOLUMENES = DEFAULT_VOLUMENES.copy()
NUM_REPETICIONES = DEFAULT_NUM_REPETICIONES

os.makedirs(RESULTADOS_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# DETECCIÓN DE ENTORNO
# ---------------------------------------------------------------------------

def detectar_entorno():
    """Detecta características del equipo (hardware y software)."""
    return {
        "fecha_ejecucion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "sistema_operativo": platform.system(),
        "version_so": platform.version(),
        "release_so": platform.release(),
        "procesador": platform.processor(),
        "arquitectura": platform.machine(),
        "nucleos_logicos": multiprocessing.cpu_count(),
        "python_version": platform.python_version(),
    }


# ---------------------------------------------------------------------------
# EJECUCIÓN SECUENCIAL
# ---------------------------------------------------------------------------

def ejecutar_secuencial(df):
    """
    Ejecuta las 3 operaciones de la Fase 3 de forma secuencial.

    Retorna:
        resultado : dict con ingresos_por_region, top_productos, ingresos_por_mes
        tiempo    : segundos transcurridos
    """
    inicio = time.perf_counter()
    resultado = ejecucion_secuencial(df)
    fin = time.perf_counter()
    return resultado, fin - inicio


# ---------------------------------------------------------------------------
# EJECUCIÓN PARALELA (4 workers)
# ---------------------------------------------------------------------------

def ejecutar_paralelo(df):
    """
    Ejecuta las 3 operaciones de la Fase 3 en paralelo con 4 workers.

    Mide cada etapa por separado:
      1. Particionamiento: dividir el DataFrame en 4 particiones
      2. Transferencia: serialización (pickle) de las particiones
      3. Workers: pool.map con procesamiento en procesos independientes
      4. Consolidación: unificar resultados parciales

    Retorna:
        resultado     : dict consolidado
        tiempo_total  : suma de todas las etapas
        tiempos_etapas: dict con tiempo de cada etapa
    """
    tiempos_etapas = {}

    # --- Etapa 1: Particionamiento ---
    t0 = time.perf_counter()
    particiones = [df.iloc[i::NUM_WORKERS].reset_index(drop=True) for i in range(NUM_WORKERS)]
    datos_particiones = [p.to_dict("records") for p in particiones]
    t1 = time.perf_counter()
    tiempos_etapas["particionamiento"] = t1 - t0

    # --- Etapa 2: Transferencia (serialización para envío a workers) ---
    t0 = time.perf_counter()
    _ = [pickle.dumps(d) for d in datos_particiones]
    t1 = time.perf_counter()
    tiempos_etapas["transferencia"] = t1 - t0

    # --- Etapa 3: Workers (procesamiento paralelo) ---
    t0 = time.perf_counter()
    with Pool(NUM_WORKERS) as pool:
        resultados_parciales = pool.map(procesar_particion, datos_particiones)
    t1 = time.perf_counter()
    tiempos_etapas["workers"] = t1 - t0

    # --- Etapa 4: Consolidación ---
    t0 = time.perf_counter()
    consolidado = consolidar_resultados(resultados_parciales)
    t1 = time.perf_counter()
    tiempos_etapas["consolidacion"] = t1 - t0

    tiempo_total = sum(tiempos_etapas.values())
    return consolidado, tiempo_total, tiempos_etapas


# ---------------------------------------------------------------------------
# COMPARACIÓN DE RESULTADOS
# ---------------------------------------------------------------------------

def comparar_resultados(res_secuencial, res_paralelo):
    """
    Verifica que los resultados secuencial y paralelo sean idénticos.

    Retorna:
        (es_identico: bool, mensaje: str)
    """
    regiones_s = res_secuencial["ingresos_por_region"]
    regiones_p = res_paralelo["ingresos_por_region"]

    if set(regiones_s.keys()) != set(regiones_p.keys()):
        return False, "Conjuntos de regiones diferentes"

    for region in regiones_s:
        if abs(regiones_s[region] - regiones_p.get(region, 0)) > 0.01:
            return False, f"Diferencia en ingresos por región: {region}"

    productos_s = res_secuencial["top_productos"]
    productos_p = res_paralelo["top_productos_globales"]

    if productos_s != productos_p:
        return False, "Top 5 productos diferentes"

    meses_s = res_secuencial["ingresos_por_mes"]
    meses_p = res_paralelo["ingresos_por_mes"]

    if set(meses_s.keys()) != set(meses_p.keys()):
        return False, "Conjuntos de meses diferentes"

    for mes in meses_s:
        if abs(meses_s[mes] - meses_p.get(mes, 0)) > 0.01:
            return False, f"Diferencia en ingresos por mes: {mes}"

    return True, "Resultados idénticos"


# ---------------------------------------------------------------------------
# GENERACIÓN DE GRÁFICO
# ---------------------------------------------------------------------------

def generar_grafico(resultados_csv):
    """Genera comparativa_rendimiento.png con dos paneles."""
    volúmenes = [r["volumen"] for r in resultados_csv]
    seq_tiempos = [r["secuencial_promedio_s"] for r in resultados_csv]
    par_tiempos = [r["paralelo_promedio_s"] for r in resultados_csv]
    speedups = [r["speedup"] for r in resultados_csv]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Panel 1: Tiempos de ejecución
    x = range(len(volúmenes))
    ax1.bar([i - 0.2 for i in x], seq_tiempos, width=0.4, label="Secuencial", color="#2c3e50")
    ax1.bar([i + 0.2 for i in x], par_tiempos, width=0.4, label="Paralelo (4 workers)", color="#27ae60")
    ax1.set_xlabel("Volumen de registros")
    ax1.set_ylabel("Tiempo promedio (segundos)")
    ax1.set_title("Tiempo de Ejecución: Secuencial vs Paralelo")
    ax1.set_xticks(list(x))
    ax1.set_xticklabels([f"{v:,}" for v in volúmenes])
    ax1.legend()
    ax1.grid(axis="y", alpha=0.3)

    # Panel 2: Speedup
    ax2.bar(list(x), speedups, width=0.6, color="#e74c3c")
    ax2.axhline(y=1.0, color="black", linestyle="--", alpha=0.5, label="Speedup = 1")
    ax2.set_xlabel("Volumen de registros")
    ax2.set_ylabel("Speedup (Secuencial / Paralelo)")
    ax2.set_title("Speedup por Volumen de Datos")
    ax2.set_xticks(list(x))
    ax2.set_xticklabels([f"{v:,}" for v in volúmenes])
    ax2.legend()
    ax2.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        os.path.join(RESULTADOS_DIR, "comparativa_rendimiento.png"),
        dpi=150,
        bbox_inches="tight",
    )
    plt.close()


# ---------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("FASE 4 - PRUEBAS DE RENDIMIENTO")
    print("=" * 70)

    # --- Detección de entorno ---
    entorno = detectar_entorno()
    print("\n--- Entorno detectado ---")
    for clave, valor in entorno.items():
        print(f"  {clave}: {valor}")

    ruta_entorno = os.path.join(RESULTADOS_DIR, "entorno_pruebas.json")
    with open(ruta_entorno, "w", encoding="utf-8") as f:
        json.dump(entorno, f, indent=2, ensure_ascii=False)
    print(f"\nEntorno guardado en: {ruta_entorno}")

    resultados_csv = []

    for volumen in VOLUMENES:
        print(f"\n{'='*70}")
        print(f"Volumen: {volumen:,} registros")
        print(f"{'='*70}")

        # Generar datos con la misma lógica de Fase 1
        df = generar_dataset(volumen)
        print(f"  Dataset generado: {len(df):,} registros | Columnas: {list(df.columns)}")

        # ===================================================================
        # SECUENCIAL
        # ===================================================================
        tiempos_seq = []
        resultado_seq = None
        for rep in range(NUM_REPETICIONES):
            res, t = ejecutar_secuencial(df)
            tiempos_seq.append(t)
            if rep == 0:
                resultado_seq = res
            print(f"  Secuencial rep {rep + 1}: {t:.4f} s")

        seq_promedio = statistics.mean(tiempos_seq)
        seq_std = statistics.stdev(tiempos_seq) if len(tiempos_seq) > 1 else 0.0

        # ===================================================================
        # PARALELO (4 workers)
        # ===================================================================
        tiempos_par = []
        etapas_acumuladas = {
            "particionamiento": [],
            "transferencia": [],
            "workers": [],
            "consolidacion": [],
        }
        resultado_par = None
        for rep in range(NUM_REPETICIONES):
            res, t_total, etapas = ejecutar_paralelo(df)
            tiempos_par.append(t_total)
            for clave in etapas_acumuladas:
                etapas_acumuladas[clave].append(etapas[clave])
            if rep == 0:
                resultado_par = res
            print(f"  Paralelo  rep {rep + 1}: {t_total:.4f} s")
            print(f"    - Particionamiento: {etapas['particionamiento']:.4f} s")
            print(f"    - Transferencia:    {etapas['transferencia']:.4f} s")
            print(f"    - Workers:          {etapas['workers']:.4f} s")
            print(f"    - Consolidacion:    {etapas['consolidacion']:.4f} s")

        par_promedio = statistics.mean(tiempos_par)
        par_std = statistics.stdev(tiempos_par) if len(tiempos_par) > 1 else 0.0

        # Etapas promedio
        etapas_promedio = {
            clave: statistics.mean(vals) for clave, vals in etapas_acumuladas.items()
        }

        # Speedup
        speedup = seq_promedio / par_promedio if par_promedio > 0 else 0.0

        # Validación de resultados idénticos
        identicos, mensaje = comparar_resultados(resultado_seq, resultado_par)
        print(f"\n  Resultados idénticos: {'SÍ' if identicos else 'NO'} - {mensaje}")

        # Almacenar fila para CSV
        resultados_csv.append(
            {
                "volumen": volumen,
                "secuencial_promedio_s": round(seq_promedio, 4),
                "secuencial_std_s": round(seq_std, 4),
                "paralelo_promedio_s": round(par_promedio, 4),
                "paralelo_std_s": round(par_std, 4),
                "speedup": round(speedup, 4),
                "particionamiento_promedio_s": round(etapas_promedio["particionamiento"], 4),
                "transferencia_promedio_s": round(etapas_promedio["transferencia"], 4),
                "workers_promedio_s": round(etapas_promedio["workers"], 4),
                "consolidacion_promedio_s": round(etapas_promedio["consolidacion"], 4),
                "resultados_identicos": identicos,
            }
        )

    # ===================================================================
    # GUARDAR tabla_rendimiento.csv
    # ===================================================================
    df_resultados = pd.DataFrame(resultados_csv)
    ruta_csv = os.path.join(RESULTADOS_DIR, "tabla_rendimiento.csv")
    df_resultados.to_csv(ruta_csv, index=False)
    print(f"\nTabla guardada en: {ruta_csv}")

    # ===================================================================
    # GENERAR comparativa_rendimiento.png
    # ===================================================================
    generar_grafico(resultados_csv)
    ruta_png = os.path.join(RESULTADOS_DIR, "comparativa_rendimiento.png")
    print(f"Gráfico guardado en: {ruta_png}")

    # ===================================================================
    # RESUMEN FINAL
    # ===================================================================
    print(f"\n{'='*70}")
    print("RESUMEN DE PRUEBAS DE RENDIMIENTO")
    print(f"{'='*70}")
    print(df_resultados.to_string(index=False))
    print(f"\nTodos los resultados guardados en: {RESULTADOS_DIR}/")


# ---------------------------------------------------------------------------
# PUNTO DE ENTRADA
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()