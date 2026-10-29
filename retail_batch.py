
import csv
from pathlib import Path

from openpyxl import load_workbook
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    avg,
    round as spark_round,
    date_format,
    to_timestamp,
    desc
)

# --------------------------------------------------
# 1. CONFIGURACION
# --------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent

EXCEL_FILE = str(BASE_DIR / "Online Retail.xlsx")
CSV_FILE = str(BASE_DIR / "Online Retail.csv")
OUTPUT_DIR = str(BASE_DIR / "retail_resultados")

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# 2. CONVERTIR EXCEL A CSV PARA LEERLO CON SPARK
# --------------------------------------------------

def convertir_excel_a_csv():
    if Path(CSV_FILE).exists():
        print(f"\nArchivo CSV existente: {CSV_FILE}")
        print("Se utilizara para cargar los datos en Spark.")
        return

    print("\nConvirtiendo Excel a CSV...")
    workbook = load_workbook(
        EXCEL_FILE,
        read_only=True,
        data_only=True
    )
    sheet = workbook.active

    with open(CSV_FILE, "w", newline="", encoding="utf-8-sig") as archivo:
        writer = csv.writer(archivo)

        for row in sheet.iter_rows(values_only=True):
            fila = []
            for value in row:
                if hasattr(value, "strftime"):
                    value = value.strftime("%Y-%m-%d %H:%M:%S")
                fila.append(value)
            writer.writerow(fila)

    workbook.close()
    print(f"Conversion completada: {CSV_FILE}")


# --------------------------------------------------
# 3. INICIAR SPARK Y CARGAR LOS DATOS
# --------------------------------------------------

def main():
    convertir_excel_a_csv()

    spark = (
        SparkSession.builder
        .appName("OnlineRetailBatchAnalysis")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("\n=== ANALISIS BATCH: ONLINE RETAIL ===")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .csv(CSV_FILE)
    )

    # --------------------------------------------------
    # 4. EXPLORACION INICIAL
    # --------------------------------------------------

    print("\n--- 1. ESTRUCTURA DEL DATAFRAME ---")
    df.printSchema()

    print("\n--- 2. PRIMERAS CINCO FILAS ---")
    df.show(5, truncate=False)

    total_original = df.count()
    print(f"\nTotal de filas originales: {total_original}")
    print(f"Total de columnas: {len(df.columns)}")

    print("\n--- 3. VALORES NULOS POR COLUMNA ---")
    df.select([
        count(col(c)).alias(c) for c in df.columns
    ]).show(truncate=False)

    print("\n--- 4. REGISTROS DUPLICADOS ---")
    duplicados = total_original - df.dropDuplicates().count()
    print(f"Filas duplicadas exactas: {duplicados}")

    # --------------------------------------------------
    # 5. LIMPIEZA Y TRANSFORMACION
    # --------------------------------------------------

    print("\n--- 5. LIMPIEZA DE DATOS ---")

    df_limpio = (
        df
        .withColumn("Quantity", col("Quantity").cast("integer"))
        .withColumn("UnitPrice", col("UnitPrice").cast("double"))
        .withColumn("CustomerID", col("CustomerID").cast("double"))
        .withColumn(
            "InvoiceDate",
            to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss")
        )
        .filter(
            col("InvoiceNo").isNotNull()
            & col("StockCode").isNotNull()
            & col("Quantity").isNotNull()
            & col("UnitPrice").isNotNull()
            & col("InvoiceDate").isNotNull()
        )
        .dropDuplicates()
        .withColumn(
            "LineAmount",
            spark_round(col("Quantity") * col("UnitPrice"), 2)
        )
        .withColumn(
            "InvoiceMonth",
            date_format(col("InvoiceDate"), "yyyy-MM")
        )
    )

    total_limpio = df_limpio.count()
    print(f"Filas antes de limpiar: {total_original}")
    print(f"Filas despues de limpiar: {total_limpio}")
    print(f"Filas excluidas: {total_original - total_limpio}")

    print(
        "\nNota: las cantidades negativas se conservan, "
        "ya que pueden representar devoluciones o cancelaciones."
    )

    # --------------------------------------------------
    # 6. ANALISIS EXPLORATORIO
    # --------------------------------------------------

    print("\n--- 6. RESUMEN ESTADISTICO ---")
    df_limpio.select(
        "Quantity", "UnitPrice", "LineAmount"
    ).describe().show()

    print("\n--- 7. INDICADORES GENERALES ---")
    df_limpio.agg(
        count("*").alias("Registros"),
        countDistinct("InvoiceNo").alias("Facturas"),
        countDistinct("StockCode").alias("Productos"),
        countDistinct("Country").alias("Paises"),
        spark_round(spark_sum("LineAmount"), 2).alias("Importe_neto")
    ).show(truncate=False)

    # Productos con mayor cantidad vendida en lineas positivas.
    print("\n--- 8. DIEZ PRODUCTOS CON MAYOR DEMANDA BRUTA ---")

    productos = (
        df_limpio
        .filter(col("Quantity") > 0)
        .groupBy("StockCode", "Description")
        .agg(spark_sum("Quantity").alias("Unidades_vendidas"))
        .orderBy(desc("Unidades_vendidas"))
        .limit(10)
    )
    productos.show(truncate=False)

    # Ingresos netos por pais.
    print("\n--- 9. DIEZ PAISES CON MAYOR IMPORTE NETO ---")

    paises = (
        df_limpio
        .groupBy("Country")
        .agg(
            spark_round(spark_sum("LineAmount"), 2)
            .alias("Importe_neto")
        )
        .orderBy(desc("Importe_neto"))
        .limit(10)
    )
    paises.show(truncate=False)

    # Evolucion mensual del importe neto.
    print("\n--- 10. IMPORTE NETO POR MES ---")

    mensual = (
        df_limpio
        .groupBy("InvoiceMonth")
        .agg(
            spark_round(spark_sum("LineAmount"), 2)
            .alias("Importe_neto")
        )
        .orderBy("InvoiceMonth")
    )
    mensual.show(30, truncate=False)

    # --------------------------------------------------
    # 7. GUARDAR RESULTADOS
    # --------------------------------------------------

    productos.toPandas().to_csv(
        f"{OUTPUT_DIR}/top_productos.csv",
        index=False,
        encoding="utf-8-sig"
    )
    paises.toPandas().to_csv(
        f"{OUTPUT_DIR}/top_paises.csv",
        index=False,
        encoding="utf-8-sig"
    )
    mensual.toPandas().to_csv(
        f"{OUTPUT_DIR}/ventas_mensuales.csv",
        index=False,
        encoding="utf-8-sig"
    )

    # --------------------------------------------------
    # 8. VISUALIZACIONES
    # --------------------------------------------------

    print("\n--- 11. GENERANDO GRAFICOS ---")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        # Grafico 1: productos con mayor demanda.
        pdf = productos.toPandas()
        pdf["Etiqueta"] = (
            pdf["Description"].fillna(pdf["StockCode"]).str.slice(0, 28)
        )

        plt.figure(figsize=(12, 6))
        plt.barh(
            pdf["Etiqueta"][::-1],
            pdf["Unidades_vendidas"][::-1]
        )
        plt.xlabel("Unidades vendidas")
        plt.title("Diez productos con mayor demanda")
        plt.tight_layout()
        plt.savefig(f"{OUTPUT_DIR}/top_productos.png", dpi=150)
        plt.close()

        # Grafico 2: importe neto por pais.
        cdf = paises.toPandas()

        plt.figure(figsize=(10, 6))
        plt.barh(
            cdf["Country"][::-1],
            cdf["Importe_neto"][::-1]
        )
        plt.xlabel("Importe neto")
        plt.title("Diez paises con mayor importe neto")
        plt.tight_layout()
        plt.savefig(f"{OUTPUT_DIR}/top_paises.png", dpi=150)
        plt.close()

        # Grafico 3: evolucion mensual.
        mdf = mensual.toPandas()

        plt.figure(figsize=(12, 6))
        plt.plot(
            mdf["InvoiceMonth"],
            mdf["Importe_neto"],
            marker="o"
        )
        plt.xlabel("Mes")
        plt.ylabel("Importe neto")
        plt.title("Evolucion mensual del importe neto")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{OUTPUT_DIR}/ventas_mensuales.png", dpi=150)
        plt.close()

        print(f"Graficos guardados en: {OUTPUT_DIR}")

    except ImportError:
        print(
            "No se encontro matplotlib. "
            "Los resultados tabulares se guardaron; "
            "instalaremos matplotlib si hace falta."
        )

    print("\n=== PROCESAMIENTO BATCH FINALIZADO ===")
    print(f"Resultados disponibles en: {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
