# Análisis Batch de Online Retail con Apache Spark

## Descripción

Este proyecto realiza un análisis batch del conjunto de datos Online Retail mediante Apache Spark y PySpark. El objetivo es limpiar y transformar los datos, obtener indicadores descriptivos y explorar el comportamiento de las transacciones comerciales.

## Tecnologías utilizadas

- Ubuntu Server 22.04 LTS

- Apache Spark 3.5.8

- PySpark

- Python 3

- Pandas

- Matplotlib

- OpenPyXL

## Análisis realizado

- Carga y revisión de la estructura del conjunto de datos.

- Identificación y eliminación de registros duplicados.

- Limpieza y transformación de columnas.

- Cálculo del importe de las transacciones.

- Análisis de los principales productos por unidades vendidas.

- Identificación de los países con mayor importe neto.

- Análisis mensual del importe neto.

- Generación de tablas CSV y gráficos PNG.

## Archivos generados

El programa guarda los resultados en la carpeta retail_resultados:

- top_productos.csv y top_productos.png

- top_paises.csv y top_paises.png

- ventas_mensuales.csv y ventas_mensuales.png

## Requisitos

Se necesita Python 3, Apache Spark, PySpark y las bibliotecas OpenPyXL, Pandas y Matplotlib.

## Ejecución

1. Descargar el conjunto de datos Online Retail desde su fuente original y colocarlo en la ruta configurada en el script.

2. Verificar las rutas de entrada y salida en retail_batch.py.

3. Ejecutar el programa con:

```py
python3 retail_batch.py
```

4. Consultar las tablas y gráficos generados en la carpeta retail_resultados.

## Fuente de los datos

El conjunto de datos utilizado en este proyecto es **Online Retail**, publicado por el UCI Machine Learning Repository.

- **Página oficial:** https://archive.ics.uci.edu/dataset/352/online%2Bretail
- **Archivo original:** `Online Retail.xlsx`
- **Periodo de los datos:** del 1 de diciembre de 2010 al 9 de diciembre de 2011.
- **Licencia:** Creative Commons Attribution 4.0 International (CC BY 4.0).

El conjunto de datos debe descargarse por separado. No se incluye en este repositorio.

**Referencia:**

Chen, D. (2015). *Online Retail* [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5BW33
