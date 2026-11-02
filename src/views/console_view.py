from pyspark.sql import DataFrame


class ConsoleView:
    """Vista encargada de presentar datos y justificaciones teóricas por consola."""

    @staticmethod
    def mostrar_dataframe(df: DataFrame, num_filas: int = 5, truncar: bool = False):
        """Muestra un número determinado de filas de un DataFrame de PySpark."""
        if df is not None:
            df.show(num_filas, truncate=truncar)

    @staticmethod
    def mostrar_esquema(df: DataFrame):
        """Imprime el esquema de un DataFrame de PySpark."""
        if df is not None:
            df.printSchema()

    @staticmethod
    def mostrar_historial(df_historial: DataFrame):
        """Muestra el historial de versiones de una tabla Delta sin truncar."""
        df_historial.show(truncate=False)

    @staticmethod
    def mostrar_version(version: int):
        print(f"Versión actual de la tabla: {version}")

    @staticmethod
    def mostrar_filas_escritas(filas: int):
        print(f"Se han escrito {filas} filas")

    @staticmethod
    def mostrar_filas_actuales(filas: int):
        print(f"Filas actuales de la tabla: {filas}")

    @staticmethod
    def mostrar_antes_despues(antes: int, despues: int):
        print(f"Filas antes: {antes} | Filas después: {despues}")

    @staticmethod
    def mostrar_comparacion_versiones(filas_actual: int, filas_antigua: int, version_antigua: int):
        print(f"Filas actual: {filas_actual} | Filas versión {version_antigua}: {filas_antigua}")

    @staticmethod
    def mostrar_error(e: Exception):
        """Muestra solo la primera línea del error (el mensaje completo de Spark es muy largo)."""
        print(str(e).split("\n")[0])

    @staticmethod
    def mostrar_justificacion_nulos():
        """Muestra la justificación teórica sobre la nulabilidad de las columnas."""
        print("Fecha: no nulo. Clave temporal fundamental para ordenar y cruzar series temporales.")
        print("Ticker: no nulo. Identificador único de la empresa.")
        print("Open, High, Low, Close: no nulo. Precios obligatorios para representar la sesión bursátil.")
        print("Volume: nulo permitido. Puede no registrar negociación por suspensiones o falta de liquidez.")
        print("Dividends: no nulo. Los días sin reparto se representan explícitamente con 0.0.")
        print("Stock Splits: no nulo. Los días sin desdoblamiento se representan explícitamente con 0.0.")
        """
        Además, los días en que una empresa no cotiza
        (porque aún no existía en bolsa, por fines de semana o por festivos bursátiles) 
        se representan por la ausencia del registro, no por una fila rellena de nulos.
        """
    
    @staticmethod
    def mostrar_justificacion_select():
        print("Parquet es columnar, así que Spark solo lee las 3 columnas pedidas y se salta el resto "
              "(column pruning): menos I/O, menos memoria y más velocidad.")

    @staticmethod
    def mostrar_justificacion_csv():
        print("Todas las columnas llegan como string porque un CSV es texto plano " \
        "y no guarda tipos.")
        
    
    @staticmethod
    def mostrar_justificacion_append_parquet():
        print("Las filas se duplican: Parquet no detecta duplicados y append solo añade ficheros nuevos.")

    @staticmethod
    def mostrar_justificacion_delta_log():
        print("Aparece la carpeta _delta_log con el commit 00000000000000000000.json (versión 0, operación CONVERT). "
      "Guarda el esquema y la lista de ficheros parquet existentes; los datos no se reescriben.")

    @staticmethod
    def mostrar_justificacion_merge_delta():
        print("La carga es idempotente: repetirla no duplica nada. " \
        "Las filas se quedan igual")

    @staticmethod
    def mostrar_justificacion_merge_schema():
        print("Al añadir la columna Close_mas5, Spark detecta que el esquema ha cambiado. "
              "Si no se indica mergeSchema, se lanza un error. "
              "Con mergeSchema, Spark actualiza el esquema del DeltaTable y añade la nueva columna.")

    @staticmethod
    def mostrar_justificacion_version_antigua():
        print("No está igual: versionAsOf solo lee la versión antigua, la tabla actual no cambia.")
