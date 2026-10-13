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
    
