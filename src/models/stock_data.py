from pyspark.sql import DataFrame, SparkSession
import yfinance as yf

class StockDataModel:
    """Modelo responsable de la ingesta y manipulación de datos bursátiles."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def descargar_historico(
        self, ticker: str, start_date: str, end_date: str
    ) -> DataFrame:
        """
        Descarga el histórico de cotización para un ticker dado entre dos fechas
        y lo devuelve como un DataFrame de PySpark.
        """
        pdf = yf.Ticker(ticker).history(start=start_date, end=end_date, interval="1d", auto_adjust=False)
        pdf.reset_index(inplace=True)
        pdf["Date"] = pdf["Date"].dt.date
        pdf["Ticker"] = ticker
        return self.spark.createDataFrame(pdf)