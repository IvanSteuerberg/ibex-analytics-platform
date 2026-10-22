from pyspark.sql import DataFrame, SparkSession
import yfinance as yf
from src.models.company_data import CompanyDataModel, Company

class StockDataModel:
    """Modelo responsable de la ingesta y manipulación de datos bursátiles."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def normalizar_cols(self, pdf):
        pdf.columns = [str(c).replace(" ", "_") for c in pdf.columns]
        return pdf

    def descargar_historico(
        self, ticker: str, start_date: str, end_date: str
    ) -> DataFrame:
        """
        Descarga el histórico de cotización para un ticker dado entre dos fechas
        y lo devuelve como un DataFrame de PySpark.
        """
        pdf = yf.Ticker(ticker).history(start=start_date, end=end_date, interval="1d", auto_adjust=False)
        pdf.reset_index(inplace=True)
        pdf = self.normalizar_cols(pdf)
        pdf["Date"] = pdf["Date"].dt.date
        pdf["Ticker"] = ticker
        return self.spark.createDataFrame(pdf)

    def descargar_historico_total(self, start_date:str, end_date:str) -> DataFrame:
        """
        Descarga el histórico de cotización para todos los ticker dado entre dos fechas
        y lo devuelve como un DataFrame de PySpark.
        """
        df = None
        for company in CompanyDataModel().get_companies():
            ticker = company.ticker
            pdf = yf.Ticker(ticker).history(start=start_date, end=end_date, interval="1d", auto_adjust=False)
            pdf.reset_index(inplace=True)
            pdf = self.normalizar_cols(pdf)
            pdf["Date"] = pdf["Date"].dt.date
            pdf["Ticker"] = ticker

            df_temp = self.spark.createDataFrame(pdf)

            if df is None:
                df = df_temp
            else:
                df = df.unionByName(df_temp)

        return df