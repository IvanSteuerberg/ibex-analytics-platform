from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
import yfinance as yf
from src.models.company_data import CompanyDataModel, Company
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, LongType, StringType

class StockDataModel:
    """Modelo responsable de la ingesta y manipulación de datos bursátiles."""
    
    ESQUEMA = StructType([
        StructField("Date", DateType(), False),
        StructField("Open", DoubleType(), False),
        StructField("High", DoubleType(), False),
        StructField("Low", DoubleType(), False),
        StructField("Close", DoubleType(), False),
        StructField("Adj_Close", DoubleType(), False),
        StructField("Volume", LongType(), True),
        StructField("Ticker", StringType(), False),
        StructField("Dividends", DoubleType(), False),
        StructField("Stock_Splits", DoubleType(), False)
    ])
    
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
        pdf = pdf[self.ESQUEMA.fieldNames()]  # Asegura que las columnas estén en el orden correcto

        return self.spark.createDataFrame(pdf, schema=self.ESQUEMA)

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
            pdf = pdf[self.ESQUEMA.fieldNames()]  # Asegura que las columnas estén en el orden correcto
            df_temp = self.spark.createDataFrame(pdf, schema=self.ESQUEMA)

            if df is None:
                df = df_temp
            else:
                df = df.unionByName(df_temp)

        return df

    def obtener_version(self, ruta:str):
        v = DeltaTable.forPath(self.spark, ruta).history(1).select("version").collect()[0]["version"]
        return v

    def merge_delta(self, nuevos: DataFrame, ruta: str):
        """
        Realiza un merge de un DataFrame con un DeltaTable existente.
        """
        destino = DeltaTable.forPath(self.spark, ruta)
        (destino.alias("d").merge(nuevos.alias("n"), "d.Ticker = n.Ticker AND d.Date = n.Date")
            .whenNotMatchedInsertAll()
            .execute())

    def borrar_filas_ticker(self, ruta: str, ticker: str):
        destino = DeltaTable.forPath(self.spark, ruta)
        destino.delete(f"Ticker = '{ticker}'")