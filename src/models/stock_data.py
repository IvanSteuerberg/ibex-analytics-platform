from pyspark.sql import DataFrame, SparkSession, functions as F
import yfinance as yf
from src.models.company_data import CompanyDataModel
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

    # Campos de yf.Ticker(t).info que se guardan de cada empresa: (clave en Yahoo, columna, tipo)
    CAMPOS_INFO = [
        ("longName", "Nombre", StringType()),
        ("sector", "Sector", StringType()),
        ("industry", "Industria", StringType()),
        ("country", "Pais", StringType()),
        ("marketCap", "MarketCap", LongType()),
        ("beta", "Beta", DoubleType()),
        ("dividendYield", "DividendYield", DoubleType()),
        ("recommendationMean", "RecomendacionMedia", DoubleType()),
    ]

    ESQUEMA_INFO = StructType(
        [StructField("Ticker", StringType(), False)]
        + [StructField(columna, tipo, True) for _, columna, tipo in CAMPOS_INFO]
    )

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
            df_temp = self.descargar_historico(company.ticker, start_date, end_date)

            if df is None:
                df = df_temp
            else:
                df = df.unionByName(df_temp)

        return df

    def descargar_info_empresas(self) -> DataFrame:
        """
        Descarga los datos descriptivos de cada empresa (sector, industria, país...) desde yf.Ticker(t).info.
        Es una foto del momento actual, no un histórico.
        """
        filas = []
        for company in CompanyDataModel().get_companies():
            info = yf.Ticker(company.ticker).info
            fila = [company.ticker]
            for clave, _, tipo in self.CAMPOS_INFO:
                valor = info.get(clave)
                # Yahoo a veces devuelve enteros en campos decimales (y Spark no los acepta como double)
                if valor is not None and isinstance(tipo, DoubleType):
                    valor = float(valor)
                fila.append(valor)
            filas.append(fila)
        return self.spark.createDataFrame(filas, schema=self.ESQUEMA_INFO)

    def anadir_close_mas5(self, df: DataFrame) -> DataFrame:
        """Añade la columna Close_mas5 = Close + 5."""
        return df.withColumn("Close_mas5", F.col("Close") + 5)