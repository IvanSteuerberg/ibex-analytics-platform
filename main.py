from src.config.spark_session import get_spark_session
from src.controllers.practice_controller import PracticeController


def main():
    spark = get_spark_session()
    controller = PracticeController(spark)

    # Ej1-a
    print("Ej1-a")
    # controller.ejecutar_ej1a()

    print("Ej1-b")
    # controller.ejecutar_ej1b()

    print("Ej1-c")
    controller.ejecutar_ej1c()

    # Finalizar la sesión de Spark
    spark.stop()



if __name__ == "__main__":
    main()
