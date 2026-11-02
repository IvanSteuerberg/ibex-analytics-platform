from src.config.spark_session import get_spark_session
from src.controllers.practice_controller import PracticeController


def main():
    spark = get_spark_session()
    controller = PracticeController(spark)

    # Ej1-a
    # print("Ej1-a")
    # controller.ejecutar_ej1a()

    # print("Ej1-b")
    # controller.ejecutar_ej1b()

    # print("Ej1-c")
    # controller.ejecutar_ej1c()

    # print("Ej2-a")
    # controller.ejecutar_ej2a()

    # print("Ej2-b")
    # controller.ejecutar_ej2b()

    # print("Ej2-c")
    # controller.ejecutar_ej2c()

    # print("Ej3")
    # controller.ejecutar_ej3()

    # print("Ej4")
    # controller.ejecutar_ej4()

    print("Ej5-a")
    controller.ejecutar_ej5a()

    print("Ej5-b")
    controller.ejecutar_ej5b()

    print("Ej5-c")
    controller.ejecutar_ej5c()

    print("Ej5-d")
    controller.ejecutar_ej5d()

    print("Ej6")
    controller.ejecutar_ej6()

    # Finalizar la sesión de Spark
    spark.stop()



if __name__ == "__main__":
    main()
