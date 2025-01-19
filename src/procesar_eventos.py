from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def crear_spark_session():
    #Crear y configurar la sesión de Spark#
    return SparkSession.builder \
        .appName("ProcesarEventosLunares") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def main():
    # Inicializar Spark
    spark = crear_spark_session()
    
    try:
        print("Iniciando procesamiento de eventos lunares...")
        
        # 1. Leer datos generados
        df = spark.read.parquet("data/eventos")
        print(f"Total de registros leídos: {df.count()}")
        
        # 2. Filtrar eventos lunares y crear columna año
        eventos_lunares = df.filter(col("event_type") == "lunar") \
            .withColumn("year", year("timestamp"))
        
        total_lunares = eventos_lunares.count()
        print(f"Total de eventos lunares encontrados: {total_lunares}")
        
        # 3. Agrupar por año y calcular total de eventos
        resumen_anual = eventos_lunares.groupBy("year") \
            .agg(count("*").alias("total_eventos"))
        
        print("\nResumen de eventos lunares por año:")
        resumen_anual.orderBy("year").show()
        
        # 4. Guardar en datamart/
        print("Guardando resultados en formato Parquet...")
        eventos_lunares \
            .write \
            .partitionBy("year") \
            .mode("overwrite") \
            .parquet("datamart")
        
        print("\nProcesamiento completado exitosamente")
        
    except Exception as e:
        print(f"Error durante el procesamiento: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()