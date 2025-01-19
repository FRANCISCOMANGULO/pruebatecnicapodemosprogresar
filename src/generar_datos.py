from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid
import random
from datetime import datetime, timedelta

def crear_spark_session():
    #Crear y configurar la sesión de Spark#
    return SparkSession.builder \
        .appName("GenerarDatos") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "8g") \
        .getOrCreate()

def generar_datos_muestra(numero_filas):
    #Genera datos de muestra para eventos#
    tipos_eventos = ["lunar", "solar", "asteroide", "meteoro", "planetario"]
    ubicaciones = ["NorteAmerica", "SurAmerica", "Europa", "Asia", "Africa", "Oceania"]
    
    datos = []
    fecha_inicio = datetime(2020, 1, 1)
    
    for _ in range(numero_filas):
        datos.append((
            str(uuid.uuid4()),                     # event_id
            random.choice(tipos_eventos),          # event_type
            fecha_inicio + timedelta(days=random.randint(0, 1825)),  # timestamp
            random.choice(ubicaciones),            # location
            f"Detalles del evento {_}"            # details
        ))
    
    return datos

def crear_esquema():
    #Define el esquema para el DataFrame#
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("location", StringType(), False),
        StructField("details", StringType(), True)
    ])

def main():
    # Configuración
    filas_por_lote = 1000000  # 1 millón de registros por lote
    numero_lotes = 500        # Total: 500 millones de registros ≈ 50GB
    
    # Inicializar Spark
    spark = crear_spark_session()
    
    try:
        esquema = crear_esquema()
        
        print("Iniciando generación de datos...")
        for numero_lote in range(numero_lotes):
            print(f"Procesando lote {numero_lote + 1} de {numero_lotes}")
            
            datos_muestra = generar_datos_muestra(filas_por_lote)
            df_lote = spark.createDataFrame(datos_muestra, esquema)
            
            # Guardar en la carpeta data
            modo = "overwrite" if numero_lote == 0 else "append"
            df_lote.write.mode(modo).parquet("data/eventos")
            
            # Limpiar memoria
            df_lote.unpersist()
        
        # Verificar total de registros generados
        df_final = spark.read.parquet("data/eventos")
        total_registros = df_final.count()
        print(f"\nTotal de registros generados: {total_registros}")
        
        print("\nGeneración de datos completada exitosamente")
        
    except Exception as e:
        print(f"Error durante la generación de datos: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()