#Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F
# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()
# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'
# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'tr>
# Eliminar duplicados 
df= df.dropDuplicates()
# Eliminar filas con valores nulos
df = df.na.drop()
#imprimimos el esquema
df.printSchema()
# Muestra las primeras filas del DataFrame
df.show()
# Estadisticas básicas
df.summary().show()
# Consulta: Filtrar por valor y seleccionar columnas
print("Peaje con valor mayor a 2000\n")
valor = df.filter(F.col('VALOR') > 2000).select('VALOR','PEAJE','ULTIMOFECHACAM>
valor.show()
# Ordenar filas por los valores en la columna "VALOR" en orden descendente
print("Valores ordenados de mayor a menor\n")
sorted_df = df.sort(F.col("VALOR").desc())
sorted_df.show()
# Hallar el peaje más barato
peaje_mas_barato = df.orderBy("Valor").first()
print(f"Peaje más barato: {peaje_mas_barato['IdPeaje']}, Valor: {peaje_mas_bara>

# Hallar el peaje más caro
peaje_mas_caro = df.orderBy(df["Valor"].desc()).first()
print(f"Peaje más caro: {peaje_mas_caro['IdPeaje']}, Valor: {peaje_mas_caro['Va>
# Agrupar por IdCategoriaTarifa y calcular estadísticas
estadisticas_categoria = df.groupBy("IdCategoriaTarifa").agg(
    F.avg("Valor").alias("Promedio"),
    F.max("Valor").alias("Maximo"),
    F.min("Valor").alias("Minimo"),
    F.expr("percentile_approx(Valor, 0.5)").alias("Mediana"),  # Cálculo de la >
    F.count("Valor").alias("Conteo")  # Contar el número de tarifas en cada cat>
)
# Mostrar resultados
estadisticas_categoria.show()
