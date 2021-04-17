# TUTORIAL APACHA SPARK | PYSPARK

## Para levantar el master (se ejecuta dentro de la carpeta sbin)
spark-class org.apache.spark.deploy.master.Master

## Para levantar un worker (se ejecuta dentro de la carpeta sbin)
spark-class org.apache.spark.deploy.worker.Worker spark://192.168.56.1:7077

# PySpark (se ejecuta dentro de la carpeta bin)
pyspark --master spark://192.168.56.1:7077

## Leemos datos del DataSet y los escribimos sobre el DataFrame al que apunta la variable df
df = spark.read.options(header='True', inferSchema='True').csv("/APACHE SPARK/dataset/*.csv")

df.count()

df.printSchema()


## Mostrar o contar los diferentes valores de una columna
df.select('event_type').distinct().show()
df.select('product_id').distinct().count()

df.select("brand").distinct().show()

## Obtener 'product_id' del registro con 'event_type=cart'
df.select(['product_id']).filter("event_type='cart'").show()

## Obtener los productos que se han comprado conjuntamente con el anterior registro
sesions=df.select(['user_session']).filter("event_type='cart' AND product_id=5844305").distinct()
products=df.select(['product_id']).filter("event_type='cart' AND product_id<>5844305").filter( df["user_session"].isin(sesions["user_session"]) )


# Para scala (se ejecuta dentro de la carpeta bin)
spark-shell --master spark://192.168.56.1:7077

## Scala
val one = spark.createDataset(Seq(1))


