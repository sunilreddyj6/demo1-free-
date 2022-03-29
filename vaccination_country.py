from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("spark").getOrCreate()
dfVaccinations = spark.read.csv('../../data/vaccination-data.csv', header=True)
dfPopulation = spark.read.csv('../../data/population-data.csv', header=True)
dfJoined = dfVaccinations.join(
    dfPopulation, dfVaccinations["ISO3"] == dfPopulation["Country Code"])

dfWithPercentages = dfJoined.select(
    "COUNTRY", "COUNTRY Code", dfJoined["2020"].alias("TOTAL POPULATION"),
    dfJoined["PERSONS_FULLY_VACCINATED"].alias("VACCINATED POPULATION"),
    "WHO_REGION", (dfJoined["PERSONS_FULLY_VACCINATED"] / dfJoined["2020"] *
                   100).alias('PERCENTAGE'))

dfFinal = dfWithPercentages.withColumn(
    "Rank",
    dense_rank().over(
        Window.partitionBy("WHO_REGION").orderBy(desc("PERCENTAGE"))))
dfFinal.write.option("header", True).partitionBy ("WHO_REGION").csv("REGION")
