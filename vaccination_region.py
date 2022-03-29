from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
dfVaccinations = spark.read.csv('./vaccination-data.csv', header=True)
dfPopulation = spark.read.csv('./population-data.csv', header=True)
dfJoined = dfVaccinations.join(
    dfPopulation, dfVaccinations["ISO3"] == dfPopulation["Country Code"])

dfWithPercentages = dfJoined.select(
    "COUNTRY", "COUNTRY Code", dfJoined["2020"].alias("TOTAL POPULATION"),
    dfJoined["PERSONS_FULLY_VACCINATED"].alias("VACCINATED POPULATION"),
    "WHO_REGION", (dfJoined["PERSONS_FULLY_VACCINATED"] / dfJoined["2020"] *
                   100).alias('PERCENTAGE'))

dfSum = dfWithPercentages.groupBy("WHO_REGION").agg({
    'TOTAL POPULATION':
    'sum',
    "VACCINATED POPULATION":
    "sum"
})
dfSum.select(
    "WHO_REGION",
    dfSum["sum(TOTAL POPULATION)"].cast(LongType()).alias("TOTAL POPULATION"),
    dfSum["sum(VACCINATED POPULATION)"].cast(
        LongType()).alias("VACCINATED POPULATION")).write.option(
            "header", True).csv('./question2Result')
