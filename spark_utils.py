import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark import SparkConf
from pyspark.sql import SparkSession
def initialise_spark():
    conf = {
        "setMaster":"local[*]",
        # "spark.executor.heartbeatInterval":10000,
        # "spark.network.timeout":10000,
        "spark.core.connection.ack.wait.timeout":3600,
        # "spark.driver.memory":"4g",
        # "spark.executor.memory":"4g",
        # "spark.driver.cores":1,
        # "spark.executor.cores":1,
        # "spark.executor.instances":2,
        # "spark.driver.instances":1
        }
    spark = SparkSession.builder.appName("learning").getOrCreate()
    for key, value in conf.items():
        spark.conf.set(key,value)
    return spark