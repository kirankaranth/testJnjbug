from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from badpipeline.config.ConfigStore import *
from badpipeline.udfs.UDFs import *
from prophecy.utils import *
from badpipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/badPipeline")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/badPipeline")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
