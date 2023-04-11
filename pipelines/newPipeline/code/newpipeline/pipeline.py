from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from newpipeline.config.ConfigStore import *
from newpipeline.udfs.UDFs import *
from prophecy.utils import *
from newpipeline.graph import *

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/newPipeline")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/newPipeline")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
