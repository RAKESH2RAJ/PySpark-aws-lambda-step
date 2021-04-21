import yaml
import os.path
from pyspark.sql.functions import explode, col


def process(spark, input_path, output_path, save_mode='overwrite'):
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_file_path = os.path.abspath(current_dir + "/../resources/application.yml")

    with open(app_config_file_path) as conf:
        app_conf = yaml.load(conf, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", app_conf["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_conf["s3_conf"]["secret_access_key"])
    hadoop_conf.set("fs.s3a.endpoint", app_conf["s3_conf"]["endpoint"])

    # hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    # hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # hadoop_conf.set("fs.s3a.access.key", 'AKIAZVZKYVLDOLL6K2SE')
    # hadoop_conf.set("fs.s3a.secret.key", '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')
    # hadoop_conf.set("fs.s3a.endpoint", 's3-eu-west-1.amazonaws.com')

    # read data
    company_df = spark.read\
        .json("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/company.json")

    company_df.printSchema()
    company_df.show(5, False)

    flattened_df = company_df.select(col("company"), explode(col("employees")).alias("employee"))
    flattened_df.show()

    flattened_df \
        .select(col("company"), col("employee.firstName").alias("emp_name")) \
        .write \
        .mode(save_mode) \
        .parquet(output_path)

    # processing
    pass

