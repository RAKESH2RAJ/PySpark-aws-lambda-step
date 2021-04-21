import ast
import sys
import os
from pyspark_job.run import run
import json

if __name__ == '__main__':
    str_parameters = sys.argv[1]

    # Local test

    # job_parameters = {
    #     "job_name": "job_raw_to_lake",
    #     "input_path": "s3a://raw-input-layer/source1",
    #     "output_path": "s3a://lake-data-layer/source1",
    #     "spark_config": {
    #         "--executor-memory": "1G",
    #         "--driver-memory": "1G"
    #     }
    # }
    #
    # str_parameters = json.dumps(job_parameters)
    parameters = ast.literal_eval(str_parameters)

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    run(parameters)
