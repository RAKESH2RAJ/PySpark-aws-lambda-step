import boto3
from os.path import join


def lambda_handler(event, context):
    emr = boto3.client('emr', region_name="eu-west-1")
    version = 'v1.0.0'
    bucket = 'pysparkbuck1234'
    main_path = join('s3://', bucket, '/pyspark_job', version, 'main.py')
    modules_path = join('s3://', bucket, '/pyspark_job', version, 'pyspark_job.zip')

    print("Message recieved from step function")
    print(event)

    job_parameters = {
        "job_name": "employee_details",
        "input_path": "s3a://pysparkbuck1234/pyspark_job/source",
        "output_path": "s3a://pysparkbuck1234/pyspark_job/target",
        "spark_config": {
            "--executor-memory": "1G",
            "--driver-memory": "1G"
        }
    }

    print(job_parameters)

    step_args = [
        "/usr/bin/spark-submit",
        '--py-files', modules_path,
        main_path, str(job_parameters)
    ]

    step = {
        "Name": job_parameters['job_name'],
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 's3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': step_args
        }
    }

    action = emr.add_job_flow_steps(JobFlowId=event['ClusterId'], Steps=[step])
    return action
