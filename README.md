# pyspark-aws-lambda-step
Running PySpark job using AWS Lambda and Step Function

Reference: https://towardsdatascience.com/zipping-and-submitting-pyspark-jobs-in-emr-through-lambda-functions-46a58a496d9e

>>EMR bootstrap script for your PySpark application
https://towardsdatascience.com/pex-the-secret-sauce-for-the-perfect-pyspark-deployment-of-aws-emr-workloads-9aef0d8fa3a5

PEX (Python EXecutable) is a file format and associated tools to create a general purpose Python environment virtualization solution similar to virtualenv. PEX was originally developed at Twitter in 2011 to deploy Python applications to production
PEX files are self-contained executable Python virtual environments.

#-------------------------------------------
# Lambda Functions : Roles
#-------------------------------------------
AmazonElasticMapReduceFullAccess
AWSLambdaBasicExecutionRole

#-------------------------------------------
# Step Functions> State machines : Roles
#-------------------------------------------
AmazonElasticMapReduceFullAccess
AWSLambdaRole

#-------------------------------------------
# Artifacts on S3 bucket
#-------------------------------------------
store artifacts in s3
   1. packed/zipped module "pyspark_etl"
   2. main.py : outside zip file imports pyspark_etl.zip file as a module. You do not have to specify .zip when you import the module

#------------------------------------------------
# Options 'ActionOnFailure': 'TERMINATE_CLUSTER'
#------------------------------------------------

Steps=[
        {
            'Name': 'spark-submit',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 's3://sparkuser/jar/example.jar',
                'Args': [
                	'spark-submit','--executor-memory', '1g', '--driver-memory', '500mb',
                	'--class', 'spark.exercise_1'

                ]
            }
        }
    ]

#-------------------------------------------
# Input for state function
#-------------------------------------------

{
  "CreateCluster": true,
  "TerminateCluster": true,
  "ClusterName": "WorkflowPysparkCluster"
}

