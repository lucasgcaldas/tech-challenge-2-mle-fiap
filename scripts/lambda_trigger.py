import boto3

def lambda_handler(event, context):
    glue = boto3.client('glue')
    response = glue.start_job_run(JobName='Job-Glue-Ibov')
    print(f"Glue Job iniciado: {response['JobRunId']}")
    return response
