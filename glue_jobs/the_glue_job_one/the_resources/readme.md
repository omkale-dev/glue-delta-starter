## Deploy to AWS
- Deploy the cloudformation template to create the glue job
    ```
    $ aws cloudformation deploy --template-file ./the_resources/job_template.yaml  --stack-name GlueJobOneStack --capabilities CAPABILITY_IAM --region us-east-1
    ```
    Note: Stack name needs to be unique in the AWS account. Use CamelCase job name as the stack name

- Copy the python script and the mapping file to the right folder in AWS.
    ```
    $ cd the_glue_job_one
    $ aws s3 cp ./src/job_one.py s3://aws-glue-assets-538296590642-us-east-1/scripts/the_glue_job_one/
    $ aws s3 cp ./the_resources/mapping.csv s3://aws-glue-assets-538296590642-us-east-1/scripts/the_glue_job_one/
    $ aws s3 cp ./the_resources/config.json s3://aws-glue-assets-538296590642-us-east-1/scripts/the_glue_job_one/
    ```
    Note: where s3://aws-glue-assets-538296590642-us-east-1/scripts/the_glue_job_one/ is the script location defined in the cloudformation template
