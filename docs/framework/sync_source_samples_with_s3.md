## syncing source_samples with an s3 location

python -m pip install --user awscli

py -m awscli --version

aws configure

AWS Access Key ID [None]: minioadmin
AWS Secret Access Key [None]: minioadmin123
Default region name [None]: us-east-1
Default output format [None]: json


aws --endpoint-url http://localhost:9000 s3 sync "C:\github\bronze-foundry\sampledata\source_samples" s3://mdf/source_samples

aws --endpoint-url http://localhost:9000 s3 sync "C:\github\bronze-foundry\sampledata\source_samples" s3://mdf/source_samples --dryrun
