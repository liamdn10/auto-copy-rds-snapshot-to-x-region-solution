# auto-copy-rds-snapshot-to-x-region-solution
solution for automated copy AWS RDS instance and cluster snapshot to X region (in a different region)

## How solution work
<p align="center"><img src="https://user-images.githubusercontent.com/38157237/86114574-ebdae800-baf4-11ea-8621-6668be9dc0ba.png"/></p>

#### Step 1: Get RDS instance/cluster event
AWS Cloudwatch event provide both RDS instance snapshot event and RDS cluster snapshot event. The json bellow is an example of RDS instance automated snapshot creation. We can get the snapshot ARN and make a copy to the X region.
```json
{
  "version": "0",
  "id": "844e2571-85d4-695f-b930-0153b71dcb42",
  "detail-type": "RDS DB Snapshot Event",
  "source": "aws.rds",
  "account": "123456789012",
  "time": "2018-10-06T12:26:13Z",
  "region": "us-east-1",
  "resources": [
    "arn:aws:rds:us-east-1:123456789012:db:mysql-instance-2018-10-06-12-24"
  ],
  "detail": {
    "EventCategories": [
      "creation"
    ],
    "SourceType": "SNAPSHOT",
    "SourceArn": "arn:aws:rds:us-east-1:123456789012:db:mysql-instance-2018-10-06-12-24",
    "Date": "2018-10-06T12:26:13.882Z",
    "SourceIdentifier": "rds:mysql-instance-2018-10-06-12-24",
    "Message": "Automated snapshot created"
  }
}
```

#### Step 2: On creation
AWS boto3 provide two function to make a copy of snapshot, ```copy_db_snapshot()``` for RDS instance snapshot and ```copy_db_cluster_snapshot()``` for aurora cluster snapshot. Get the **detail-type** field to determine which boto3 function will be used (the value is *RDS DB Snapshot Event* or *RDS DB Cluster Snapshot Event*).
After copy snapshot to X region, clean up the old automated snapshot. You can let a number of automated snapshot in the cloudformation template.

#### Step 3: On manual deletion
On manual deletion event, we provide an option that remove or not the copy version in the target region. If the value of **AutomatedDeleteManualSnapshot** is *no*, the copies will not be deleted.

## Deploy the solution
#### Prerequisites
- Create a KMS Key in the target region (X region)
- Your account should have enough permissions to create resources
#### Resources which are created by CloudFormation template
- 1 event rule
- 1 lambda invoke permission
- 1 lambda function
- 1 role for lambda function
- 1 policy for lambba invoke role
- 1 SNS Topic
#### Parameters

| Parameter | Default | Description |
|---|---|---|
|ProjectPrefix||Project prefix for creating resource name like ${ProjectPrefix}-resourceName|
|State|ENABLED|State of solution ENALBLED/DISABLED|
|ApplyFor|RDS-Instance|This is for fun :D|
|EnableCustomEncryptKeyForRdsInstanceSnapshot|no|Create Key to encrypt RDS instance snapshot. (If source DB instance volume unencrypted)|
|KmsEncryptKeyArn||Key arn of KMS encrypt key in the target region|
|AutomatedDeleteManualSnapshot|yes|Delete copy of backup snapshot or not when a manual snapshot deleted in source region|
|MaximumOfCopiesOfAutomatedSnapshot|7|maximum of copies version of automated snapshot in source region, select 0 to nolimit versions of copies|
|RdsClusters||instance that apply solution. If more than one instance, split by ",". For example, instance1,instance2,instace3. Or let blank if there is no cluster to apply|
|RdsInstances||instance that apply solution. If more than one instance, split by ",". For example, instance1,instance2,instace3. Or let blank if there is no instance to apply|
|TargetRegion||Target region (Region X) that snapshot make a copy version to|

#### Note

- Select the different target region to the source region.
- Run the cloudformation template in the source region
