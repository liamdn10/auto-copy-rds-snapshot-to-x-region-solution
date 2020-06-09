import json
import boto3
import os
import sys
import sns_client


class RdsInstance:
    def __init__(self, src_region):
        self.AUTOMATED_DELETE_MANUAL_SNAPSHOT = os.environ['automated_delete_manual_snapshot']
        self.AUTOMATED_SNAPSHOT_MAXIMUM_COPIES = os.environ['automated_snapshot_maximum_copies']
        self.DEST_REGION = os.environ['dest_region']
        self.RDS_INSTANCES = os.environ['rds_instances'].split(',')
        self.KMS_KEY_ID = os.environ['kms_key_id']
        self.ENCRYPT_RDS_INSTANCE_SNAPSHOT = os.environ['encrypt_rds_instance_snapshot']
        self.__sns_client = sns_client.SnsClient()
        
        try:
            self.__rds_src_client = boto3.client('rds', region_name=src_region)
            self.__rds_tar_client = boto3.client('rds', self.DEST_REGION)
        except Exception as e:
            print("ERROR: failed to connect to RDS")
            # print(e)
            self.__sns_client.error_notification(e)
            sys.exit(1)
            
    def test_function(self, db_instance_identifier):
        self.__clean_copies_of_automated_snapshot(db_instance_identifier)
            
    def copy_instance_snapshot(self, event):
        source_snapshot_info = self.__get_source_snapshot_info(event)
        
        if source_snapshot_info['message'] == 'Creating automated snapshot' or source_snapshot_info['message'] == 'Creating manual snapshot':
            # print('snapshot is not in available state')
            sys.exit(0)
        
        # break if source instance is not in rds_instances list
        if source_snapshot_info['db_instance_identifier'] not in self.RDS_INSTANCES:
            # print('instance ' + source_snapshot_info['db_instance_identifier'] + ' is not in rds_instances list')
            return{
                'statusCode': 200,
                'body': json.dumps('instance is not in rds_instances list')
            }
        
        # copy snapshot to target region
        target_snapshot_identifer = source_snapshot_info['source_snapshot_identifier'].replace(":","-") + '-autocopied'
        try:
            if source_snapshot_info['is_encrypted'] == False and self.ENCRYPT_RDS_INSTANCE_SNAPSHOT == 'no':
                self.__rds_tar_client.copy_db_snapshot(
                    SourceDBSnapshotIdentifier = source_snapshot_info['source_snapshot_arn'],
                    TargetDBSnapshotIdentifier = target_snapshot_identifer,
                    Tags = [
                        {
                            'Key': 'Source-Snapshot',
                            'Value': source_snapshot_info['source_snapshot_arn']
                        },
                        {
                            'Key': 'Source-Snapshot-Type',
                            'Value': source_snapshot_info['source_snapshot_type']
                        }
                    ],
                    CopyTags = True,
                    SourceRegion = event['region']
                )
            else:
                self.__rds_tar_client.copy_db_snapshot(
                    SourceDBSnapshotIdentifier = source_snapshot_info['source_snapshot_arn'],
                    TargetDBSnapshotIdentifier = target_snapshot_identifer,
                    Tags = [
                        {
                            'Key': 'Source-Snapshot',
                            'Value': source_snapshot_info['source_snapshot_arn']
                        },
                        {
                            'Key': 'Source-Snapshot-Type',
                            'Value': source_snapshot_info['source_snapshot_type']
                        }
                    ],
                    CopyTags = True,
                    SourceRegion = event['region'],
                    KmsKeyId = self.KMS_KEY_ID
                )
                
        except Exception as e:
            # print("ERROR: RDS Snapshot is not in available state")
            # print(e)
            self.__sns_client.error_notification(e)
            sys.exit(1)
        
        if source_snapshot_info['source_snapshot_type'] == 'Automated':    
            self.__clean_copies_of_automated_snapshot(source_snapshot_info['db_instance_identifier'])
        else:
            print("Manual snapshot: Skip clean copies of automated snapshot")
    
    def delete_instance_snapshot(self, event):
        source_snapshot_info = self.__get_source_snapshot_info(event)
        target_snapshot_identifer = source_snapshot_info['source_snapshot_identifier'].replace(":","-") + '-autocopied'
        
        if source_snapshot_info['source_snapshot_type'] == 'Other':
            # print('skip delete other snapshot')
            sys.exit(0)
        
        if self.AUTOMATED_DELETE_MANUAL_SNAPSHOT == 'no':
            if source_snapshot_info['source_snapshot_type'] == 'Manual':
                # print('skip delete manual snapshot')
                sys.exit(0)
                
        try:
            self.__rds_tar_client.delete_db_snapshot(
                DBSnapshotIdentifier = target_snapshot_identifer
            )
        except self.__rds_tar_client.exceptions.DBSnapshotNotFoundFault:
            import rds_cluster
            rdsi = rds_cluster.RdsCluster(event['region'])
            rdsi.delete_cluster_snapshot(event)
        except Exception as e:
            # print("ERROR: failed to delete backup snapshot")
            # print(e)
            self.__sns_client.error_notification(e)
            sys.exit(1)
            
    def __clean_copies_of_automated_snapshot(self, db_instance_identifier):
        if self.AUTOMATED_SNAPSHOT_MAXIMUM_COPIES == '0':
            # print('skip clean snapshots')
            return{
                'statusCode': 200,
                'body': json.dumps('skip clean snapshots')
            }
        else:
            # print('cleaning rds instance snapshots')
            automated_snapshots = self.__get_automated_copies_of_snapshots(db_instance_identifier)
            # for i in automated_snapshots:
            #     print(i)
            automated_snapshot_number = len(automated_snapshots)
            if automated_snapshot_number >= int(self.AUTOMATED_SNAPSHOT_MAXIMUM_COPIES):
                for i in range((int(self.AUTOMATED_SNAPSHOT_MAXIMUM_COPIES)-1), automated_snapshot_number):
                    try:
                        # print(automated_snapshots[i]['target_snapshot_identifer'])
                        self.__rds_tar_client.delete_db_snapshot(
                            DBSnapshotIdentifier = automated_snapshots[i]['target_snapshot_identifer']
                        )
                    except Exception as e:
                        # print("ERROR: failed to delete backup snapshot")
                        # print(e)
                        self.__sns_client.error_notification(e)
                        sys.exit(1)
        
    def __get_automated_copies_of_snapshots(self, db_instance_identifier):
        target_snapshots = []
        res = self.__rds_tar_client.describe_db_snapshots(
            DBInstanceIdentifier = db_instance_identifier
        )['DBSnapshots']
        
        for target_snapshot in res:
            target_tags = self.__rds_tar_client.list_tags_for_resource(
                ResourceName = target_snapshot['DBSnapshotArn']
            )['TagList']
            
            if "SnapshotCreateTime" not in target_snapshot:
                continue
            else:
                for tag in target_tags:
                    if tag['Key'] == 'Source-Snapshot-Type' and tag['Value'] == 'Automated':
                        target_snapshots.append(
                            {
                                'target_snapshot_identifer': target_snapshot['DBSnapshotIdentifier'],
                                'target_snapshot_created_time': target_snapshot['SnapshotCreateTime']
                            }
                        )
                        break
                    else:
                        continue
                
        target_snapshots.sort(key=lambda x: x.get('target_snapshot_created_time'), reverse=True)

        return target_snapshots
    
    def __get_rds_instance_info(self, source_snapshot_identifier, region):
        try:
            res = self.__rds_src_client.describe_db_snapshots(
                DBSnapshotIdentifier = source_snapshot_identifier
            )
        
            return {
                'db_instance_identifier': res['DBSnapshots'][0]['DBInstanceIdentifier'],
                'is_encrypted': res['DBSnapshots'][0]['Encrypted']
            }
        except Exception as e:
            # print('Failed to get rds instance identifier')
            # print(e)
            self.__sns_client.error_notification(e)
            return {
                'db_instance_identifier': '',
                'is_encrypted': ''
            }
    
    def __get_source_snapshot_info(self, event):
        region = event['region']
        event_detail = event['detail']
        
        source_snapshot_arn = event_detail['SourceArn']
        source_snapshot_identifier = event_detail['SourceIdentifier']
        message = event_detail['Message']
        
        db_instance_identifier = ''
        is_encrypted = ''
        
        if message == 'Automated snapshot created':
            source_snapshot_type = 'Automated'
            rds_instance_info = self.__get_rds_instance_info(source_snapshot_identifier, region)
            db_instance_identifier = rds_instance_info['db_instance_identifier']
            is_encrypted = rds_instance_info['is_encrypted']
        elif message == 'Manual snapshot created':
            source_snapshot_type = 'Manual'
            rds_instance_info = self.__get_rds_instance_info(source_snapshot_identifier, region)
            db_instance_identifier = rds_instance_info['db_instance_identifier']
            is_encrypted = rds_instance_info['is_encrypted']
        elif message == 'Deleted automated snapshot':
            source_snapshot_type = 'Automated'
        elif message == 'Deleted manual snapshot':
            source_snapshot_type = 'Manual'
        else:
            source_snapshot_type = 'Other'
        
        event_categories = event_detail['EventCategories'][0]
        
        return (
            {
                'source_snapshot_arn': source_snapshot_arn,
                'source_snapshot_identifier': source_snapshot_identifier,
                'db_instance_identifier': db_instance_identifier,
                'message': message,
                'event_categories': event_categories,
                'source_snapshot_type': source_snapshot_type,
                'is_encrypted': is_encrypted
            }
        )
    
    