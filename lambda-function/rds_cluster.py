import json
import boto3
import os
import sys
import sns_client


class RdsCluster:
    def __init__(self, src_region):
        self.AUTOMATED_DELETE_MANUAL_SNAPSHOT = os.environ['automated_delete_manual_snapshot']
        self.AUTOMATED_SNAPSHOT_MAXIMUM_COPIES = os.environ['automated_snapshot_maximum_copies']
        self.DEST_REGION = os.environ['dest_region']
        self.RDS_CLUSTERS = os.environ['rds_clusters'].split(',')
        self.KMS_KEY_ID = os.environ['kms_key_id']

        self.__sns_client = sns_client.SnsClient()
        
        try:
            self.__rds_src_client = boto3.client('rds', region_name=src_region)
            self.__rds_tar_client = boto3.client('rds', self.DEST_REGION)
        except Exception as e:
            print("ERROR: failed to connect to RDS")
            # print(e)
            self.__sns_client.error_notification(e)
            sys.exit(1)
            
    def test_function(self, db_cluster_identifier):
        self.__clean_copies_of_automated_snapshot(db_cluster_identifier)
            
    def copy_cluster_snapshot(self, event):
        source_snapshot_info = self.__get_source_snapshot_info(event)
        
        if source_snapshot_info['message'] == 'Creating automated cluster snapshot' or source_snapshot_info['message'] == 'Creating manual cluster snapshot':
            print('snapshot is not in available state')
            sys.exit(0)
        
        # break if source cluster is not in RDS_CLUSTERS list
        if source_snapshot_info['db_cluster_identifier'] not in self.RDS_CLUSTERS:
            print('cluster ' + source_snapshot_info['db_cluster_identifier'] + ' is not in RDS_CLUSTERS list')
            return{
                'statusCode': 200,
                'body': json.dumps('instance is not in RDS_CLUSTERS list')
            }
        
        # else, copy snapshot to target region
        target_snapshot_identifer = source_snapshot_info['source_snapshot_identifier'].replace(":","-") + '-autocopied'
        try:
            if source_snapshot_info['is_encrypted']:
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
            else:
                self.__rds_tar_client.copy_db_cluster_snapshot(
                    SourceDBClusterSnapshotIdentifier = source_snapshot_info['source_snapshot_arn'],
                    TargetDBClusterSnapshotIdentifier = target_snapshot_identifer,
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
        except Exception as e:
            # print("ERROR:")
            # print(e)
            self.__sns_client.error_notification(e)
            sys.exit(1)
        
        if source_snapshot_info['source_snapshot_type'] == 'Automated':    
            self.__clean_copies_of_automated_snapshot(source_snapshot_info['db_cluster_identifier'])
        else:
            print("Manual snapshot: Skip clean copies of automated snapshot")
    
    def delete_cluster_snapshot(self, event):
        source_snapshot_info = self.__get_source_snapshot_info(event)
        target_snapshot_identifer = source_snapshot_info['source_snapshot_identifier'].replace(":","-") + '-autocopied'
        
        if source_snapshot_info['source_snapshot_type'] == 'Other':
            print('skip delete other snapshot')
            return{
                'statusCode': 200,
                'body': json.dumps('skip manual snapshot')
            }
            sys.exit(0)
        
        if self.AUTOMATED_DELETE_MANUAL_SNAPSHOT == 'no':
            if source_snapshot_info['source_snapshot_type'] == 'Manual':
                print('skip delete manual snapshot')
                return{
                    'statusCode': 200,
                    'body': json.dumps('skip manual snapshot')
                }
                sys.exit(0)
                
        try:
            self.__rds_tar_client.delete_db_cluster_snapshot(
                DBClusterSnapshotIdentifier = target_snapshot_identifer
            )
        except Exception as e:
            # print("ERROR: failed to delete backup snapshot")
            # print(e)
            self.__sns_client.error_notification(e)
            sys.exit(1)
            
    def __clean_copies_of_automated_snapshot(self, db_cluster_identifier):
        if self.AUTOMATED_SNAPSHOT_MAXIMUM_COPIES == '0':
            print('skip clean snapshots')
            return{
                'statusCode': 200,
                'body': json.dumps('skip clean snapshots')
            }
        else:
            print('cleaning rds cluster snapshots')
            automated_snapshots = self.__get_automated_copies_of_snapshots(db_cluster_identifier)
            for i in automated_snapshots:
                print(i)
            automated_snapshot_number = len(automated_snapshots)
            if automated_snapshot_number >= int(self.AUTOMATED_SNAPSHOT_MAXIMUM_COPIES):
                for i in range((int(self.AUTOMATED_SNAPSHOT_MAXIMUM_COPIES)-1), automated_snapshot_number):
                    try:
                        print(automated_snapshots[i]['target_snapshot_identifer'])
                        self.__rds_tar_client.delete_db_cluster_snapshot(
                            DBClusterSnapshotIdentifier = automated_snapshots[i]['target_snapshot_identifer']
                        )
                    except Exception as e:
                        # print("ERROR: failed to delete backup snapshot")
                        # print(e)
                        self.__sns_client.error_notification(e)
                        sys.exit(1)
        
    def __get_automated_copies_of_snapshots(self, db_cluster_identifier):
        target_snapshots = []
        # try?
        res = self.__rds_tar_client.describe_db_cluster_snapshots(
            DBClusterIdentifier = db_cluster_identifier
        )['DBClusterSnapshots']
        
        for target_snapshot in res:
            target_tags = self.__rds_tar_client.list_tags_for_resource(
                ResourceName = target_snapshot['DBClusterSnapshotArn']
            )['TagList']
            
            if "SnapshotCreateTime" not in target_snapshot:
                continue
            else:
                for tag in target_tags:
                    if tag['Key'] == 'Source-Snapshot-Type' and tag['Value'] == 'Automated':
                        target_snapshots.append(
                            {
                                'target_snapshot_identifer': target_snapshot['DBClusterSnapshotIdentifier'],
                                'target_snapshot_created_time': target_snapshot['SnapshotCreateTime']
                            }
                        )
                        break
                    else:
                        continue
                
        target_snapshots.sort(key=lambda x: x.get('target_snapshot_created_time'), reverse=True)

        return target_snapshots
    
    def __get_rds_cluster_identifier(self, source_snapshot_identifier, region):
        try:
            res = self.__rds_src_client.describe_db_cluster_snapshots(
                DBClusterSnapshotIdentifier = source_snapshot_identifier
            )
        
            return {
                'db_cluster_identifier': res['DBClusterSnapshots'][0]['DBClusterIdentifier'],
                'is_encrypted': res['DBClusterSnapshots'][0]['StorageEncrypted']
            }
        except Exception as e:
            # print('Failed to get rds cluster identifier')
            # print(e)
            self.__sns_client.error_notification(e)
            return {
                'db_cluster_identifier': '',
                'is_encrypted': ''
            }
    
    def __get_source_snapshot_info(self, event):
        region = event['region']
        event_detail = event['detail']
        
        source_snapshot_arn = event_detail['SourceArn']
        source_snapshot_identifier = event_detail['SourceIdentifier']
        message = event_detail['Message']
        
        db_cluster_identifier = ''
        is_encrypted = ''
        
        if message == 'Automated cluster snapshot created':
            source_snapshot_type = 'Automated'
            db_cluster_identifier = self.__get_rds_cluster_identifier(source_snapshot_identifier, region)
            is_encrypted = ''
        elif message == 'Manual cluster snapshot created':
            source_snapshot_type = 'Manual'
            db_cluster_identifier = self.__get_rds_cluster_identifier(source_snapshot_identifier, region)
            is_encrypted = ''
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
                'db_cluster_identifier': db_cluster_identifier,
                'message': message,
                'event_categories': event_categories,
                'source_snapshot_type': source_snapshot_type,
                'is_encrypted': is_encrypted
            }
        )
    
    