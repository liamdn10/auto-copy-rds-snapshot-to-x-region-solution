import json
import os
import rds_instance
import rds_cluster


def lambda_handler(event, context):
    print(event)
    # TODO implement
    event_category = event['detail']['EventCategories'][0]
    event_detail_type = event['detail-type']
    
    # rdsi = rds_instance.RdsInstance(event['region'])
    # rdsi.test_function('iot-data')
    # return(0)

    # event handler
    if event_category == 'creation':
        if event_detail_type == 'RDS DB Snapshot Event':
            rdsi = rds_instance.RdsInstance(event['region'])
            rdsi.copy_instance_snapshot(event)
        elif event_detail_type == 'RDS DB Cluster Snapshot Event':
            rdsi = rds_cluster.RdsCluster(event['region'])
            rdsi.copy_cluster_snapshot(event)
    elif event_category == 'backup':
        if event_detail_type == 'RDS DB Snapshot Event':
            rdsi = rds_instance.RdsInstance(event['region'])
            rdsi.copy_instance_snapshot(event)
        elif event_detail_type == 'RDS DB Cluster Snapshot Event':
            rdsi = rds_cluster.RdsCluster(event['region'])
            rdsi.copy_cluster_snapshot(event)
    elif event_category == 'deletion':
        rdsi = rds_instance.RdsInstance(event['region'])
        rdsi.delete_instance_snapshot(event)
    
    return {
        'statusCode': 200,
        'body': json.dumps('SUCESSED: Completed copied RDS snapshot to destination region')
    }
