import boto3
from botocore.exceptions import ClientError
import config
import pickle
import tempfile
from io import BytesIO
import time
from utils import log


log.info('initializing aws...')
aws_session = boto3.session.Session(config.AWS_ACCESS_KEY, 
                                    config.AWS_SECRET_KEY,
                                    region_name=config.AWS_REGION)
ec2 = aws_session.resource('ec2')
ec2client = aws_session.client('ec2')
vpc = ec2.Vpc(config.AWS_VPC)
s3 = aws_session.resource('s3')
log.info('done')


def wait_ssh(instance):
    log.info('waiting for instance {} running...'.format(instance.id))
    instance.wait_until_running()
    while True:
        statuses = ec2client.describe_instance_status(InstanceIds=[instance.id])
        status = statuses['InstanceStatuses'][0]
        if status['InstanceStatus']['Status'] == 'ok' \
            and status['SystemStatus']['Status'] == 'ok':
            break
        log.info('waiting for ssh ready on inshtace {}...'.format(instance.id))
        time.sleep(5)
    log.info("Instance {} is running, ready to ssh to it".format(instance.id))

def get_instance(instance_type):
    instance=ec2.create_instances(InstanceType=instance_type,
                                  MinCount=1,
                                  MaxCount=1,
                                  ImageId=config.AWS_IMAGE_ID,
                                  SubnetId=config.AWS_SUBNET_ID, 
                                  KeyName=config.AWS_KEY_NAME)[0]
    wait_ssh(instance)
    instance = ec2.Instance(instance.id)
   
    
   
    
    return instance

def get_instances(instance_type, n):
    result = []
    instances = ec2.create_instances(InstanceType=instance_type,
                                  MinCount=n,
                                  MaxCount=n,
                                  ImageId=config.AWS_IMAGE_ID,
                                  SubnetId=config.AWS_SUBNET_ID, 
                                  KeyName=config.AWS_KEY_NAME)
    for instance in instances:
        wait_ssh(instance)
        result.append(ec2.Instance(instance.id))
    return result



def object_to_s3(obj, bucket_name, key):
    data = pickle.dumps(obj)
    fileobj = BytesIO(data)
    bucket = s3.Bucket(bucket_name)
    result = bucket.upload_fileobj(fileobj, key)

def bytes_to_s3(data, bucket_name, key):
    fileobj = BytesIO(data)
    bucket = s3.Bucket(bucket_name)
    result = bucket.upload_fileobj(fileobj, key)


def object_from_s3(bucket_name, key):
    try:
        data = bytes_from_s3(bucket_name, key)
    except ClientError:
        return None
    return pickle.loads(data)

def bytes_from_s3(bucket_name, key):
    fileobj = BytesIO(b'')
    bucket = s3.Bucket(bucket_name)
    obj = bucket.Object(key)
    obj.download_fileobj(fileobj)
    fileobj.seek(0)
    return fileobj.read()
