import boto3
import uuid
"""
Description :
     Creating two S3 buckets using boto3, with the credentials and confing files that are in .aws folder
     Then crating a txt file and uploading it to the first bucket,
     Downloading the file form the first bucket to current dir,
     Copying the first file from first bucket to the second one,
     Deleting the first file form both buckets,
     Uploading a second file with public read access grants,
     Creating a 3ed file with Server Side Encryption using AES-256 algorithm,
     Enabling versioning and creating two versions of the first file, 
     Printing all buckets name and attributes
     and deleting all of the buckets and its content
     
"""


def create_bucket_name(bucket_prefix):
    """
    Creating a unique bucket name from a prefix with uuid module

    :param bucket_prefix: the prefix of the name of the bucket
    :return: bucket name : an unique name for the bucket including the prefix
    """
    # The generated bucket name must be between 3 and 63 chars long
    return ''.join([bucket_prefix, str(uuid.uuid4())])


def create_bucket(bucket_prefix, s3_connection):
    """
    Creating an AWS S3 Bucket, by creating boto3 session object.
    The bucket region is taken from the config file,

    :param bucket_prefix: the prefix of the name of the bucket
    :param s3_connection: the s3 connection object - resource type
    :return: AWS S3 Bucket - bucket_name, bucket_response
    """
    session = boto3.session.Session()
    current_region = session.region_name
    bucket_name = create_bucket_name(bucket_prefix)
    bucket_response = s3_connection.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
        'LocationConstraint': current_region})
    print(bucket_name, current_region)
    return bucket_name, bucket_response


def create_temp_file(size, file_name, file_content):
    """
    Creating a temp file to upload to the bucket.

    :param size: the size of the new created file
    :param file_name: the name of the new created file
    :param file_content: the content in the new created file
    :return: New file name
    """
    random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
    with open(random_file_name, 'w') as f:
        f.write(str(file_content) * size)
    return random_file_name


def copy_to_bucket(bucket_from_name, bucket_to_name, file_name):
    """
    Copyinf a file form one bucket to another

    :param bucket_from_name: first bucket name
    :param bucket_to_name: second bucket name
    :param file_name: the file name
    :return: -
    """
    copy_source = {
        'Bucket': bucket_from_name,
        'Key': file_name
    }
    s3_resource.Object(bucket_to_name, file_name).copy(copy_source)


def enable_bucket_versioning(bucket_name):
    """
    Enable S3 bucket versioning for a bucket

    :param bucket_name: the bucket name to use the versioning
    :return: -
    """
    bkt_versioning = s3_resource.BucketVersioning(bucket_name)
    bkt_versioning.enable()
    print(bkt_versioning.status)


def delete_all_objects(bucket_name):
    """
    Deleting all object in a S3 bucket
    :param bucket_name: the name of the bucket whom hold the objects to delete
    :return: -
    """
    res = []
    bucket=s3_resource.Bucket(bucket_name)
    for obj_version in bucket.object_versions.all():
        res.append({'Key': obj_version.object_key,
                    'VersionId': obj_version.id})
    print(res)
    bucket.delete_objects(Delete={'Objects': res})


if __name__ == '__main__':
# Creating an S3 object by boto3
    s3_resource = boto3.resource('s3')
    s3_client = boto3.client('s3')

# creating the first bucket
    first_bucket_name, first_response = create_bucket(bucket_prefix='firstpythonbucket',
                                                      s3_connection=s3_resource.meta.client)
# creating the second bucket
    second_bucket_name, second_response = create_bucket(bucket_prefix='secondpythonbucket',
                                                        s3_connection=s3_resource)
# Creating a file name
    first_file_name = create_temp_file(300, 'firstfile.txt', 'f')
# adding the file to the buckets
    first_bucket = s3_resource.Bucket(name=first_bucket_name)
    second_bucket = s3_resource.Bucket(name=second_bucket_name)
    first_object = s3_resource.Object(
        bucket_name=first_bucket_name, key=first_file_name)
    second_object = s3_resource.Object(
        bucket_name=second_bucket_name, key=first_file_name)
# Direct access to the Object
    first_object_again = first_bucket.Object(first_file_name)
    second_object_again = second_bucket.Object(first_file_name)
# Uploading with the created object to the first bucket
    first_object.upload_file(first_file_name)
# Downloading the file from the first bucket
    s3_resource.Object(first_bucket_name, first_file_name).download_file(f'{first_file_name}')
# Copying the first file from first bucket to the second one
    copy_to_bucket(first_bucket_name, second_bucket_name, first_file_name)
# Deleting the from both of the objects
    s3_resource.Object(second_bucket_name, first_file_name).delete()
    s3_resource.Object(first_bucket_name, first_file_name).delete()
# Creating a second file with ACL : public-read option
    second_file_name = create_temp_file(400, 'secondfile.txt', 's')
    second_object = s3_resource.Object(first_bucket.name, second_file_name)
    second_object.upload_file(second_file_name, ExtraArgs={
                          'ACL': 'public-read'})
# Creating a ACL object and printing its grants attribute
    second_object_acl = second_object.Acl()
    print(second_object_acl.grants)
# Creating a 3ed file with Server Side Encryption using AES-256 algorithm
    third_file_name = create_temp_file(300, 'thirdfile.txt', 't')
    third_object = s3_resource.Object(first_bucket_name, third_file_name)
    third_object.upload_file(third_file_name, ExtraArgs={
                         'ServerSideEncryption': 'AES256'})
# Printing the file encryption type
    print(third_object.server_side_encryption)
# Reupload the third_object and set its storage class to Standard_IA
    third_object.upload_file(third_file_name, ExtraArgs={
                         'ServerSideEncryption': 'AES256',
                         'StorageClass': 'STANDARD_IA'})
    third_object.reload()
    print(third_object.storage_class)
# Enabling versioning for the first bucket
    enable_bucket_versioning(first_bucket_name)
# Uploading the first file
    s3_resource.Object(first_bucket_name, first_file_name).upload_file(
        first_file_name)
# Updating the first file that was uploaded with the content of the 3rd file
    s3_resource.Object(first_bucket_name, first_file_name).upload_file(
        third_file_name)
# Uploading the 2nd file
    s3_resource.Object(first_bucket_name, second_file_name).upload_file(
        second_file_name)
# retrieving the latest available version of the first file
    s3_resource.Object(first_bucket_name, first_file_name).version_id
# printing all the buckets name using Traversal
    for bucket in s3_resource.buckets.all():
        print(bucket.name)
# printing all buckets attributes
    for obj in first_bucket.objects.all():
        subsrc = obj.Object()
        print(obj.key, obj.storage_class, obj.last_modified, subsrc.version_id, subsrc.metadata)
# Deleting all the objects and the buckets
    for bucket in s3_resource.buckets.all():
        print(bucket.name)
        try:
            delete_all_objects(bucket.name)
        except Exception as e:
            print(e)
        try:
            s3_resource.Bucket(bucket.name).delete()
        except Exception as e:
            print(e)