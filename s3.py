import os
import sys
from boto3.s3.transfer import TransferConfig
from boto3.s3.transfer import  S3Transfer
from botocore.errorfactory import ClientError

from etag import possible_etags


def make_progress(file_size, prefix_msg):
    processed = 0
    count = 0

    def progress(chunk):
        nonlocal processed
        processed += chunk
        nonlocal count
        count += 1

        done_pct = processed / (file_size * 0.01)
        if count % 20 == 0:
            sys.stdout.write('{0} {1:3.2f}%\n'.format(prefix_msg, done_pct))
            sys.stdout.flush()

    return progress


def s3_upload_file(s3_client, s3_bucket, local_filename, remote_filename):
    print_progress = make_progress(os.stat(local_filename).st_size, '[INF] File uploaded for')

    config = TransferConfig(
        multipart_threshold=1024*25,
        max_concurrency=10,
        multipart_chunksize=1024*25,
        use_threads=True
    )
    transfer = S3Transfer(s3_client, config)
    transfer.upload_file(local_filename, s3_bucket, remote_filename, callback=print_progress)

def s3_file_exists(s3_client, s3_bucket, filename):
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=filename)
    except ClientError:
        return False
    return True


def s3_etag(s3_client, s3_bucket, filename):
    try:
        head = s3_client.head_object(
            Bucket=s3_bucket,
            Key=filename
        )
        return head['ETag'][1:-1]
    except ClientError:
        pass
    return ''


def s3_md5_check(s3_client, s3_bucket, s3_file, local_file):
    s3_md5 = s3_etag(s3_client, s3_bucket, s3_file)
    print('[DBG] s3_etag: {0}'.format(s3_md5))
    num_parts = 0
    if len(s3_md5.split('-')) > 1:
        num_parts = int(s3_md5.split('-')[1])

    local_etags = possible_etags(local_file, num_parts)
    print('[DBG] local etags: {0}'.format(local_etags))
    if s3_md5 not in local_etags:
        return False

    return True


def keys(s3_client, bucket_name, prefix='/', delimiter='/', start_after=''):
    s3_paginator = s3_client.get_paginator('list_objects_v2')
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
    for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after):
        for content in page.get('Contents', ()):
            print('[DBG] {0}'.format(content))
            yield content['Key']


def s3_list_files(s3_client, s3_bucket, prefix):
    return keys(s3_client, s3_bucket, prefix=prefix)
