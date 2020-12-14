import os
import sys
from datetime import datetime, timedelta
import time
from boto3.s3.transfer import TransferConfig
from boto3.s3.transfer import S3Transfer
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
        multipart_threshold=1024 * 25,
        max_concurrency=10,
        multipart_chunksize=1024 * 25,
        use_threads=True
    )
    transfer = S3Transfer(s3_client, config)
    transfer.upload_file(local_filename, s3_bucket, remote_filename, callback=print_progress)


def s3_download_file(s3_client, s3_bucket, local_filename, remote_filename):
    s3keys = [s3key for s3key in s3_list_files(s3_client, s3_bucket, remote_filename)]
    if len(s3keys) < 1:
        raise ClientError({'Message': 'S3 Key not found: {0}'.format(remote_filename)})

    print_progress = make_progress(s3keys[0].size, '[INF] File downloaded for')

    config = TransferConfig(
        multipart_threshold=1024 * 25,
        max_concurrency=10,
        multipart_chunksize=1024 * 25,
        use_threads=True
    )
    transfer = S3Transfer(s3_client, config)
    transfer.download_file(s3_bucket, remote_filename, local_filename, callback=print_progress)


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
            yield S3Key(content['Key'], content['LastModified'], content['ETag'], content['Size'],
                        content['StorageClass'])


def s3_list_files(s3_client, s3_bucket, prefix):
    return keys(s3_client, s3_bucket, prefix=prefix)


def s3keys_total_size(lst_s3keys):
    total = 0
    for s3key in lst_s3keys:
        total += s3key.size
    return total


class S3Key(object):
    def __init__(self, key, last_modified, etag, size, storage_class):
        self.key = key
        self.last_modified = last_modified
        self.etag = etag
        self.size = size
        self.storage_class = storage_class

    def __str__(self):
        return '{key}\t{size:4.4f} MBytes\t{last_modified}'.format(
            key=self.key,
            size=self.size / 1024 / 1024,
            last_modified=str(self.last_modified)
        )

    def __gt__(self, other):
        return self.last_modified.timestamp() > other.last_modified.timestamp()


def s3key2delete(s3key: S3Key, months, days_set):
    now = datetime.now()
    delta = now - s3key.last_modified
    if delta.days >= 7 and s3key.last_modified.day not in days_set:
        return True

    if delta.days >= 30 * months:
        return True

    return False


def test_s3key2delete_new_file():
    s3key = S3Key(key='test', last_modified=datetime.now(), etag='zzz', size=0, storage_class='')
    assert s3key2delete(s3key, months=1, days_set=[1, 15, 30]) == False


def test_s3key2delete_week_older_file():
    last_modified = (datetime.now() - timedelta(days=7))
    s3key = S3Key(key='test', last_modified=last_modified, etag='zzz', size=0, storage_class='')
    assert s3key2delete(s3key, months=1, days_set=[1, 15, 30]) == True


def test_s3key2delete_older_then_week_file():
    last_modified = (datetime.now() - timedelta(days=8))
    s3key = S3Key(key='test', last_modified=last_modified, etag='zzz', size=0, storage_class='')
    assert s3key2delete(s3key, months=1, days_set=[1, 15, 30]) == True


def test_s3key2delete_older_then_week_but_in_days_file():
    last_modified = (datetime.now() - timedelta(days=60))

    last_modified = last_modified.replace(day=15)
    s3key = S3Key(key='test', last_modified=last_modified, etag='zzz', size=0, storage_class='')
    assert s3key2delete(s3key, months=10, days_set=[1, 15, 30]) == False
    assert s3key2delete(s3key, months=1, days_set=[1, 15, 30]) == True

    last_modified = last_modified.replace(day=12)
    s3key = S3Key(key='test', last_modified=last_modified, etag='zzz', size=0, storage_class='')
    assert s3key2delete(s3key, months=10, days_set=[1, 15, 30]) == True
    assert s3key2delete(s3key, months=1, days_set=[1, 15, 30]) == True
