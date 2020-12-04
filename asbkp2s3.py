#!/usr/bin/env python3

import sys
import os
from datetime import datetime
import time
import boto3
from boto3.s3.transfer import TransferConfig
from boto3.s3.transfer import  S3Transfer
from botocore.errorfactory import ClientError
import hashlib
import pathlib
from config import SERVERS


def usage(app_name):
    print('Usage: {0} <host> <namespace> create'.format(app_name))


def mkdirs(*args):
    """ Create directory if not exists"""
    for arg in args:
        if not os.path.exists(arg):
            os.makedirs(arg)


def make_file_name(namespace, str_now):
    return '{namespace}_{now}.asbackup.gz'.format(
        namespace=namespace,
        now=str_now
    )


def make_cmd_string(host, namespace, setconfig, str_now):
    str_nice = ''
    if 'nice' in setconfig.keys():
        str_nice = '--nice {0}'.format(setconfig['nice'])

    str_cmd = 'asbackup -h {host} {nice} -n {namespace} -r -o - | gzip -1 > {local_path}/{filename}'.format(
        host=host,
        nice=str_nice,
        namespace=namespace,
        local_path=setconfig['local_path'],
        filename=make_file_name(namespace, str_now)
    )

    print('[DBG] {0}'.format(str_cmd))
    return str_cmd


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


def create_asbackup(host, namespace, setconfig, str_now):
    cmd = make_cmd_string(host, namespace, setconfig, str_now)
    result = os.system(cmd)
    if result != 0:
        print('[DBG] asbackup returned non zero code!')
        return False
    return True


def post_err(msg):
    # TODO: post to slack
    pass


def estimated_size_ok(filename, estimated_size):
    stat = os.stat(filename)
    print('[DBG] File size: {0}'.format(stat.st_size))
    print('[DBG] Estimated size: {0}'.format(estimated_size))
    if stat.st_size <= estimated_size:
        return False
    return True


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
        print('[DBG] head:')
        print(head)
        return head['ETag'][1:-1]
    except ClientError:
        pass
    return ''


def etag_checksum(filename):
    chunk_size=8 * 1024 * 1024
    md5s = []
    with open(filename, 'rb') as f:
        for data in iter(lambda: f.read(chunk_size), b''):
            md5s.append(hashlib.md5(data).digest())
    m = hashlib.md5(b''.join(md5s))
    return '{}-{}'.format(m.hexdigest(), len(md5s))


def md5sum(filename):
    return hashlib.md5(pathlib.Path(filename).read_bytes()).hexdigest()


def s3_md5_check(s3_client, s3_bucket, s3_file, local_file):
    s3_md5 = s3_etag(s3_client, s3_bucket, s3_file)
    print('[DBG] s3_md5sum: {0}'.format(s3_md5))
    local_md5 = etag_checksum(local_file)
    print('[DBG] local_md5sum: {0}'.format(local_md5))
    if s3_md5 != local_md5:
        return False
    return True


def now_as_string():
    return datetime.now().strftime('%Y%m%d-%H%M%S')


def main(args=sys.argv):
    str_now = now_as_string()
    if len(args) != 4 or args[3] not in ['create', 'list', 'get']:
        usage(args[0])
        post_err('wrong args')
        exit(1)

    host = args[1]
    namespace = args[2]
    action = args[3]

    try:
        setconfig = SERVERS[host][namespace]

        mkdirs(setconfig['local_path'])

        if action == 'create':
            print('[INF] Executing asbackup...')
            if not create_asbackup(host, namespace, setconfig, str_now):
                msg = '[ERR] Can not create asbackup file.'
                print(msg)
                post_err(msg)
                exit(4)

            filename = '{directory}/{filename}'.format(directory=setconfig['local_path'], filename=make_file_name(namespace, str_now))
            if not estimated_size_ok(filename, setconfig['estimated_size']):
                msg = '[ERR] Estimated file size is not OK!'
                print(msg)
                post_err(msg)
                exit(5)

            remote_filename = '{s3_path}/{filename}'.format(s3_path=setconfig['s3_path'], filename=make_file_name(namespace, str_now))
            print('[INF] Uploading {local_file} to s3://{bucket}/{remote_filename}...'.format(
                local_file=filename,
                bucket=setconfig['s3_bucket'],
                remote_filename=remote_filename
            ))

            start_time = time.time()
            s3_client = boto3.client('s3')
            s3_upload_file(s3_client, setconfig['s3_bucket'], filename, remote_filename)
            print('[INF] File successfully uploaded in {delta_time:4.2f} minutes!'.format(
                delta_time = (time.time() - start_time)/60)
            )

            if not s3_file_exists(s3_client, setconfig['s3_bucket'], remote_filename):
                msg = '[ERR] File does not exist on S3. Upload error!'
                post_err(msg)
                exit(6)
            print('[INF] s3 file does exist - OK!')

            if not s3_md5_check(s3_client, setconfig['s3_bucket'], remote_filename, filename):
                msg = '[ERR] local md5 != remote md5'
                post_err(msg)
                exit(7)
            print('[INF] s3 md5sum equal local md5sum!')

            if setconfig['remove_local']:
                print('[INF] Removing file {0}...'.format(filename))
                os.unlink(filename)
                print('[INF] Done!')

        elif action == 'list':
            pass
        elif action == 'get':
            pass

    except KeyError:
        print('[ERR] host ({0}) or set ({1}) is not present in configuration.'.format(args[1], args[2]))
        exit(2)
    except PermissionError as e:
        print('[ERR] Fatal error: {0}'.format(e))
        exit(3)


if __name__ == '__main__':
    main(sys.argv)