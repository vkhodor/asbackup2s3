#!/usr/bin/env python3

import sys
import os
from datetime import datetime
import boto3
from boto3.s3.transfer import TransferConfig
from boto3.s3.transfer import  S3Transfer

from config import SERVERS


def usage(app_name):
    print('Usage: {0} <host> <namespace> create'.format(app_name))


def mkdirs(*args):
    """ Create directory if not exists"""
    for arg in args:
        if not os.path.exists(arg):
            os.makedirs(arg)


def make_cmd_string(host, namespace, setconfig, str_now):
    str_nice = ''
    if 'nice' in setconfig.keys():
        str_nice = '--nice {0}'.format(setconfig['nice'])

    str_cmd = 'asbackup -h {host} {nice} -n {namespace} -r -o {local_path}/{namespace}_{now}.asbackup &> {log_directory}/{namespace}_{now}.log'.format(
        host=host,
        nice=str_nice,
        namespace=namespace,
        local_path=setconfig['local_path'],
        now=str_now,
        log_directory=setconfig['log_directory']
    )

    print('[DBG] {0}'.format(str_cmd))
    return str_cmd

def s3_upload_file(s3_bucket, local_filename, remote_filename):
    s3_client = boto3.client('s3')
    stat = os.stat(local_filename)
    total_length = stat.st_size
    downloaded = 0

    def progress(chunk):
        nonlocal downloaded
        downloaded += chunk
        done = int(50 * downloaded / total_length)
        sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50 - done)))
        sys.stdout.flush()

    config = TransferConfig(
        multipart_threshold=1024*25,
        max_concurrency=10,
        multipart_chunksize=1024*25,
        use_threads=True
    )
    transfer = S3Transfer(s3_client, config)
    transfer.upload_file(local_filename, s3_bucket, remote_filename, callback=progress)


def create_asbackup(host, namespace, setconfig, str_now):
    cmd = make_cmd_string(host, namespace, setconfig, str_now)
    result = os.system(cmd)
    if result != 0:
        print('[DBG] asbackup returned non zero code!')
        return False
    return True

def now_as_string():
    return datetime.now().strftime('%Y%m%d-%H%M%S')


def main(args=sys.argv):
    str_now = now_as_string()
    if len(args) != 4 or args[3] not in ['create', 'list', 'get']:
        usage(args[0])
        exit(1)

    host = args[1]
    namespace = args[2]
    action = args[3]

    try:
        setconfig = SERVERS[host][namespace]

        mkdirs(setconfig['local_path'], setconfig['log_directory'])

        if action == 'create':
            if not create_asbackup(host, namespace, setconfig, str_now):
                print('[ERR] Can not create asbackup file.')
                exit(4)
            filename = '{0}/{1}_{2}.asbackup'.format(setconfig['local_path'], namespace, str_now)
            remote_filename = '{0}/{1}_{2}.asbackup'.format(setconfig['s3_path'], namespace, str_now)
            s3_upload_file(setconfig['s3_bucket'], filename, remote_filename)
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