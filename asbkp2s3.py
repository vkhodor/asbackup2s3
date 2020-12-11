#!/usr/bin/env python3

import requests
import json
import boto3

from config import SERVERS, SLACK

from s3 import *


def usage(app_name):
    print('Usage: {0} <host> <namespace> create|list|get <s3_key> <filename>'.format(app_name))
    print('Example: {0} 172.31.31.11 userdata get us/userdata_201212_101010.asbackup.gz ./us_latest.asbackup.gz'.format(app_name))


def mkdirs(*args):
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


def create_asbackup(host, namespace, setconfig, str_now):
    cmd = make_cmd_string(host, namespace, setconfig, str_now)
    result = os.system(cmd)
    if result != 0:
        print('[DBG] asbackup returned non zero code!')
        return False
    return True


def post_msg_to_slack(msg, url, username, channel):
    return requests.post(
        url,
        json.dumps(
            {
                'channel': channel,
                'text': msg,
                'username': username
            }
        ),
        headers={'content-type': 'application/json'}
    )


def estimated_min_size_ok(file_size, estimated_min_size):
    print('[DBG] File size: {0}'.format(file_size))
    print('[DBG] Estimated MIN size: {0}'.format(estimated_min_size))
    if file_size <= estimated_min_size:
        return False
    return True


def estimated_max_size_ok(file_size, estimated_max_size):
    print('[DBG] File size: {0}'.format(file_size))
    print('[DBG] Estimated MAX size: {0}'.format(estimated_max_size))
    if file_size > estimated_max_size:
        return False
    return True


def now_as_string():
    return datetime.now().strftime('%Y%m%d-%H%M%S')


def main(args=None):
    if args is None:
        args = sys.argv

    str_now = now_as_string()
    if len(args) < 4 or args[3] not in ['create', 'list', 'get']:
        usage(args[0])
        post_msg_to_slack('wrong args {0}'.format(args), url=SLACK['url'], username=SLACK['username'], channel=SLACK['channel'])
        exit(1)

    host = args[1]
    namespace = args[2]
    action = args[3]

    s3_client = boto3.client('s3')

    try:
        setconfig = SERVERS[host][namespace]

        mkdirs(setconfig['local_path'])

        if action == 'create':
            start_bkp_time = time.time()

            print('[INF] Executing asbackup...')
            if not create_asbackup(host, namespace, setconfig, str_now):
                msg = '[ERR] Can not create asbackup file. ({0}:{1})'.format(host, namespace)
                print(msg)
                post_msg_to_slack(msg, url=SLACK['url'], username=SLACK['username'], channel=SLACK['channel'])
                exit(4)

            filename = '{directory}/{filename}'.format(directory=setconfig['local_path'], filename=make_file_name(namespace, str_now))
            file_size = os.stat(filename).st_size
            if not estimated_min_size_ok(file_size, setconfig['estimated_min_size']):
                msg = '[ERR] Estimated file size is not OK! ({0}:{1})'.format(host, namespace)
                print(msg)
                post_msg_to_slack(msg, url=SLACK['url'], username=SLACK['username'], channel=SLACK['channel'])
                exit(5)

            if not estimated_max_size_ok(file_size, setconfig['estimated_max_size']):
                msg = '[ERR] Estimated file size is not OK! ({0}:{1})'.format(host, namespace)
                print(msg)
                post_msg_to_slack(msg, url=SLACK['url'], username=SLACK['username'], channel=SLACK['channel'])
                exit(6)

            remote_filename = '{s3_path}/{filename}'.format(s3_path=setconfig['s3_path'], filename=make_file_name(namespace, str_now))
            print('[INF] Uploading {local_file} to s3://{bucket}/{remote_filename}...'.format(
                local_file=filename,
                bucket=setconfig['s3_bucket'],
                remote_filename=remote_filename
            ))

            start_time = time.time()
            s3_upload_file(s3_client, setconfig['s3_bucket'], filename, remote_filename)
            print('[INF] File successfully uploaded in {delta_time:4.2f} minutes!'.format(
                    delta_time = (time.time() - start_time)/60
                )
            )

            if not s3_file_exists(s3_client, setconfig['s3_bucket'], remote_filename):
                msg = '[ERR] File does not exist on S3. Upload error! ({0}:{1})'.format(host, namespace)
                post_msg_to_slack(msg, url=SLACK['url'], username=SLACK['username'], channel=SLACK['channel'])
                exit(7)
            print('[INF] s3 file does exist - OK!')

            if not s3_md5_check(s3_client, setconfig['s3_bucket'], remote_filename, filename):
                msg = '[ERR] local md5 != remote md5 ({0}:{1})'.format(host, namespace)
                post_msg_to_slack(msg, url=SLACK['url'], username=SLACK['username'], channel=SLACK['channel'])
                exit(8)
            print('[INF] s3 md5sum equals local md5sum.')

            if setconfig['remove_local']:
                print('[INF] Removing file {0}...'.format(filename))
                os.unlink(filename)

            total_minutes = (time.time() - start_bkp_time)/60
            megabytes_size = file_size / 1024 / 1024
            msg = 'Namespace {namespace} successfully backed up from host {host} in {minutes} minutes. Backup size - {file_size:4.2f} Mbytes.'.format(
                namespace=namespace,
                host=host,
                minutes=total_minutes,
                file_size=megabytes_size
            )
            print('[INF] {0}'.format(msg))
            if SLACK['always_report']:
                res = post_msg_to_slack(msg, url=SLACK['url'], username=SLACK['username'], channel=SLACK['channel'])
                if res.status_code != 200:
                    print('[ERR] Wrong HTTP code. Slack messaging error!')
                    exit(8)
            print('[INF] Done!')

            s3keys = [ s3key for s3key in s3_list_files(
                            s3_client,
                            s3_bucket=setconfig['s3_bucket'],
                            prefix='{0}/{1}'.format(setconfig['s3_path'], namespace)
                        )
                    ]

            print('[INF] Removing old s3 files...')
            for s3key in s3keys:
                if s3key2delete(s3key, setconfig['s3_store_months'], setconfig['s3_store_days']):
                    try:
                        print('[INF] Removing s3 file {0}...'.format(s3key))
#                        s3_client.delete_object(Bucket=setconfig['s3_bucket'], Key=s3key.key)
                    except ClientError as e:
                        print('[ERR] Client error: {0}'.format(e))

        elif action == 'list':
            s3keys = [ s3key for s3key in s3_list_files(
                            s3_client,
                            s3_bucket=setconfig['s3_bucket'],
                            prefix='{0}/{1}'.format(setconfig['s3_path'], namespace)
                        )
                    ]
            for f in sorted(s3keys):
                print(f)
            print('----------')
            print('Total S3 usage: {:4.4f} MBytes'.format(s3keys_total_size(s3keys)/1024/1024))

        elif action == 'get':
            if len(args) != 6:
                usage(args[0])
                exit(10)

            print('[INF] Downloading {remote_filename} from s3://{s3_bucket}'.format(
                    s3_bucket=setconfig['s3_bucket'],
                    remote_filename=args[4]
                )
            )

            start_time = time.time()
            s3_download_file(s3_client, setconfig['s3_bucket'], args[5], args[4])
            print('[INF] File successfully downloaded in {delta_time:4.2f} minutes!'.format(
                    delta_time = (time.time() - start_time)/60
                )
            )

    except KeyError as e:
        print('[ERR] host ({0}) or namespace ({1}) is not present in configuration. {2}'.format(args[1], args[2], e))
        exit(2)
    except PermissionError as e:
        print('[ERR] Fatal error: {0}'.format(e))
        exit(3)


if __name__ == '__main__':
    main()
