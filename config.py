SERVERS = {
  '172.31.31.11': {
    'userdata': {
      'nice': 30,
      'local_path': '/mnt/aerospike_backup/userdata',
      's3_bucket': 'personartb-backup',
      's3_path': 'prod/us-aerospike-cluster',
      'log_directory': '/tmp',
      'remote_store_size': '5Tb',
      'remove_local': True,
      'with_md5': True,
    },
    'dictionary': {
      'nice': 30,
      'local_path': '/mnt/aerospike_backup/directory',
      's3_bucket': 'personartb-backup',
      's3_path': 'prod/us-aerospike-cluster',
      'log_directory': '/tmp',
      'remote_store_size': '500Gb',
      'remove_local': True,
    },
    'trackdata': {
      'nice': 30,
      'local_path': '/mnt/aerospike_backup/trackdata',
      's3_bucket': 'personartb-backup',
      's3_path': 'prod/us-aerospike-cluster',
      'log_directory': '/tmp',
      'remote_store_size': '500Gb',
      'remove_local': True
    }
  }
}

