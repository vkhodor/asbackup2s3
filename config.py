SERVERS = {
  '172.31.31.11': {
    'userdata': {
      'nice': 30,
      'local_path': '/mnt/aerospike-backups/userdata',
      's3_bucket': 'personartb-backup',
      's3_path': 'prod/us-aerospike-cluster',
      'remote_store_size': '5Tb',
      'remove_local': True,
      'with_md5': True,
      'estimated_min_size': 30000000000,
      'estimated_max_size': 30000000000

    },
    'dictionary': {
      'nice': 30,
      'local_path': '/mnt/aerospike-backups/directory',
      's3_bucket': 'personartb-backup',
      's3_path': 'prod/us-aerospike-cluster',
      'remote_store_size': '500Gb',
      'remove_local': True,
      'estimated_min_size': 600000000,
      'estimated_max_size': 600000000
    },
    'trackdata': {
      'nice': 30,
      'local_path': '/mnt/aerospike-backups/trackdata',
      's3_bucket': 'personartb-backup',
      's3_path': 'prod/us-aerospike-cluster',
      'remote_store_size': '500Gb',
      'remove_local': True,
      'estimated_min_size': 30000000000,
      'estimated_max_size': 30000000000
    }
  }
}

SLACK = {
  'url': 'https://hooks.slack.com/services/test',
  'username': 'test',
  'channel': 'test',
  'always_report': True
}
