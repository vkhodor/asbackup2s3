SERVERS = {
  '3.12.67.231': {
    'userdata': {
      'nice': 30,
      'local_path': '/mnt/aerospike_backup/userdata',
      'remote_path': 's3://personartb-backup/aerospike-cluster/userdata',
      'log_directory': '/tmp',
      'remote_store_size': '5Tb',
      'remove_local': True,
      'with_md5': True,
      'gzip': False
    },
    'directory': {
      'nice': 30,
      'local_path': '/mnt/aerospike_backup/directory',
      'remote_path': 's3://personartb-backup/aerospike-cluster/directory',
      'log_directory': '/tmp',
      'remote_store_size': '500Gb',
      'remove_local': True
    },
    'trackdata': {
      'nice': 30,
      'local_path': '/mnt/aerospike_backup/trackdata',
      'remote_path': 's3://personartb-backup/aerospike-cluster/trackdata',
      'log_directory': '/tmp',
      'remote_store_size': '500Gb',
      'remove_local': True
    }
  }
}

