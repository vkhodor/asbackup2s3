import os
from hashlib import md5


def factor_of_1MB(filesize, num_parts):
  x = filesize / int(num_parts)
  y = x % 1048576
  return int(x + 1048576 - y)


def calc_etag(inputfile, partsize):
  md5_digests = []
  with open(inputfile, 'rb') as f:
    for chunk in iter(lambda: f.read(partsize), b''):
      md5_digests.append(md5(chunk).digest())
  return md5(b''.join(md5_digests)).hexdigest() + '-' + str(len(md5_digests))


def possible_partsizes(filesize, num_parts):
  return lambda partsize: partsize < filesize and (float(filesize) / float(partsize)) <= num_parts


def possible_etags(filename, num_parts):
    filesize = os.stat(filename).st_size
    etags = []

    # Default Part Sizes Map
    part_sizes = [
        8388608, # aws_cli/boto3
        15728640, # s3cmd
        factor_of_1MB(filesize, num_parts) # used by many clients to upload large files
    ]

    return [calc_etag(filename, part_size) for part_size in filter(possible_partsizes(filesize, num_parts), part_sizes)]