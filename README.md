# hash_all_files
Create csv file with information about all files in specific directory.


help:

usage: hash_all_files [-h] [--dir [start directory]] [--hash HASH] [--w W] output file

Create hash for all files in specific directory

positional arguments:
  output file           Output file

optional arguments:
  -h, --help            show this help message and exit
  --dir [start directory]
                        Start directory (default: c:\)
  --hash HASH           Select hash functions [md5, sha1, sha256] (default:
                        md5, sha1)
  --w W                 Set maximum of workers 0 = same as number of cores
                        (default: 0)
