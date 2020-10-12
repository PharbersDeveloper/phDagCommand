# -*- coding: utf-8 -*-
# !/usr/bin/python3


import click
from ph_storage.model.hdfs_storage import PhHdfsStorage
from ph_storage.model.local_storage import PhLocalStorage
from ph_storage.model.s3_storage import PhS3Storage

@click.command("hdfsclean", short_help='HDFS硬删除')
@click.argument("paths")
def main(paths):
    if paths is None or paths == "{}":
        paths = ["/logs/yarnLogs", "/logs/sparkLogs"]

    if not isinstance(paths, list):
        paths = paths.replace('[', '').replace(']', '').split(',')

    hdfs_ins = PhHdfsStorage(PhLocalStorage(), PhS3Storage())
    hdfs_ins.clean_hdfs(paths)


if __name__ == '__main__':
    main()

