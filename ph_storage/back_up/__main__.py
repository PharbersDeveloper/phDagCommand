# -*- coding: utf-8 -*-
# !/usr/bin/python3


import click
from ph_storage.model.hdfs_storage import PhHdfsStorage
from ph_storage.model.local_storage import PhLocalStorage
from ph_storage.model.s3_storage import PhS3Storage


@click.command("hdfsbackup", short_help='HDFS数据备份到S3')
@click.argument("paths")
def main(paths):
    hdfs_ins = PhHdfsStorage(PhLocalStorage(), PhS3Storage())
    hdfs_ins.back_up(paths)


if __name__ == '__main__':
    main()

