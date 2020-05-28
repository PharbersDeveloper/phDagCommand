#!/usr/bin/python

from command.phcommand import ph_command
from phs3.phs3 import PhS3


def main():
    ph_command()


if __name__ == '__main__':
    s3 = PhS3()
    s3.get_object_lines("ph-cli-dag-template", "template/phDagJob.tmp")
    main()
