#!/usr/bin/python
import click
from command.phcommand import maxauto
from phlmd.__main__ import lmd


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
def phcli():
    pass


phcli.add_command(maxauto)
phcli.add_command(lmd)


if __name__ == '__main__':
    phcli()
