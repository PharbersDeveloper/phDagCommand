# -*- coding: utf-8 -*-
#!/usr/bin/python

import click
from ph_max_auto.phcommand import maxauto
from ph_lmd.__main__ import main as phlam_main
from ph_data_clean.__main__ import main as clean_main


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
def phcli():
    pass


phcli.add_command(maxauto)
phcli.add_command(phlam_main)
phcli.add_command(clean_main)


if __name__ == '__main__':
    phcli()
