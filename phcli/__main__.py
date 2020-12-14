# -*- coding: utf-8 -*-
#!/usr/bin/python

import click
from ph_max_auto.phcommand import maxauto
from ph_logs.__main__ import main as logs_main
from ph_lmd.__main__ import main as phlam_main
from ph_admin.__main__ import main as phadmin_main
from ph_sql.ph_hive.__main__ import main as phhive_main
from ph_data_clean.__main__ import main as clean_main
from ph_storage.back_up.__main__ import main as hdfs_back_up_main
from ph_storage.clean.__main__ import main as hdfs_clean_main


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
def phcli():
    """
    Pharbers Command Line Interface v1.2.2
    """
    pass


phcli.add_command(logs_main)
phcli.add_command(maxauto)
phcli.add_command(phlam_main)
phcli.add_command(phadmin_main)
phcli.add_command(phhive_main)
phcli.add_command(clean_main)
phcli.add_command(hdfs_back_up_main)
phcli.add_command(hdfs_clean_main)


if __name__ == '__main__':
    phcli()
