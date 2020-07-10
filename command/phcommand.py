# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the usage of class phCommand,
which help users to create, update, and publish the jobs they created.
"""
import click
from phcontext.phcontextfacade import PhContextFacade


@click.command()
@click.option("--cmd", prompt="Your command is", help="The command that you want to process.",
              type=click.Choice(["create", "combine", "dag", "publish", "run", "submit", "status"]))
@click.option("-p", "--path", prompt="Your config and python job file directory",
              help="The concert job you want the process.")
@click.option("-c", "--context", default="{}")
def maxauto(cmd, path, context):
    """The Pharbers Max Job Command Line Interface (CLI)

        --cmd Args: \n
            create: to generate a job template \n
            combine: to combine job into a job sequence \n
            publish: to publish job to pharbers IPaaS \n

        --path Args: \n
            the dictionary that specify the py and yaml file
    """
    facade = PhContextFacade(cmd, path, context)
    click.get_current_context().exit(facade.execute())
