# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the usage of class phCommand,
which help users to create, update, and publish the jobs they created.
"""
import click
from ph_max_auto.phcontext.phcontextfacade import PhContextFacade


@click.command()
@click.option("-r", "--runtime", prompt="Your programming language is", help="You use programming language.",
              type=click.Choice(["python3", "r"]), default="python3")
@click.option("-g", "--group", help="The concert job you want the process group.", default="")
@click.option("-p", "--path", prompt="Your config and python job file directory",
              help="The concert job you want the process.")
@click.option("--cmd", prompt="Your command is", help="The command that you want to process.",
              type=click.Choice(["create", "run", "combine", "dag", "publish", "submit", "status"]), default="status")
@click.option("--job_id", default="")
@click.option("-c", "--context", help="submit context", default="{}")
@click.argument('args', nargs=1, default="{}")
def maxauto(**kwargs):
    """The Pharbers Max Job Command Line Interface (CLI)
        --runtime Args: \n
            python: This is to see \n
            R: This is to see \n

        --cmd Args: \n
            status: \n
            create: to generate a job template \n
            run: \n
            combine: to combine job into a job sequence \n
            dag: \n
            submit: \n
            publish: to publish job to pharbers IPaaS \n

        --group Args: \n
            The concert job you want the process group.

        --path Args: \n
            The dictionary that specify the py and yaml file
    """
    facade = PhContextFacade(**kwargs)
    click.get_current_context().exit(facade.execute())
