# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the usage of class phCommand,
which help users to create, update, and publish the jobs they created.
"""
import os
import click
from phcli.ph_max_auto import define_value as dv
from phcli.ph_max_auto.phcontext.phcontextfacade import PhContextFacade

context_args = {}


@click.group("maxauto")
@click.option("--ide",
              prompt="Your IDE is",
              help="You IDE.",
              type=click.Choice(["c9", "jupyter"]),
              default=os.getenv(dv.ENV_CUR_IDE_KEY, dv.ENV_CUR_IDE_DEFAULT))
@click.option("-r", "--runtime",
              prompt="Your programming language is",
              help="You use programming language.",
              type=click.Choice(["python3", "r"]),
              default=os.getenv(dv.ENV_CUR_RUNTIME_KEY, dv.ENV_CUR_RUNTIME_DEFAULT))
@click.option("-g", "--group",
              prompt="The job group is",
              help="The job group.",
              default="")
@click.option("-n", "--name",
              prompt="The job name is",
              help="The job name.")
def maxauto(**kwargs):
    """
    The Pharbers Max Job Command Line Interface (CLI)
    """
    global context_args
    context_args = kwargs


@maxauto.command("create")
def create(**kwargs):
    """
    创建一个 Job
    """
    context_args.update(kwargs)
    PhContextFacade(**context_args).command_create_exec()
    print(context_args)
    print('create')


@maxauto.command("run")
def run():
    """
    运行一个 Job
    """
    print(context_args)
    print('run')


@maxauto.command("combine")
def combine():
    """
    关联一组 Job
    """
    print(context_args)
    print('combine')


@maxauto.command("dag")
def dag():
    """
    通过 combine 生成一组 DAG 运行文件
    """
    print(context_args)
    print('dag')


@maxauto.command("publish")
def publish():
    """
    发布 DAG 运行文件和相关依赖
    """
    print(context_args)
    print('publish')


@maxauto.command("submit")
@click.option("--owner", default="")
@click.option("--run_id", default="")
@click.option("--job_id", default="")
@click.option("-c", "--context", help="submit context", default="{}")
@click.argument('args', nargs=1, default="{}")
def submit():
    """
    通过指定 Job name 执行一个 spark submit
    """
    print(context_args)
    print('submit')


@maxauto.command("status")
def status():
    """
    获取执行状态（暂无）
    """
    print(context_args)
    print('status')


# def maxauto(**kwargs):
#     """The Pharbers Max Job Command Line Interface (CLI)
#         --runtime Args: \n
#             python: This is to see \n
#             R: This is to see \n
#
#         --cmd Args: \n
#             status: \n
#             create: to generate a job template \n
#             run: \n
#             combine: to combine job into a job sequence \n
#             dag: \n
#             submit: \n
#             publish: to publish job to pharbers IPaaS \n
#
#         --group Args: \n
#             The concert job you want the process group.
#
#         --owner Args: Current Owner. \n
#         --run_id Args: Current run_id. \n
#         --job_id Args: Current job_id. \n
#
#         --path Args: \n
#             The dictionary that specify the py and yaml file
#     """
#     facade = PhContextFacade(**kwargs)
    click.get_current_context().exit(facade.execute())
