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
@click.option("-I", "--ide",
              help="You IDE.",
              type=click.Choice(["c9", "jupyter"]),
              default=os.getenv(dv.ENV_CUR_IDE_KEY, dv.ENV_CUR_IDE_DEFAULT))
@click.option("-R", "--runtime",
              help="You use programming language.",
              type=click.Choice(["python3", "r"]),
              default=os.getenv(dv.ENV_CUR_RUNTIME_KEY, dv.ENV_CUR_RUNTIME_DEFAULT))
def maxauto(**kwargs):
    """
    The Pharbers Max Job Command Line Interface (CLI)
    """
    global context_args
    context_args = kwargs


@maxauto.command("create")
@click.option("-g", "--group",
              prompt="The job group is",
              help="The job group.",
              default="")
@click.option("-n", "--name",
              prompt="The job name is",
              help="The job name.")
@click.option("-C", "--command",
              prompt="The job command is",
              help="The job command.",
              type=click.Choice(["submit", "script"]),
              default="submit")
@click.option("-T", "--timeout",
              prompt="The job timeout (min) is",
              help="The job timeout (min) .",
              type=click.FLOAT,
              default="10")
@click.option("-i", "--inputs",
              prompt="The job inputs is",
              help="The job inputs.",
              default="a,b")
@click.option("-o", "--outputs",
              prompt="The job outputs is",
              help="The job outputs.",
              default="c,d")
def create(**kwargs):
    """
    创建一个 Job
    """
    context_args.update(kwargs)
    PhContextFacade(**context_args).command_create_exec()
    click.secho("创建完成", fg='green', blink=True, bold=True)


@maxauto.command("run")
@click.option("-g", "--group",
              prompt="The job group is",
              help="The job group.",
              default="")
@click.option("-n", "--name",
              prompt="The job name is",
              help="The job name.")
def run(**kwargs):
    """
    运行一个 Job
    """
    context_args.update(kwargs)
    PhContextFacade(**context_args).command_run_exec()
    click.secho("运行完成", fg='green', blink=True, bold=True)


@maxauto.command("combine")
@click.option("-n", "--name",
              prompt="The dag name is",
              help="The dag name.")
@click.option("-o", "--owner",
              prompt="The dag owner is",
              help="The dag owner.",
              default="default")
@click.option("-t", "--tag",
              prompt="The dag tag is",
              help="The dag tag.",
              default="default")
@click.option("-T", "--timeout",
              prompt="The dag timeout (min) is, default=$(jobs timeout total)",
              help="The dag timeout.",
              default="")
@click.option("-j", "--jobs",
              prompt="The dag jobs is",
              help="The dag jobs.",
              default="group.test1, group.test2")
def combine(**kwargs):
    """
    关联一组 Job
    """
    context_args.update(kwargs)
    PhContextFacade(**context_args).command_combine_exec()
    click.secho("关联完成", fg='green', blink=True, bold=True)


@maxauto.command("dag")
@click.option("-n", "--name",
              prompt="The dag name is",
              help="The dag name.")
def dag(**kwargs):
    """
    通过 combine 生成一组 DAG 运行文件
    """
    context_args.update(kwargs)
    PhContextFacade(**context_args).command_dag_exec()
    click.secho("DAG完成", fg='green', blink=True, bold=True)


@maxauto.command("publish")
@click.option("-n", "--name",
              prompt="The dag name is",
              help="The dag name.")
def publish(**kwargs):
    """
    发布 DAG 运行文件和相关依赖
    """
    context_args.update(kwargs)
    PhContextFacade(**context_args).command_publish_exec()
    click.secho("发布完成", fg='green', blink=True, bold=True)


@maxauto.command("recall")
@click.option("-n", "--name",
              prompt="The dag name is",
              help="The dag name.")
def recall(**kwargs):
    """
    召回 DAG 运行文件和相关依赖
    """
    context_args.update(kwargs)
    PhContextFacade(**context_args).command_recall_exec()
    click.secho("召回完成", fg='green', blink=True, bold=True)


@maxauto.command("online_run")
@click.option("-g", "--group",
              prompt="The dag job group is",
              help="The dag job group.",
              default="")
@click.option("-n", "--name",
              prompt="The dag job name is",
              help="The dag job name.")
@click.option("--owner", default="owner")
@click.option("--run_id", default="run_id")
@click.option("--job_id", default="job_id")
@click.option("-c", "--context", help="online_run context", default="{}")
@click.argument('args', nargs=1, default="{}")
def online_run(**kwargs):
    """
    通过指定 Job name 在线上执行
    """
    context_args.update(kwargs)
    PhContextFacade(**context_args).command_online_run_exec()
    click.secho("online_run完成", fg='green', blink=True, bold=True)


@maxauto.command("status")
def status(**kwargs):
    """
    获取执行状态（暂无）
    """
    context_args.update(kwargs)
    PhContextFacade(**context_args).command_status_exec()
    click.secho("查看状态完成", fg='green', blink=True, bold=True)
