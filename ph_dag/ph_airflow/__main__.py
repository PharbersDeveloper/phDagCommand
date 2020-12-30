import click
from ph_dag.ph_airflow import ms
from ph_dag.ph_airflow.model import Dag

@click.group("airflow")
def airflow():
    """
    airflow 管理
    """
    pass


@airflow.command("list", short_help='列出符合条件的 DAG 信息')
@click.option("-o", "--owners", help="所有者", default=None)
def airflow_list(**kwargs):
    for dag in ms.query(Dag(**kwargs)):
        click.secho(str(dag), fg='green', blink=True, bold=True)
