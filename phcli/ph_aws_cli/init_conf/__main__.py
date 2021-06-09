import click
from phcli.ph_aws_cli.init_conf.ph_init_conf import PhInitConf

@click.command('init_conf', short_help='aws cp hadoop和spark的conf')
@click.option("-c", "--cluster_id",
              prompt="The emr cluster_id is",
              help="The emr cluster_id.")
def aws_init(**kwargs):
    """
    初始化c9 hadoop和spark的conf文件
    """
    try:
        cluster_id = kwargs["cluster_id"]
        PhInitConf.init_conf(cluster_id=cluster_id)
    except Exception as e:
        click.secho("初始化失败: " + str(e), fg='red', blink=True, bold=True)
    else:
        click.secho("初始化完成", fg='green', blink=True, bold=True)


