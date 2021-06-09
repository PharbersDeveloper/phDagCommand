import click
from phcli.ph_auto_setup.emr_operation.ph_emr_operation import PhEmrOperation

@click.command('emr_operation', short_help='对emr的操作')
@click.option("-n", "--name",
              prompt="The emr name is",
              help="The emr name.")
@click.option("-o", "--operation",
              prompt="The operation on emr is",
              type=click.Choice(["create", "delete"]),
              help="The operation on emr.")
def emr_operation(**kwargs):
    """
    初始化c9 hadoop和spark的conf文件
    """
    try:

        PhEmrOperation(**kwargs).choice_operation()
    except Exception as e:
        click.secho("操作失败: " + str(e), fg='red', blink=True, bold=True)
    else:
        click.secho("操作完成", fg='green', blink=True, bold=True)


