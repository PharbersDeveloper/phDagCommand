import click
from tmp.ph_emr_errlog.py import phErrLogs

@click.command('emr_errlogs', short_help='提取错误日志')
@click.option("-c","--cluster-id",
              prompt="The cluster-id is",
              help="The cluster id"
             )
@click.option("-s","--step-id",
              prompt="The step-id is",
              help="The step id"
             )

def emr_errlogs(**args):
     try:
        phErrLogs(**args).start()
    except Exception as e:
        click.secho("操作失败: " + str(e), fg='red', blink=True, bold=True)
    else:
        click.secho("操作完成", fg='green', blink=True, bold=True)
   