import click
import base64
from ph_db.ph_postgresql.ph_pg import PhPg
from ph_admin.ph_models.partner import Partner


def _pg():
    return PhPg(
        base64.b64decode('cGgtZGItbGFtYmRhLmNuZ2sxamV1cm1udi5yZHMuY24tbm9ydGh3ZXN0LTEuYW1hem9uYXdzLmNvbS5jbgo=').decode('utf8')[:-1],
        base64.b64decode('NTQzMgo=').decode('utf8')[:-1],
        base64.b64decode('cGhhcmJlcnMK').decode('utf8')[:-1],
        base64.b64decode('QWJjZGUxOTYxMjUK').decode('utf8')[:-1],
        db=base64.b64decode('cGhjb21tb24K').decode('utf8')[:-1],
    )


@click.group("partner", short_help='公司管理工具')
def main():
    """
    公司管理工具
    """


@click.command("create", short_help='创建公司')
@click.option("-n", "--name", help="公司名", prompt="公司名")
@click.option("-a", "--address", help="公司地址", prompt="公司地址")
@click.option("-p", "--phonenumber", help="公司电话", prompt="公司电话")
@click.option("-w", "--web", help="公司官网", prompt="公司官网")
def create_partner(**kwargs):
    print(_pg().insert(Partner(**kwargs)))


@click.command("update", short_help='更新公司')
def update_partner():
    click.secho('未实现', fg='red', blink=True, bold=True)
    pass


@click.command("list", short_help='列举公司')
def list_partner():
    for p in _pg().query(Partner()):
        print(p)


@click.command("get", short_help='查找公司')
@click.option("--id", help="公司id", default=None)
@click.option("-n", "--name", help="公司名", default=None)
@click.option("-a", "--address", help="公司地址", default=None)
@click.option("-p", "--phonenumber", help="公司电话", default=None)
@click.option("-w", "--web", help="公司官网", default=None)
def get_partner(**kwargs):
    for p in _pg().query(Partner(**kwargs)):
        print(p)


@click.command("delete", short_help='删除公司')
@click.option("--id", help="公司id", default=None)
@click.option("-n", "--name", help="公司名", default=None)
@click.option("-a", "--address", help="公司地址", default=None)
@click.option("-p", "--phonenumber", help="公司电话", default=None)
@click.option("-w", "--web", help="公司官网", default=None)
def delete_partner(**kwargs):
    for p in _pg().delete(Partner(**kwargs)):
        print(p)


main.add_command(create_partner)
main.add_command(update_partner)
main.add_command(list_partner)
main.add_command(get_partner)
main.add_command(delete_partner)
