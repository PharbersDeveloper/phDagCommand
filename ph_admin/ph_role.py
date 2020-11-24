import click
import base64
from ph_db.ph_postgresql.ph_pg import PhPg
from ph_admin.ph_models import Role


def _pg():
    return PhPg(
        base64.b64decode('cGgtZGItbGFtYmRhLmNuZ2sxamV1cm1udi5yZHMuY24tbm9ydGh3ZXN0LTEuYW1hem9uYXdzLmNvbS5jbgo=').decode('utf8')[:-1],
        base64.b64decode('NTQzMgo=').decode('utf8')[:-1],
        base64.b64decode('cGhhcmJlcnMK').decode('utf8')[:-1],
        base64.b64decode('QWJjZGUxOTYxMjUK').decode('utf8')[:-1],
        db=base64.b64decode('cGhjb21tb24K').decode('utf8')[:-1],
    )


@click.group("role", short_help='角色管理工具')
def main():
    pass


@click.command("create", short_help='创建角色')
@click.option("-n", "--name", help="角色名", prompt="角色名")
@click.option("-a", "--address", help="角色地址", prompt="角色地址")
@click.option("-p", "--phonenumber", help="角色电话", prompt="角色电话")
@click.option("-w", "--web", help="角色官网", prompt="角色官网")
def create_role(**kwargs):
    print(_pg().insert(Role(**kwargs)))


@click.command("update", short_help='更新角色')
def update_role():
    click.secho('未实现', fg='red', blink=True, bold=True)
    pass


@click.command("list", short_help='列举角色')
def list_role():
    for p in _pg().query(Role()):
        print(p)


@click.command("get", short_help='查找角色')
@click.option("--id", help="角色id", default=None)
@click.option("-n", "--name", help="角色名", default=None)
@click.option("-a", "--address", help="角色地址", default=None)
@click.option("-p", "--phonenumber", help="角色电话", default=None)
@click.option("-w", "--web", help="角色官网", default=None)
def get_role(**kwargs):
    for p in _pg().query(Role(**kwargs)):
        print(p)


@click.command("delete", short_help='删除角色')
@click.option("--id", help="角色id", default=None)
@click.option("-n", "--name", help="角色名", default=None)
@click.option("-a", "--address", help="角色地址", default=None)
@click.option("-p", "--phonenumber", help="角色电话", default=None)
@click.option("-w", "--web", help="角色官网", default=None)
def delete_role(**kwargs):
    print(_pg().delete(Role(**kwargs)))


main.add_command(create_role)
main.add_command(update_role)
main.add_command(list_role)
main.add_command(get_role)
main.add_command(delete_role)
