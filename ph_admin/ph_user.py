import click
import base64
import hashlib
from datetime import datetime
from ph_db.ph_postgresql.ph_pg import PhPg
from ph_admin.ph_models.accounts import Account
from ph_admin.ph_models.role import Role
from ph_admin.ph_models.partner import Partner


def _pg():
    return PhPg(
        base64.b64decode('cGgtZGItbGFtYmRhLmNuZ2sxamV1cm1udi5yZHMuY24tbm9ydGh3ZXN0LTEuYW1hem9uYXdzLmNvbS5jbgo=').decode('utf8')[:-1],
        base64.b64decode('NTQzMgo=').decode('utf8')[:-1],
        base64.b64decode('cGhhcmJlcnMK').decode('utf8')[:-1],
        base64.b64decode('QWJjZGUxOTYxMjUK').decode('utf8')[:-1],
        db=base64.b64decode('cGhjb21tb24K').decode('utf8')[:-1],
    )


@click.group("user", short_help='用户管理工具')
def main():
    pass


@click.command("create", short_help='创建用户')
@click.option("-n", "--name", help="用户名", prompt="用户名")
@click.option("-p", "--password", help="用户密码", prompt="用户密码", hide_input=True, confirmation_prompt=True)
@click.option("--phonenumber", help="用户电话", prompt="用户电话")
@click.option("-r", "--defaultrole", help="默认角色", prompt="默认角色", default='test')
@click.option("-e", "--email", help="用户邮箱", prompt="用户邮箱")
@click.option("--employer", help="所属公司", prompt="所属公司", default='test')
@click.option("--firstname", help="名", prompt="名")
@click.option("--lastname", help="姓", prompt="姓")
def create_user(**kwargs):
    pg = _pg()
    sha256 = hashlib.sha256()
    sha256.update(kwargs['password'].encode('utf-8'))
    kwargs['password'] = sha256.hexdigest()
    defaultrole = kwargs.pop('defaultrole')
    employer = kwargs.pop('employer')

    account = pg.insert(Account(**kwargs))

    roles = pg.query(Role(name=defaultrole))
    account.defaultRole = [r.id for r in roles]

    employers = pg.query(Partner(name=employer))
    account.employer = [e.id for e in employers]

    pg.update(account)


@click.command("update", short_help='更新用户')
@click.option("-n", "--name", help="用户名", prompt="用户名")
@click.option("-p", "--password", help="用户密码", default=None, hide_input=True, confirmation_prompt=True)
@click.option("--phonenumber", help="用户电话", default=None)
@click.option("-r", "--defaultrole", help="默认角色", default=None)
@click.option("-e", "--email", help="用户邮箱", default=None)
@click.option("--employer", help="所属公司", default=None)
@click.option("--firstname", help="名", default=None)
@click.option("--lastname", help="姓", default=None)
def update_user(**kwargs):
    pg = _pg()
    name = kwargs.pop('name')
    accounts = pg.query(Account(name=name))

    if not accounts:
        print("无更新数据")
        return

    kwargs = dict([(k, v) for k, v in kwargs.items() if v])
    for account in accounts:
        pg.update(Account(id=account.id, modified=datetime.now(), **kwargs))


@click.command("list", short_help='列举用户')
def list_user():
    for a in _pg().query(Account()):
        print(a)


@click.command("get", short_help='查找用户')
@click.option("-n", "--name", help="用户名", prompt="用户名")
def get_user(**kwargs):
    for a in _pg().query(Account(**kwargs)):
        print(a)


@click.command("delete", short_help='删除用户')
@click.option("-n", "--name", help="用户名", prompt="用户名")
def delete_user(**kwargs):
    for a in _pg().delete(Account(**kwargs)):
        print(a)


main.add_command(create_user)
main.add_command(update_user)
main.add_command(list_user)
main.add_command(get_user)
main.add_command(delete_user)
