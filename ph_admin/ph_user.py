import click
import hashlib
from ph_admin import pg
from datetime import datetime
from ph_admin.ph_models import Account, Role, Partner


@click.group("user", short_help='用户管理工具')
def main():
    pass


@click.command("create", short_help='创建用户')
@click.option("-n", "--name", help="用户名", prompt="用户名")
@click.option("-p", "--password", help="用户密码", prompt="用户密码", hide_input=True, confirmation_prompt=True)
@click.option("--phoneNumber", help="用户电话", prompt="用户电话")
@click.option("-r", "--defaultRole", help="默认角色", prompt="默认角色", default='test')
@click.option("-e", "--email", help="用户邮箱", prompt="用户邮箱")
@click.option("--employer", help="所属公司", prompt="所属公司", default='test')
@click.option("--firstName", help="名", prompt="名")
@click.option("--lastName", help="姓", prompt="姓")
def create_user(**kwargs):
    kwargs['phoneNumber'] = kwargs.pop('phonenumber')
    kwargs['defaultRole'] = kwargs.pop('defaultrole')
    kwargs['firstName'] = kwargs.pop('firstname')
    kwargs['lastName'] = kwargs.pop('lastname')

    sha256 = hashlib.sha256()
    sha256.update(kwargs['password'].encode('utf-8'))
    kwargs['password'] = sha256.hexdigest()
    default_role = kwargs.pop('defaultRole')
    employer = kwargs.pop('employer')

    roles = pg.query(Role(name=default_role))
    role_id = roles[0].id if roles else None
    role_account = roles[0].accountRole if role_id else []

    employers = pg.query(Partner(name=employer))
    employer_id = employers[0].id if employers else None
    employer_employee = employers[0].employee if employer_id else []

    kwargs['defaultRole'] = role_id
    kwargs['employer'] = employer_id
    account = pg.insert(Account(**kwargs))
    pg.update(Role(id=role_id, accountRole=role_account+[account.id], modified=datetime.now()))
    pg.update(Partner(id=employer_id, employee=employer_employee+[account.id], modified=datetime.now()))

    click.secho('添加成功'+str(account), fg='green', blink=True, bold=True)
    pg.commit()


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
