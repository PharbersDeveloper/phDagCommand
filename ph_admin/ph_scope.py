import click
import base64
from ph_db.ph_postgresql.ph_pg import PhPg


_pg = PhPg(
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
def create_user():
    print(_pg.tables())
    pass


@click.command("update", short_help='更新用户')
def update_user():
    pass


@click.command("list", short_help='列举用户')
def list_user():
    pass


@click.command("retrieve", short_help='查找用户')
def retrieve_user():
    pass


@click.command("delete", short_help='删除用户')
def delete_user():
    pass


main.add_command(create_user)
main.add_command(update_user)
main.add_command(list_user)
main.add_command(retrieve_user)
main.add_command(delete_user)
