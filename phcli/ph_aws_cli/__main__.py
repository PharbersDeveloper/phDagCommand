import click
from phcli.ph_aws_cli.init_conf.__main__ import aws_init


@click.group("aws_cli", short_help='aws_cli系列命令')
def main():
    """
    本脚本用于执行aws cli系列命令
    """
    pass


main.add_command(aws_init)


if __name__ == '__main__':
    main()