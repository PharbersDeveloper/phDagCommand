import click
import os


@click.group()
def cli():
    pass


@cli.command()
def initdb():
    click.echo('Initialized the database')


@cli.command()
@click.option('--count', default=1, help='number of greetings')
@click.argument('name')
def hello(count, name):
    for x in range(count):
        click.echo(f"Hello {name}!")


@cli.command()
@click.option('--username', prompt=True,
              default=lambda: os.environ.get('USER', ''),
              show_default='current user')
def prompt(username):
    print("Hello, ", username)


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo('Version 1.0')
    ctx.exit()


@cli.command()
@click.option('--version', is_flag=True, callback=print_version,
              expose_value=False, is_eager=True)
def callback():
    click.echo('Hello World!')


def abort_if_false(ctx, param, value):
    if not value:
        ctx.abort()


@cli.command()
@click.option('--yes', is_flag=True, callback=abort_if_false,
              expose_value=False,
              prompt='Are you sure you want to drop the db?')
def dropdb():
    click.echo('Dropped all tables!')


@cli.command()
@click.option('/debug;/no-debug')
def log(debug):
    click.echo(f"debug = {'on' if debug else 'off'}")


@cli.group()
def sub():
    pass


def get_colors(ctx, args, incomplete):
    colors = [('red', 'a warm color'),
              ('blue', 'a cool color'),
              ('green', 'the other starter color')]
    return [c for c in colors if incomplete in c[0]]


@sub.command()
@click.option('--count', default=1, help='number of greetings')
@click.argument('name', autocompletion=get_colors)
def hello(count, name):
    for x in range(count):
        click.secho(f"Hello {name}!", fg='green', blink=True, bold=True)


@sub.command()
def less():
    click.echo_via_pager("\n".join(f"Line {idx}" for idx in range(200)))


@sub.command()
def pb():
    import time
    with click.progressbar([1, 2, 3]) as bar:
        for x in bar:
            print(f"sleep({x})...")
            time.sleep(x)


@sub.command()
@click.option('/debug;/no-debug')
def log(debug):
    click.echo(f"debug = {'on' if debug else 'off'}")

import boto3
import pandas as pd
if __name__ == '__main__':
    cli()
    # client = boto3.client('s3')
    # resource = client.get_object(
    #     Bucket='ph-origin-files',
    #     Key='CHC/ACN/XTDET项目交付表表-2001.xlsx'
    # )
    # excel_data = pd.ExcelFile(resource['Body'].read())
    # dataframe = excel_data.parse(excel_data.sheet_names[-1])
    # print(dataframe)

