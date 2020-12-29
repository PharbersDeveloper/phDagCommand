import base64
from ph_db.ph_mysql import PhMysql


def conn_mysql():
    return PhMysql(
        host=base64.b64decode('cGgtZHctaW5zLWNsdXN0ZXIuY2x1c3Rlci1jbmdrMWpldXJtbnYucmRzLmNuLW5vcnRod2VzdC0xLmFtYXpvbmF3cy5jb20uY24K').decode('utf8')[:-1],
        port=base64.b64decode('MzMwNgo=').decode('utf8')[:-1],
        user=base64.b64decode('cGhhcmJlcnMK').decode('utf8')[:-1],
        passwd=base64.b64decode('QWJjZGUxOTYxMjUK').decode('utf8')[:-1],
        db=base64.b64decode('YWlyZmxvdwo=').decode('utf8')[:-1],
    )


ms = conn_mysql()
