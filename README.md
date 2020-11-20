# phDagCommand
Pharbers Python 工具集合

## 打包和发布方式
```androiddatabinding
# pipy 打包发布方式
$ python setup.py sdist bdist_egg bdist_wheel
$ python -m twine upload dist/*

将生成的 dist/phcli-XXX-py3.8.egg 添加到 s3://s3fs-ph-airflow/airflow/dags/phjobs/common/ 下

# zip 打包方式
$ python setup.py sdist --formats=zip
```

## 安装方式
```androiddatabinding
$ pip install phcli
```

## 使用方法
```androiddatabinding
> phcli -h
```
