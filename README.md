# phDagCommand
Pharbers Python 工具集合

## 打包和发布方式
```androiddatabinding
# pipy 打包发布方式
1. 修改 setup.py 中的 setuptools.setup.version 
2. 修改 phcli/__main__.py phcli() 和 ph_sql/ph_hive/__main__.py main() 中注释的版本
3. 修改 ph_max_auto/ph_runtime/ph_python3.py 中的 submit_file
4. 修改 file/ph_max_auto/phDagJob-*.tmp 中的 install_phcli
   并将 file/ph_max_auto/phDagJob-*.tmp 上传到 s3://ph-platform/*/template/python/phcli/maxauto/ 下
5. 修改 ph_max_auto/define_value.py 中新的模板文件版本

6. 打包
$ rm build/ dist/
$ python setup.py sdist bdist_egg bdist_wheel

7. 上传
发布 pypi 
$ python -m twine upload dist/*
将生成的 dist/phcli-XXX-py3.8.egg 添加到 s3://ph-platform/*/jobs/python/phcli/common/ 下
```

## 清洗打包流程
```
# zip 打包方式(scala 调用方式)
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
