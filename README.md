# phDagCommand
Pharbers Python 工具集合

## 打包和发布方式
```androiddatabinding
# pipy 打包发布方式
1. 修改 setup.py 中的 setuptools.setup.version 
2. 修改 phcli/__main__.py phcli() 和 phcli/ph_sql/ph_hive/__main__.py main() 中注释的版本
3. 修改 phcli/ph_max_auto/phcontext/ph_runtime/ph_python3.py 中的 submit_file
4. 修改 file/ph_max_auto/phDagJob-*.tmp 中的 install_phcli
   并将 file/ph_max_auto/phDagJob-*.tmp 上传到 s3://ph-platform/*/template/python/phcli/maxauto/ 下
5. 修改 phcli/ph_max_auto/define_value.py 中新的模板文件版本

6. 打包
$ rm -rf build/ dist/
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

# 更新文档
## 2.0.4
1. 修复 jupyter dag 时 timeout 参数的 bug
2. 修复 preset.write_asset 的 bug

## 2.0.3
1. phcli maxauto create 新增 option [--timeout]，指定 job 运行的超时时间（min），默认为 60 min。

## 2.0.0
1. 之前的 phcli maxauto --cmd XXX --option 改为 phcli maxauto XXX --option
2. phcli maxauto 新增两个 option，[--ide] 可选 c9(默认)，jupyter。[--runtime] 可选 python3(默认)，r。
3. phcli maxauto create 新增 option [--command] 可选 submit(默认)，script。
4. create/combine/dag 执行过程中出现目录已存在会提示是否覆盖。
5. phcli maxauto combine 新增参数 [--owner]，[--tag]，[--jobs]。
6. phcli maxauto submit 改为 phcli maxauto online_run

## 1.2.3
1. dag airflow list 完成
2. phcli 1.2.3 spark submit 中文乱码 bug 解决
3. phcli 1.2.2 修复 submit jar 问题
