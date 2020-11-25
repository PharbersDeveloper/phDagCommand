# phDagCommand
Pharbers Python 工具集合

## 打包和发布方式
```androiddatabinding
# pipy 打包发布方式
1. 修改 ph_max_auto/ph_runtime/ph_python3.py 中的 submit_file
2. 修改 file/ph_max_auto/phDagJob-*.tmp 中的 install_phcli
3. 将 file/ph_max_auto/phDagJob-*.tmp 上传到 s3://ph-platform/*/template/python/phcli/maxauto/ 下

4. 打包
$ python setup.py sdist bdist_egg bdist_wheel
$ python -m twine upload dist/*

5. 上传
将生成的 dist/phcli-XXX-py3.8.egg 添加到 s3://ph-platform/*/jobs/python/phcli/common/ 下

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
