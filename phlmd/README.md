# phDagCommand/lmd
用于快捷部署 AWS Lambda 的工具脚本

# 一、安装方式
进入 [https://github.com/PharbersDeveloper/phDagCommand/releases](https://github.com/PharbersDeveloper/phDagCommand/releases) 下载一个最新的 releases 版本的二进制文件。将其放到环境变量中即可。

# 二、使用方式
## 2.1 项目初始化
在 lambda 项目目录下执行以下命令：

#### python
```$xslt
phcli lmd deploy init \
    --name example \
    --runtime python3.8 \
    --desc 自动化发布 \
    --lib_path .venv/lib/python3.8/site-packages/ \
    --code_path hello_world \
    --handler hello_world.app.lambda_handler
```

#### nodejs
```$xslt
phcli lmd deploy init \
    --name example \
    --runtime nodejs10.x \
    --desc 自动化发布 \
    --lib_path node_modules/ \
    --code_path dist/,config/,app.js \
    --handler app.lambdaHandler
```

## 2.2 使用方式
### 首次使用需要创建 AWS 的四种资源
AWS Lambda 基于 role、layer、function、API Gateway 四种资源, 此操作需要一定的管理员权限，建议联系相关运维人员
```$xslt
phcli lmd deploy push -n example --all
```

### 常用发布
只发布当前代码并绑定到 API Gateway，可使用如下命令
```$xslt
phcli lmd deploy push -n example
```

### 只更新当前代码，不绑定到 API Gateway
```$xslt
phcli lmd deploy push -n example -o code
```

### lib 依赖库更新
```$xslt
phcli lmd deploy push -n example -o lib
```


# 开发人员手册
## 二进制打包方式
```$xslt
pyinstaller phcli/__main__.py -F -n phcli -p .
```
