# -*- coding: utf-8 -*-

from phcli.ph_max_auto import define_value as dv
from phcli.ph_max_auto.ph_config.phconfig.phconfig import PhYAMLConfig


def create(path, phs3):
    # 1. /phjob.R file
    phs3.download(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHJOB_FILE_R, path + "/phjob.R")
    config = PhYAMLConfig(path)
    config.load_yaml()

    with open(path + "/phjob.R", "a") as file:
        file.write("execute <- function(")
        for arg_index in range(len(config.spec.containers.args)):
            arg = config.spec.containers.args[arg_index]
            if arg_index == len(config.spec.containers.args) - 1:
                file.write(arg.key)
            else:
                file.write(arg.key + ", ")
        file.write("){\n")
        file.write('\t# please input your code below\n')
        file.write('\tprint(a)\n')
        file.write('\tprint(b)\n')
        file.write('}')

    # 2. /phmain.R file
    f_lines = phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHMAIN_FILE_R)
    with open(path + "/phmain.R", "w") as file:
        options_args = []
        for arg in config.spec.containers.args:
            options_args.append('c("key"="{key}", "desc"="参数{key}")'.format(key=arg.key))

        for line in f_lines:
            line = line + "\n"
            if "$options_args" in line:
                line = line.replace("$options_args", ',\n\t'.join(options_args))
            file.write(line)


def submit_conf(path, phs3, runtime):
    return {}


def submit_file(submit_prefix):
    return {
        "files": submit_prefix + "phjob.R",
    }


def submit_main(submit_prefix):
    return submit_prefix + "phmain.R"

