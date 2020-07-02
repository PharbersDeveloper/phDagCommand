import os
import ph_data_clean


def test_go_rt_pkg_layer_pipenv():
    print("abc")
    ph_data_clean.init()
    # args = {
    #     "package_name": "test_go_rt_layer_pipenv.zip",
    #     "is_pipenv": True,
    # }
    #
    # go_rt.pkg_layer(args)
    # assert os.path.exists(args["package_name"])
    # os.remove(args["package_name"])
