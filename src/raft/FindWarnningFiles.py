# -*- coding: utf-8 -*-
# @Time    : 2024/1/16 15:03
# @Author  : qxcnwu
# @FileName: FindWarnningFiles.py
# @Software: PyCharm

import os


def find_all(path: str):
    """

    :param path:
    :return:
    """
    for file in os.listdir(path):
        if file.endswith("log"):
            with open(os.path.join(path, file), "r") as fd:
                content = fd.read()
            fd.close()
            if "FAIL" not in content:
                os.remove(os.path.join(path, file))
                os.remove(os.path.join(path, file.replace("log","err")))
    return


if __name__ == '__main__':
    find_all(r"/home/user/golearn/6.5840/src/raft")
