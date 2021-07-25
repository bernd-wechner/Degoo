#!/usr/bin/python3
# encoding: utf-8
'''
Degoo commands -- A builder for command line tools to interact with Degoo

Degoo lack a command line client, they lack a Linux client, but they
expose a GraphQL API which their web app (and probably their phone app)
communicate with. This is a reverse engineering based on observations of
those communications aand a Python client implementation.

@author:     Bernd Wechner

@copyright:  2020. All rights reserved.

@license:    The Hippocratic License 2.1

@contact:    YndlY2huZXJAeWFob28uY29t    (base64 encoded)
@deffield    updated: Updated
'''
import degoo
import os
import stat

source = "commands.py"
commands = ["cd", "get", "ll", "login", "ls", "mkdir", "path", "props", "put", "pwd", "rm", "mv", "tree", "user", "test"]

# Make sure the os. functions have the script dir as their working directory
cwd = os.path.dirname(os.path.abspath(__file__))
os.chdir(cwd)

if os.getcwd() == cwd:
    Commands = ['d'] + [degoo.command_prefix + c for c in commands]

    for c in Commands:
        try:
            os.link(source, c)
            os.chmod(c, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH);
        except FileExistsError:
            pass

    # TODO: Copy default_properties to config dir.
    # TODO: Create test_data folder if it doesn't exist
else:
    print("Weirdness happened!")
