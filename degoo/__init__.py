#!/usr/bin/python3
# encoding: utf-8
'''
Degoo API -- An API interface to interact with a Degoo cloud drive

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
import os

from appdirs import user_config_dir

# A string to prefix CLI commands with (configurable, and used by
# build.py     - to make the commands and,
# commands.py  - to implement the commands
command_prefix = "degoo_"

###########################################################################
# Get the path to user configuration diectory for this app
conf_dir = user_config_dir("degoo")

# Ensure the user configuration directory exists
if not os.path.exists(conf_dir):
    os.makedirs(conf_dir)

###########################################################################
# Import useful util functions to the package level

from .util import login, CWD, cd, ls, tree, mkpath, rm, mv, get_item, get_dir, get, put, userinfo, test
