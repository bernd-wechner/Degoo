###########################################################################
# Library functions supporting the Degoo CLI package

import os


def ddd(ID, Path):
    '''
    A Degoo Directory Dictionary (ddd).

    A convenient way to represent a current working directory so it can be
    saved, restored and communicated.

    :param ID:    The Degoo ID of a a Degoo Item
    :param Path:  The Path of that same Item
    '''
    return {"ID": ID, "Path": Path}


def split_path(path):
    '''
    Given a path string, splits it into a list of elements.

    os.sep is used to split the path and appears in none of the elements,
    with the exception of parts[0] which is equal to os.sep for an absolute
    path and not for relative path.

    :param path:  A file path string
    '''
    allparts = []
    while 1:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            allparts.insert(0, parts[0])
            break
        elif parts[1] == path:  # sentinel for relative paths
            allparts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            allparts.insert(0, parts[1])
    return allparts


def absolute_remote_path(CWD, path):
    '''
    Convert a given path string to an absolute one (if it's relative).

    :param path: The path to convert.
    :returns: The absolute version of path
    '''
    if path and path[0] == os.sep:
        return os.path.normpath(path.rstrip(os.sep) if len(path.strip()) > 1 else path)
    else:
        return os.path.normpath(os.path.join(CWD["Path"], path.rstrip(os.sep)))
