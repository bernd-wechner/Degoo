###########################################################################
# A further wrapper around the Degoo API functions to provide useful
# utility functions.

import os
import sys
import json
import wget
import time
import magic
import base64
import getpass
import requests
import humanfriendly

from appdirs import user_config_dir
from datetime import datetime
from dateutil.tz import tzutc, tzlocal

from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor
# from clint.textui.progress import Bar as ProgressBar

from .API import API
from .lib import ddd, split_path, absolute_remote_path

###########################################################################
# Get the path to user configuration diectory for this app
conf_dir = user_config_dir("degoo")

# Ensure the user configuration directory exists
if not os.path.exists(conf_dir):
    os.makedirs(conf_dir)

###########################################################################
# Load the current working directory, if available

cwd_file = os.path.join(conf_dir, "cwd.json")

if os.path.isfile(cwd_file):
    with open(cwd_file, "r") as file:
        CWD = json.loads(file.read())
else:
    CWD = ddd(0, "/")

###########################################################################
# Read a schedule file or write one with default schedule if not there.
#
# get and put should have an argument that is optional for respecting a
# schedule and the commands a -s option to respect the schedule thus
# defined.
#
# get and put should sleep outside of the schedule window.
#
# Time formats to be secified to they can be read with:
#
# time.strptime(time_string, "%H:%M:%S")

DEFAULT_SCHEDULE = {  "upload": ("01:00:00", "06:00:00"),
                    "download": ("01:00:00", "06:00:00") }

SCHEDULE = DEFAULT_SCHEDULE

sched_file = os.path.join(conf_dir, "schedule.json")

if os.path.isfile(sched_file):
    with open(sched_file, "r") as file:
        SCHEDULE = json.loads(file.read())
else:
    with open(sched_file, "w") as file:
        file.write(json.dumps(DEFAULT_SCHEDULE))

###########################################################################
# Instantiate an API
api = API()

###########################################################################
# An Error class for Degoo functions to raise if need be


class DegooError(api.Error):
    '''Generic exception to raise and log different fatal errors.'''


###########################################################################
# A local cache of Degoo items and contents, to speed up successive
# queries for them. By convention we have Degoo ID 0 as the root directory
# and the API returns no properties for that so we dummy some up for local
# use to give it the appearance of a root directory.
__CACHE_ITEMS__ = {0: {
                        "ID": 0,
                        "ParentID": None,
                        "Name": "/",
                        "FilePath": "/",
                        "Category": None,
                        "CategoryName": "Root",
                        }
                    }

# A cache of directory contents
__CACHE_CONTENTS__ = {}


def decache(degoo_id):
    '''
    Removed an item from the cahce store, thus forcing it to be refetched from
    Degoo when next needed.

    :param degoo_id: The ID of a degoo item/object
    '''
    __CACHE_ITEMS__.pop(degoo_id, None)
    __CACHE_CONTENTS__.pop(degoo_id, None)

###########################################################################
# Scheduling functions


def wait_until_next(time_of_day, verbose=0):
    '''
    Wait until the specified time. Uses Python sleep() which uses no CPU as rule
    and works on a basis liek Zeno's dichotomy paradox, by sleeping to the half
    way point in successive trials unil we're with 0.1 second of the target.

    Used herein for scheduling uploads and downloads.

    :param time_of_day: A time of day as time.time_struct.
    '''
    now = time.localtime()
    if now < time_of_day:
        today = datetime.datetime.now().date()
        until = datetime.datetime.combine(today, datetime.datetime.fromtimestamp(time.mktime(time_of_day)).time())
    else:
        tomorrow = datetime.datetime.now().date() + datetime.timedelta(days=1)
        until = datetime.datetime.combine(tomorrow, datetime.datetime.fromtimestamp(time.mktime(time_of_day)).time())

    if verbose > 0:
        print(f"Waiting until {until.strftime('%A, %d/%m/%Y %H:%M:%S')}")

    while True:
        diff = (until - datetime.datetime.now()).total_seconds()
        if diff < 0: return  # In case end_datetime was in past to begin with

        if verbose > 1:
            print(f"Waiting for {humanfriendly.format_timespan(diff/2)} seconds")

        time.sleep(diff / 2)
        if diff <= 0.1: return

###########################################################################
# Command functions - these are entry points for the CLI tools


def login(username=None, password=None):
    '''
    Logs the user in
    '''
    if username is None:
        username = input("username: ")
    if password is None:
        password = getpass.getpass()

    return api.login(username, password)


def userinfo():
    '''
    Returns information about the logged in user
    '''
    return api.getUserInfo()


def mkpath(path, verbose=0):
    '''
    Analagous to Linux "mkdir -p", creating all necessary parents along the way.

    :param name: A name or path. If it starts with {os.sep} it's interpreted
                 from root if not it's interpreted from CWD.
    '''
    dirs = split_path(path)

    if dirs[0] == os.sep:
        current_dir = 0
        dirs.pop(0)  # We don't have to make the root dir
    else:
        current_dir = CWD["ID"]

    for d in dirs:
        # If the last char in path is os.sep then the last item in dirs is empty
        if d:
            current_dir = mkdir(d, current_dir, verbose)

    return current_dir


def mkdir(name, parent_id=None, verbose=0, dry_run=False):
    '''
    Makes a Degoo directory/folder or device

    :param name: The name of a directory/folder to make in the CWD or nominated (by id) parent
    :param parent_id: Optionally a the degoo ID of a parent. If not specified the CWD is used.
    '''
    if parent_id == None and "Path" in CWD:
        parent_id = get_item(CWD["Path"]).get("ID", None)

    if parent_id:
        contents = get_children(parent_id)
        existing_names = [f["Name"] for f in contents]
        ids = {f["Name"]: int(f["ID"]) for f in contents}
        if name in existing_names:
            if verbose > 0:
                print(f"{name} already exists")
            return ids[name]
        else:
            if not dry_run:
                ID = api.setUploadFile3(name, parent_id)
            else:
                # Dry run, no ID created
                ID = None

            if verbose > 0:
                print(f"Created directory {name} with ID {ID}")

            return ID
    else:
        raise DegooError("mkdir: No parent_id provided.")


def mv(source, target):
    '''
    Move a file or folder

    :param source: Path to file or folder
    :param target: New path, or name, to move the file or folder
    :return: Message with result of operation
    '''
    if source == target:
        raise DegooError(f"mv: Cannot move {source} to itself")

    source_folder, source_name = os.path.split(source)
    target_folder, target_name = os.path.split(target)

    # If target is a folder move the source into the target folder with same name
    if exists(target):
        if is_folder(target):
            target_folder = target
            target_name = source_name
        else:
            raise DegooError(f"mv: Cannot move {source} to {target} (an existing file)")

    # If no folder is specified use the current working directory
    if not source_folder:
        source_folder = CWD['Path']

    if not target_folder:
        target_folder = CWD['Path']

    # If moving a file in the same directory we need both a source and target filename
    if source_folder == target_folder and (not source_name or not target_name):
        raise DegooError(f"mv: Cannot move {source} to {target}")

    source_id = path_id(source)

    if not source_id:
        raise DegooError(f"mv: '{source}' does not exist on the Degoo drive")

    target_id = path_id(target)

    if target_id:
        # It had better be folder or we can do the move
        if not is_folder(target):
            raise DegooError(f"mv: '{target}' already exists as a file, cannot move {source} there.")
    else:
        # There is a name the target folder is its path (parent)
        if target_name:
            target_id = path_id(target_folder)
            if not target_id:
                target_id = mkpath(target_folder)
        # If target is a folder make sure it exists
        else:
            target_id = mkpath(target)

        if not target_id:
            raise DegooError(f"mv: '{target}' does not exist and could not be created on the Degoo drive")

    if source_folder == target_folder:
        source_id = api.setRenameFile(source_id, target_name)
    else:
        renamed = False
        if not target_name == source_name:
            # This poses a slight problem.
            # We know the new name does not exist in the new folder
            #    That is tested above and we bail as we don't want to overwrite a target file.
            #    TODO: Couldhave a command line flag -f to force such an overwrite.
            # the new name does not exist in the old folder we can rename first then move
            intermediate = os.path.join(source_folder, target_name)
            intermediate_id = path_id(intermediate)
            if not intermediate_id:
                # Rename it before moving
                source_id = api.setRenameFile(source_id, target_name)
                target_id = path_id(target_folder)

                if not (source_id and target_id):
                    raise DegooError(f"mv: Unidentified error trying to move '{source}' to '{intermediate}' in preapration for mooving it to '{target_folder}'")
                else:
                    renamed = True

        source_id = api.setMoveFile(source_id, target_id)

        if not target_name == source_name and not renamed:
            # Means that the intermediate above existed in the source folder
            # So we try with an intermediate in the target folder
            intermediate = os.path.join(target_folder, source_name)
            intermediate_id = path_id(intermediate)
            if not intermediate_id:
                source_id = path_id(intermediate)

                if not (source_id and target_name):
                    raise DegooError(f"mv: Unidentified error trying to move '{source}' to '{intermediate}' in preapration for mooving it to '{target_folder}'")
                else:

                    # Rename it before moving
                    api.setRenameFile(source_id, target_name)

    # Remove it from cache as the cached FilePath is now wrong for this object
    decache(source_id)

    return source_id


def rm(file):
    '''
    Deletes (Removes) a nominated file from the Degoo filesystem.

    Unless the remote server deletes the actual file content from the cloud server this is not
    secure of course. In fact it supports trash and removing the file or folder, moves it to
    the Recycle Bin.

    :param file: Either a string which specifies a file or an int which provides A Degoo ID.
    '''
    file_id = path_id(file)

    if not file_id:
        raise DegooError(f"rm: Illegal file: {file}")

    path = api.getOverlay3(file_id)["FilePath"]
    api.setDeleteFile5(file_id)  # @UnusedVariable

    return path


def cd(path):
    '''
    Change the current working directory (in the Degoo filesystem)

    :param path: an absolute or relative path.
    '''
    CWD = get_dir(path)
    with open(cwd_file, "w") as file:
        file.write(json.dumps(CWD))
    return CWD


def device_names():
    '''
    Returns a dictionary of devices, keyed on Degoo ID, containing the name of the device.

    Top level folders in the Degoo Filesystem are called devices.

    TODO: Degoo's web interface does not currently allow creation of devices even when licensed to.
    Thus we have no way of working out an API call that does so and we're stuck with devices they
    give us (even when licensed to have as many as you like).
    '''
    devices = {}
    root = get_children(0)
    for d in root:
        if d['CategoryName'] == "Device":
            devices[int(d['DeviceID'])] = d['Name']
    return devices


def device_ids():
    '''
    Returns a dictionary of devices, keyed on name, containing the Degoo ID of the device.

    Top level folders in the Degoo Filesystem are called devices.
    '''
    devices = {}
    root = get_children(0)
    for d in root:
        if d['CategoryName'] == "Device":
            devices[d['Name']] = int(d['DeviceID'])
    return devices


def get_dir(path=None):
    '''
    Returns a Degoo Directory Dictionary (ddd) for the specified directory.

    Is impartial actually, and works for Files and Folders alike.

    :param path: The path (absolute or relative) of a Degoo item. If not specified returns the current working directory.
    '''
    if path:
        item = get_item(path)
    else:
        # Trust the CWD Path more than the ID
        # A known weakness is if we delete a folder and recreate
        # it the web interface it's recreated with a new ID. By checking
        # the path and getting the ID we confirm it's real.
        item = get_item(CWD['Path'])
    return ddd(item["ID"], item["FilePath"])


def get_parent(degoo_id):
    '''
    Given the Degoo ID returns the Degoo Directory Dictionary (ddd) for the parent directory.

    :param degoo_id: The Degoo ID of an item.
    '''
    try:
        parent = get_item(degoo_id)["ParentID"]
    except:
        parent = None

    if not parent is None:
        try:
            props = get_item(parent)
            return ddd(props.get("ID", None), props.get("FilePath", None))
        except:
            return None
    else:
        return None


def path_str(degoo_id):
    '''
    Returns the FilePath property of a Degoo item.

    :param degoo_id: The Degoo ID of the item.
    '''
    try:
        return get_item(degoo_id)["FilePath"]
    except:
        return None


def parent_id(degoo_id):
    '''
    Returns the Degoo ID of the parent of a Degoo Item.

    :param degoo_id: The Degoo ID of the item concerned.
    '''
    try:
        return get_item(degoo_id)["ParentID"]
    except:
        return None


def path_id(path):
    '''
    Returns the Degoo ID of the object at path (Folder or File, or whatever).

    If an int is passed just returns that ID but if a str is passed finds the ID and returns it.

    if no path is specified returns the ID of the Current Working Directory (CWD).

    :param path: An int or str or None (which ask for the current working directory)
    '''
    try:
        return get_item(path)["ID"]
    except:
        return None


def exists(path):
    '''
    Does the remote path exist on the Degoo drive?

    :param path: An int or str or None (which ask for the current working directory)
    '''
    return not path_id(path) is None


def is_folder(path):
    '''
    Returns true if the remote Degoo item referred to by path is a Folder

    :param path: An int or str or None (for the current working directory)
    '''
    try:
        return get_item(path)["CategoryName"] in api.folder_types
    except:
        return False


def get_item(path=None, verbose=0, recursive=False):
    '''
    Return the property dictionary representing a nominated Degoo item.

    :param path: An int or str or None (for the current working directory)
    '''

    def props(degoo_id):
        # The root is special, it returns no properties from the degoo API
        # We dummy some up for internal use:
        if degoo_id not in __CACHE_ITEMS__:
            __CACHE_ITEMS__[degoo_id] = api.getOverlay3(degoo_id)

        return __CACHE_ITEMS__[degoo_id]

    if path is None:
        if CWD:
            path = CWD["ID"]  # Current working directory if it exists
        else:
            path = 0  # Root directory if nowhere else
    elif isinstance(path, str):
        abs_path = absolute_remote_path(CWD, path)

        paths = {item["FilePath"]: item for _, item in __CACHE_ITEMS__.items()}  # @ReservedAssignment

        if not recursive and abs_path in paths:
            return paths[abs_path]
        else:
            parts = split_path(abs_path)  # has no ".." parts thanks to absolute_remote_path

            if parts[0] == os.sep:
                part_id = 0
                parts.pop(0)
            else:
                part_id = CWD["ID"]

            for p in parts:
                if verbose > 1:
                    print(f"get_item: getting children of {part_id} hoping for find {p}")

                contents = get_children(part_id)
                ids = {f["Name"]: int(f["ID"]) for f in contents}
                if p in ids:
                    part_id = ids[p]
                else:
                    raise DegooError(f"{p} does not exist in {path_str(part_id)}")

            # Now we have the item ID we can call back with an int part_id.
            return get_item(part_id, verbose, recursive)

    # If recursing we pass in a prop dictionary
    elif recursive and isinstance(path, dict):
        path = path.get("ID", 0)

    if isinstance(path, int):
        item = props(path)

        if recursive:
            items = {item["FilePath"]: item}

            if item["CategoryName"] in api.folder_types:
                if verbose > 1:
                    print(f"Recursive get_item descends to {item['FilePath']}")

                children = get_children(item)

                if verbose > 1:
                    print(f"\tand finds {len(children)} children.")

                for child in children:
                    if child["CategoryName"] in api.folder_types:
                        items.update(get_item(child, verbose, recursive))
                    else:
                        items[child["FilePath"]] = child
            elif verbose > 1:
                print(f"Recursive get_item stops at {item['FilePath']}. Category: {item['CategoryName']}")

            return items
        else:
            return item
    else:
        raise DegooError(f"Illegal path: {path}")


def get_children(directory=None):
    '''
    Returns a list of children (as a property dictionary) of a dominated directory.

    :param directory: The path (absolute of relative) of a Folder item,
                        the property dictionary representing a Degoo Folder or
                        None for the current working directory,

    :returns: A list of property dictionaries, one for each child, contianing the properties of that child.
    '''
    if directory is None:
        if CWD["ID"]:
            dir_id = CWD["ID"]
        else:
            dir_id = 0
    elif isinstance(directory, dict):
        dir_id = directory.get("ID", 0)
    elif isinstance(directory, int):
        dir_id = directory
    elif isinstance(directory, str):
        dir_id = path_id(directory)
    else:
        raise DegooError(f"get_children: Illegal directory: {directory}")

    if dir_id not in __CACHE_CONTENTS__:
        __CACHE_CONTENTS__[dir_id] = api.getFileChildren3(dir_id)
        # Having the props of all children we cache those too
        # Can overwrite existing cache as this fetch is more current anyhow
        for item in __CACHE_CONTENTS__[dir_id]:
            __CACHE_ITEMS__[item["ID"]] = item

    return __CACHE_CONTENTS__[dir_id]


def has_changed(local_filename, remote_path, verbose=0):
    '''
    Determines if a local local_filename has changed since last upload.

    :param local_filename: The local local_filename ((full or relative remote_path)
    :param remote_path:    The Degoo path it was uploaded to (can be a Folder or a File, either relative or abolute remote_path)
    :param verbose:        Print useful tracking/diagnostic information

    :returns: True if local local_filename has chnaged since last upload, false if not.
    '''
    # We need the local local_filename name, size and last modification time
    Name = os.path.basename(local_filename)
    Size = os.path.getsize(local_filename)
    LastModificationTime = datetime.fromtimestamp(os.path.getmtime(local_filename)).astimezone(tzlocal())

    # Get the files' properties either from the folder it's in or the file itself
    # Depending on what was specified in remote_path (the containing folder or the file)
    if is_folder(remote_path):
        files = get_children(remote_path)

        sizes = {f["Name"]: int(f["Size"]) for f in files}
        times = {f["Name"]: int(f["LastUploadTime"]) for f in files}

        if Name in sizes:
            Remote_Size = sizes[Name]
            LastUploadTime = datetime.utcfromtimestamp(int(times[Name]) / 1000).replace(tzinfo=tzutc()).astimezone(tzlocal())
        else:
            Remote_Size = 0
            LastUploadTime = datetime.utcfromtimestamp(0).replace(tzinfo=tzutc()).astimezone(tzlocal())
    else:
        props = get_item(remote_path)

        if props:
            Remote_Size = props["Size"]
            LastUploadTime = datetime.utcfromtimestamp(int(props["LastUploadTime"]) / 1000).replace(tzinfo=tzutc()).astimezone(tzlocal())
        else:
            Remote_Size = 0
            LastUploadTime = datetime.utcfromtimestamp(0).replace(tzinfo=tzutc()).astimezone(tzlocal())

    if verbose > 0:
        print(f"{local_filename}: ")
        print(f"\tLocal size: {Size}")
        print(f"\tRemote size: {Remote_Size}")
        print(f"\tLast Modified: {LastModificationTime}")
        print(f"\tLast Uploaded: {LastUploadTime}")

    # We only have size and upload time available at present
    # TODO: See if we can coax the check_sum we calculated for upload out of Degoo for testing again against the local check_sum.
    return Size != Remote_Size or LastModificationTime > LastUploadTime


def get_file(remote_file, local_directory=None, verbose=0, if_missing=False, dry_run=False, schedule=False):
    '''
    Downloads a specified remote_file from Degoo.

    :param remote_file: An int or str or None (for the current working directory)
    :param local_directory: The local directory into which to drop the downloaded file
    :param verbose:    Print useful tracking/diagnostic information
    :param if_missing: Only download the remote_file if it's missing locally (i.e. don't overwrite local files)
    :param dry_run:    Don't actually download the remote_file ...
    :param schedule:   Respect the configured schedule (i.e download only when schedule permits)

    :returns: the FilePath property of the downloaded remote_file.
    '''
    if schedule:
        window = SCHEDULE["download"]
        window_start = time.strptime(window[0], "%H:%M:%S")
        window_end = time.strptime(window[1], "%H:%M:%S")
        now = time.localtime()

        in_window = now > min(window_start, window_end) and now < max(window_start, window_end)

        if ((window_start < window_end and not in_window)
        or  (window_start > window_end and in_window)):
            wait_until_next(window_start, verbose)

    item = get_item(remote_file)

    # If we landed here with a directory rather than remote_file, just redirect
    # to the approproate downloader.
    if item["CategoryName"] in api.folder_types:
        return get_directory(remote_file)

    # Try the Optimized URL first I guess
    URL = item.get("OptimizedURL", None)
    if not URL:
        URL = item.get("URL", None)

    Name = item.get("Name", None)
    Path = item.get("FilePath", None)
    Size = item.get('Size', 0)
    Data = item.get('Data', None)

    # Remember the current working directory
    cwd = os.getcwd()

    if local_directory is None:
        dest_file = os.path.join(os.getcwd(), Name)
    elif os.path.isdir(local_directory):
        os.chdir(local_directory)
        dest_file = os.path.join(local_directory, Name)
    else:
        raise DegooError(f"get_file: '{local_directory}' is not a directory.")

    if URL and Name:
        if not if_missing or not os.path.exists(dest_file):
            # We use wget which outputs a progress bar to stdout.
            # This is the only method that writes to stdout because wget does so we do.
            if verbose > 0:
                if dry_run:
                    print(f"Would download {Path} to {dest_file}")
                else:
                    print(f"Downloading {Path} to {dest_file}")

            # Note:
            #
            # This relies on a special version of wget at:
            #
            #    https://github.com/bernd-wechner/python3-wget
            #
            # which has an upstream PR are:
            #
            #    https://github.com/jamiejackherer/python3-wget/pull/4
            #
            # wget has a bug and while it has a nice progress bar it renders garbage
            # for Degoo because the download source fails to set the content-length
            # header. We know the content length from the Degoo metadata though and so
            # can specify it manually.
            #
            # The Degoo API also rejects the User-Agent that urllib provides by default
            # and we need to set one it accepts. Anything works in fact just not the one
            # python-urllib uses, which seems to be blacklisted.

            if not dry_run:
                try:
                    wget.download(URL, out=Name, size=Size, headers={'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/83.0.4103.61 Chrome/83.0.4103.61 Safari/537.36"})
                except Exception as e:
                    # I have seen a 302 reported as follows:
                    #     Exception: 302: Moved Temporarily
                    #     degoo_get: 302: Moved Temporarily
                    # Don't know from that the exception type, so addded a report here to catch it if it happens again
                    # and report the tuype, so we can look up how to handle them. A 302 shoudl be accompanied by a new URL,
                    # and conceivable just try wget again here with that new URL. Getting that new URL from an exception is
                    # the challenge, and so here is the frist step, to report the exception type to do some reading on that.
                    #
                    # Alas retrying the wget that failed above did not produce a 302, so it's not reliably reproducible,
                    # But it is a legitimate repsonse and we shoudl handle it properly.

                    print(f"wget Exception of type: {type(e)}")
                    print(f"\t{str(e)}")

                # The default wqet progress bar leaves cursor at end of line.
                # It fails to print a new line. This causing glitchy printing
                # Easy fixed,
                print("")

            # Having downloaded the file chdir back to where we started
            os.chdir(cwd)

            return item["FilePath"]
        else:
            if verbose > 1:
                if dry_run:
                    print(f"Would NOT download {Path}")
                else:
                    print(f"Not downloading {Path}")

    elif Data and Name:
        decoded_content = base64.b64decode(Data).decode("utf-8")
        with open(dest_file, "w") as text_file:
            text_file.write(decoded_content)

    else:
        raise DegooError(f"{Path} apparantly has no URL to download from.")


def get_directory(remote_folder, local_directory=None, verbose=0, if_missing=False, dry_run=False, schedule=False):
    '''
    Downloads a Directory and all its contents (recursively).

    :param remote_folder: An int or str or None (for the current working directory)
    :param local_directory: The local directory into which to drop the downloaded folder
    :param verbose:    Print useful tracking/diagnostic information
    :param if_missing: Only download files missing locally (i.e don't overwrite local files)
    :param dry_run:    Don't actually download the file ...
    :param schedule:   Respect the configured schedule (i.e download only when schedule permits)
    '''
    item = get_item(remote_folder)

    # If we landed here with a file rather than folder, just redirect
    # to the approproate downloader.
    if not item["CategoryName"] in api.folder_types:
        return get_file(remote_folder)

    dir_id = item['ID']

    # Remember the current working directory
    cwd = os.getcwd()

    if local_directory is None:
        pass  # all good, just use cwd
    elif os.path.isdir(local_directory):
        os.chdir(local_directory)
    else:
        raise DegooError(f"get_file: '{local_directory}' is not a directory.")

    # Make the target direcory if needed
    try:
        os.mkdir(item['Name'])
    except FileExistsError:
        # Not a problem if the remote folder already exists
        pass

    # Step down into the new directory for the downloads
    os.chdir(item['Name'])

    # Fetch and classify all Degoo drive contents of this remote folder
    children = get_children(dir_id)

    files = [child for child in children if not child["CategoryName"] in api.folder_types]
    folders = [child for child in children if child["CategoryName"] in api.folder_types]

    # Download files
    for f in files:
        try:
            get_file(f['ID'], local_directory, verbose, if_missing, dry_run, schedule)

        # Don't stop on a DegooError, report it but keep going.
        except DegooError as e:
            if verbose > 0:
                print(e, file=sys.stderr)

    # Make the local folders and download into them
    for f in folders:
        get_directory(f['ID'], local_directory, verbose, if_missing, dry_run, schedule)

    # Having downloaded all the items in this remote folder chdir back to where we started
    os.chdir(cwd)


def get(remote_path, local_directory=None, verbose=0, if_missing=False, dry_run=False, schedule=False):
    '''
    Downloads a file or folder from Degoo.

    :param remote_path: The file or folder (a degoo path) to download
    :param local_directory: The local directory into which to drop the download
    :param verbose:    Print useful tracking/diagnostic information
    :param if_missing: Only download the file if it's missing (i.e. don't overwrite local files)
    :param dry_run:    Don't actually download the file ...
    :param schedule:   Respect the configured schedule (i.e download only when schedule permits)
    '''
    item = get_item(remote_path)

    if item["CategoryName"] in api.folder_types:
        return get_directory(item['ID'], local_directory, verbose, if_missing, dry_run, schedule)
    else:
        return get_file(item['ID'], local_directory, verbose, if_missing, dry_run, schedule)


def put_file(local_file, remote_folder, verbose=0, if_changed=False, dry_run=False, schedule=False):
    '''
    Uploads a local_file to the Degoo cloud store.

    :param local_file:     The local file ((full or relative remote_folder)
    :param remote_folder:  The Degoo folder upload it to (must be a Folder, either relative or abolute path)
    :param verbose:        Print useful tracking/diagnostic information
    :param if_changed:     Only upload the local_file if it's changed
    :param dry_run:        Don't actually upload the local_file ...
    :param schedule:       Respect the configured schedule (i.e upload only when schedule permits)

    :returns: A tuple containing the Degoo ID, Remote file path and the download URL of the local_file.
    '''

    def progress(monitor):
        return wget.callback_progress(monitor.bytes_read, 1, monitor.encoder.len, wget.bar_adaptive)

    if schedule:
        window = SCHEDULE["upload"]
        window_start = time.strptime(window[0], "%H:%M:%S")
        window_end = time.strptime(window[1], "%H:%M:%S")
        now = time.localtime()

        in_window = now > min(window_start, window_end) and now < max(window_start, window_end)

        if ((window_start < window_end and not in_window)
        or  (window_start > window_end and in_window)):
            wait_until_next(window_start, verbose)

    dest = get_item(remote_folder)
    dir_id = dest["ID"]
    dir_path = dest["FilePath"]

    if not is_folder(dir_id):
        raise DegooError(f"put_file: {remote_folder} is not a remote folder!")

    if verbose > 1:
        print(f"Asked to upload {local_file} to {dir_path}: {if_changed=} {dry_run=}")

    # Upload only if:
    #    if_changed is False and dry_run is False (neither is true)
    #    if_changed is True and has_changed is true and dry_run is False
    if (not if_changed or has_changed(local_file, remote_folder, verbose - 1)):
        if dry_run:
            if verbose > 0:
                print(f"Would upload {local_file} to {dir_path}")
        else:
            if verbose > 0:
                print(f"Uploading {local_file} to {dir_path}")
            # The steps involved in an upload are 4 and as follows:
            #
            # 1. Call getBucketWriteAuth4 to get the URL and parameters we need for upload
            # 2. Post the actual file to the BaseURL provided by that
            # 3. Call setUploadFile3 to inform Degoo it worked and create the Degoo item that maps to it
            # 4. Call getOverlay3 to fetch the Degoo item this created so we can see that it worked (and return the download URL)

            MimeTypeOfFile = magic.Magic(mime=True).from_file(local_file)

            #################################################################
            # # STEP 1: getBucketWriteAuth4

            # Get the Authorisation to write to this directory
            # Provides the metdata we need for the upload
            result = api.getBucketWriteAuth4(dir_id)

            #################################################################
            # # STEP 2: POST to BaseURL

            # Then upload the local_file to the nominated URL
            BaseURL = result["AuthData"]["BaseURL"]

            # We now POST to BaseURL and the body is the local_file but all these fields too
            Signature = result["AuthData"]["Signature"]
            GoogleAccessId = result["AuthData"]["AccessKey"]["Value"]
            CacheControl = result["AuthData"]["AdditionalBody"][0]["Value"]  # Only one item in list not sure why indexed
            Policy = result["AuthData"]["PolicyBase64"]
            ACL = result["AuthData"]["ACL"]
            KeyPrefix = result["AuthData"]["KeyPrefix"]  # Has a trailing /

            # This one is a bit mysterious. The Key seems to be made up of 4 parts
            # separated by /. The first two are provided by getBucketWriteAuth4 as
            # as the KeyPrefix, the next appears to be the local_file extension, and the
            # last an apparent filename that is consctucted as checksum.extension.
            # Odd, to say the least.
            Type = os.path.splitext(local_file)[1][1:]
            Checksum = api.check_sum(local_file)

            if Type:
                Key = "{}{}/{}.{}".format(KeyPrefix, Type, Checksum, Type)
            else:
                # TODO: When there is no local_file extension, the Degoo webapp uses "unknown"
                # This requires a little more testing. It seems to work with any value.
                Key = "{}{}/{}.{}".format(KeyPrefix, "@", Checksum, "@")

            # We need filesize
            Size = os.path.getsize(local_file)

            # Now upload the local_file
            parts = [
                ('key', (None, Key)),
                ('acl', (None, ACL)),
                ('policy', (None, Policy)),
                ('signature', (None, Signature)),
                ('GoogleAccessId', (None, GoogleAccessId)),
                ('Cache-control', (None, CacheControl)),
                ('Content-Type', (None, MimeTypeOfFile)),
                ('file', (os.path.basename(local_file), open(local_file, 'rb'), MimeTypeOfFile))
            ]

            # Perform the upload
            multipart = MultipartEncoder(fields=dict(parts))
            monitor = MultipartEncoderMonitor(multipart, progress)

            heads = {"ngsw-bypass": "1", "content-type": multipart.content_type, "content-length": str(multipart.len)}

            response = requests.post(BaseURL, data=monitor, headers=heads)

            # A new line after the progress bar is complete
            print()

            # We expect a 204 status result, which is silent acknowledgement of success.
            if response.ok and response.status_code == 204:
                # Theres'a Google Upload ID returned in the headers. Not sure what use it is.
                # google_id = response.headers["X-GUploader-UploadID"]

                if verbose > 1:
                    print("Google Response Headers:")
                    for h in response.headers:
                        print(f"\t{h}: {response.headers[h]}")
                    print("Google Response Content:")
                    if response.content:
                        print(json.dumps(json.loads(response.content), indent=4))
                    else:
                        print("\tNothing, Nil, Nada, Empty")
                    print("")

#                 # Empirically the download URL seems fairly predictable from the inputs we have.
#                 # with two caveats:
#                 #
#                 # The expiry time is a little different. It's 14 days from now all right but
#                 # now being when setUploadFile3 on the server has as now and not the now we have
#                 # here.
#                 #
#                 # The Signature is new, it's NOT the Signature that getBucketWriteAuth4 returned
#                 # nor any obvious variation upon it (like base64 encoding)
#                 #
#                 # After some testing it's clear that the signature is a base64 encoded signature
#                 # and is generated using a Degoo private key from the URL, which we can't predict.
#                 #
#                 # In fact Google's feedback on a faulty signature is:
#                 # <Error>
#                 #     <Code>SignatureDoesNotMatch</Code>
#                 #     <Message>
#                 #         The request signature we calculated does not match the signature you provided. Check your Google secret key and signing method.
#                 #     </Message>
#                 #     <StringToSign>
#                 #         GET 1593121729 /degoo-production-large-local_file-us-east1.degoo.me/gCkuIp/tISlDA/ChT/gXKXPh2ULNAtufkHfMQ+hE0CSRAA
#                 #     </StringToSign>
#                 # </Error>
#                 #
#                 # That is the Signature provided needs signing using the Degoo private key.
#                 #
#                 # I'd bet that setUploadFile3 given he checksum can build that string and
#                 # using the Degoo private key generate a signature. But alas it doestn't
#                 # return one and so we need to use getOverlay3 to fetch it explicitly.
#                 expiry = str(int((datetime.utcnow() + timedelta(days=14)).timestamp()))
#                 expected_URL = "".join([
#                             BaseURL.replace("storage-upload.googleapis.com/",""),
#                             Key,
#                             "?GoogleAccessId=", GoogleAccessId,
#                             "&Expires=", expiry,
#                             "&Signature=", Signature,
#                             "&use-cf-cache=true"]) # @UnusedVariable

                #################################################################
                # # STEP 3: setUploadFile3

                degoo_id = api.setUploadFile3(os.path.basename(local_file), dir_id, Size, Checksum)

                #################################################################
                # # STEP 4: getOverlay3

                props = api.getOverlay3(degoo_id)

                Path = props['FilePath']
                URL = props['URL']

#                 if not URL:
#                     if verbose>0:
#                         print("EXPERIMENT: Trying fallback URL.")
#                     # This won't work!
#                     URL = expected_URL

                return (degoo_id, Path, URL)
            else:
                raise DegooError(f"Upload failed with: Failed with: {response}")
    else:
        if dry_run and verbose:
            print(f"Would NOT upload {local_file} to {dir_path} as it has not changed since last upload.")

        children = get_children(dir_id)
        props = {child['Name']: child for child in children}

        filename = os.path.basename(local_file)
        if filename in props:
            ID = props[filename]['ID']
            Path = props[filename]['FilePath']
            URL = props[filename]['URL']

        return (ID, Path, URL)


def put_directory(local_directory, remote_folder, verbose=0, if_changed=False, dry_run=False, schedule=False):
    '''
    Uploads a local directory recursively to the Degoo cloud store.

    :param local_directory: The local directory (full or relative remote_folder)
    :param remote_folder:    The Degoo folder to upload it to (must be a Folder, either relative or abolute path)
    :param verbose: Print useful tracking/diagnostic information
    :param if_changed: Uploads only files changed since last upload
    :param dry_run: Don't actually upload anything ...
    :param schedule:   Respect the configured schedule (i.e upload only when schedule permits)

    :returns: A tuple containing the Degoo ID and the Remote file path
    '''
    IDs = {}

    target_dir = get_dir(remote_folder)
    (target_junk, target_name) = os.path.split(local_directory)

    Root = target_name
    IDs[Root] = mkdir(target_name, target_dir['ID'], verbose - 1, dry_run)

    for root, dirs, files in os.walk(local_directory):
        # if local directory contains a head that is included in root then we don't want
        # it when we're making dirs on the remote (cloud) drive and remembering the
        # IDs of those dirs.
        if target_junk:
            relative_root = root.replace(target_junk + os.sep, "", 1)
        else:
            relative_root = root

        for name in dirs:
            Name = os.path.join(root, name)

            IDs[Name] = mkdir(name, IDs[relative_root], verbose - 1, dry_run)

        for name in files:
            Name = os.path.join(root, name)

            put_file(Name, IDs[relative_root], verbose, if_changed, dry_run, schedule)

    # Directories have no download URL, they exist only as Degoo metadata
    return (IDs[Root], target_dir["Path"])


def put(local_path, remote_folder, verbose=0, if_changed=False, dry_run=False, schedule=False):
    '''
    Uplads a file or folder to the Degoo cloud store

    :param local_path: The path (absolute or relative) of a local file or folder
    :param remote_folder: The Degoo path to upload it to (must be a Folder, either relative or absolute path)
    :param verbose: Print useful tracking/diagnostic information
    :param if_changed: Uploads only files changed since last upload
    :param schedule:   Respect the configured schedule (i.e upload only when schedule permits)
    '''
    isFile = os.path.isfile(local_path)
    isDirectory = os.path.isdir(local_path)

    if isDirectory:
        return put_directory(local_path, remote_folder, verbose, if_changed, dry_run, schedule)
    elif isFile:
        return put_file(local_path, remote_folder, verbose, if_changed, dry_run, schedule)
    else:
        return None

###########################################################################
# Text output functions
#
# Functions above here should not output text
# (excepting error messages to stderr and verbose output to stdout)


def ls(directory=None, long=False, recursive=False):
    if recursive:
        props = get_item(directory)
        print(f"{props['FilePath']}:")

    items = get_children(directory)

    for i in items:
        if long:
            times = f"c:{i['Time_Created']}\tm:{i['Time_LastModified']}\tu:{i['Time_LastUpload']}"
            print(f"{i['ID']}\t{i['CategoryName']:{api.CATLEN}s}\t{i['Name']:{api.NAMELEN}s}\t{times}")
        else:
            print(f"{i['Name']}")

    if recursive:
        print('')
        for i in items:
            if i['CategoryName'] in api.folder_types:
                ls(i['ID'], long, recursive)


def tree(dir_id=0, show_times=False, _done=[]):
    T = "├── "
    I = "│   "
    L = "└── "
    E = "    "

    # Print name of the root item in the tree
    if not _done:
        props = get_item(dir_id)
        name = props.get("FilePath", "")
        print(name)

    kids = get_children(dir_id)

    if kids:
        last_id = kids[-1]['ID']
        for kid in kids:
            ID = kid['ID']
            name = kid.get("Name", "")
            cat = kid.get("CategoryName", kid.get("Category", None))

            postfix = ""
            if show_times:
                postfix = f" (c:{kid['Time_Created']}, m:{kid['Time_LastModified']}, u:{kid['Time_LastUpload']})"

            prefix = "".join([E if d else I for d in _done])
            prefix = prefix + (L if ID == last_id else T)

            print(prefix + name + postfix)

            if cat in api.folder_types:
                new_done = _done.copy()
                new_done.append(ID == last_id)
                tree(ID, show_times, new_done)

###########################################################################
# A Test hook


def test():
    localfile = "/home/bernd/workspace/Degoo/test_data/Image/Image 2.jpg"
    remotepath = "/Web/Test2"
    return has_changed(localfile, remotepath, verbose=1)
#     api.getSchema()

#     device_id = device_ids()["Web"]
#     path = ""
#     api.getFilesFromPaths(device_id, path)
