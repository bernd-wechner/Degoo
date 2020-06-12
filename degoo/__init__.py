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
from appdirs import user_config_dir
from urllib import request
from dateutil import parser
import os, csv, json, time, datetime, requests, wget, magic, humanize, hashlib, base64
from genericpath import isfile

# An Error class for Degoo functions to raise if need be
class DegooError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super().__init__(type(self))
        self.msg = msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

# URLS

# A string to rpefix CLI commands with
command_prefix = "degoo_"

URL_login = "https://api.degoo.com/v1/production/login"
URL_API   = "https://production-appsync.degoo.com/graphql"

# Local config and state files
cred_file  = os.path.join(user_config_dir("degoo"), "credentials.json")
keys_file  = os.path.join(user_config_dir("degoo"), "keys.json")
cwd_file   = os.path.join(user_config_dir("degoo"), "cwd.json")
DP_file    = os.path.join(user_config_dir("degoo"), "default_properties.txt")

__CACHE_ITEMS__ = {}

###########################################################################
# Support functions

def fsi_dict(ID, Path):
    return {"ID": ID, "Path": Path}

###########################################################################
# Load the local config and state

CWD = fsi_dict(0, "/")
if os.path.isfile(cwd_file):
    with open(cwd_file, "r") as file:
        CWD = json.loads(file.read())

###########################################################################
# Logging in is a prerequisite to using the API (a pre API step). The 
# login function reads from the configure cred_file and writes keys the
# API needs to the keys_file.

def login():
    CREDS = {}
    if os.path.isfile(cred_file):
        with open(cred_file, "r") as file:
            CREDS = json.loads(file.read())
    
    if CREDS:
        response = requests.post(URL_login, data=json.dumps(CREDS))
        
        if response.ok:
            rd = json.loads(response.text)
            
            keys = {"Token": rd["Token"], "x-api-key": api.API_KEY}
        
            with open(keys_file, "w") as file:
                file.write(json.dumps(keys))
        
            print("Successfuly logged in.")
    else:
        with open(cred_file, "w") as file:
            file.write(json.dumps({"Username": "<your Degoo username here>", "Password": "<your Degoo password here>"}))
            
        print("No login credentials available. Please add account details to {cred_file}")

###########################################################################
# Bundle all the API interactions into an API class

class API:
    # Empirically determined, largest value degoo supports for the Limit 
    # on the Limit paramater to the GetFileChildren3 operation. It's used
    # for paging, and if more items exist there'll be a NextToken returned.
    # TODO: Determine the syntax and use of that NextToken.
    LIMIT_MAX = int('1'*31, 2)-1

    # This appears to be an invariant key that the API expects in the header
    # x-api-key:
    API_KEY = "da2-vs6twz5vnjdavpqndtbzg3prra"
    
    # Keys needed to interact with the API. Provided during login.
    KEYS = None
    
    # Known Degoo API categegories
    CATS = {  0: "Error",
              1: "Device",
              2: "Folder",
              3: "Image",
              4: "Video",
              5: "Music",
              6: "Document",
             10: "Recycle Bin",
           }
    
    folder_types = ["Folder", "Device", "Recycle Bin"]
    
    # A guess at the plans available
    PLANS = { 0: "Free 100 GB",
              1: "Pro 500 GB",
              2: "Ultimate 10 TB",
              3: "Ultimate Stackcommerce offer 10 TB"
            }
    
    # Width of a Category field in output
    CATLEN = 10
    
    # Width of Name field for the last call to getFileChildren3
    NAMELEN = 20
    
    PROPERTIES = ""
    
    def __init__(self):
        keys = {}
        if os.path.isfile(keys_file):
            with open(keys_file, "r") as file:
                keys = json.loads(file.read())
                
        self.KEYS = keys
        
        if os.path.isfile(DP_file):
            with open(DP_file, "r") as file:
                self.PROPERTIES = file.read()     
                
        self.CATLEN = max([len(n) for _,n in self.CATS.items()]) 
    
    def _human_readable_times(self, creation, modification, upload):
        # Add a set of Human Readable timestamps
        c_time = creation
        m_secs = int(modification)/1000
        u_secs = int(upload)/1000
        
        c_datetime = parser.parse(c_time)
        m_datetime = datetime.datetime.utcfromtimestamp(m_secs)
        u_datetime = datetime.datetime.utcfromtimestamp(u_secs)

        date_format = "%Y-%m-%d %H:%M:%S"
        c_dt = c_datetime.strftime(date_format)
        m_dt = m_datetime.strftime(date_format)
        u_dt = u_datetime.strftime(date_format)
        
        return (c_dt, m_dt, u_dt)        
    
    def check_sum(self, filename, blocksize=65536):
        Seed = bytes([13, 7, 2, 2, 15, 40, 75, 117, 13, 10, 19, 16, 29, 23, 3, 36])
        Hash = hashlib.sha1(Seed)
        with open(filename, "rb") as f:
            for block in iter(lambda: f.read(blocksize), b""):
                Hash.update(block)
                
        cs = list(bytearray(Hash.digest()))
        
        # On our test file We now have:
        # [82, 130, 147, 14, 109, 84, 251, 153, 64, 39, 135, 7, 81, 9, 21, 80, 203, 120, 35, 150]
        # and need to encode this to:
        # [10, 20, 82, 130, 147, 14, 109, 84, 251, 153, 64, 39, 135, 7, 81, 9, 21, 80, 203, 120, 35, 150, 16, 0]
        # Which is four bytes longer, prepended by a word and appended by a word.
        # JS code inspection is non conclusive, it's well obfuscated Webpack JS.
        # But hypthesis is:
        #
        # 10, len(checksum), checksum, 16, type
        # And type is always for file uploads.
        CS = [10, len(cs)] + cs + [16, 0]
        
        # Finally, Degoo base64 encode is cehcksum.
        checksum = base64.b64encode(bytes(CS)).decode()
        
        return  checksum    
    
    def getUserInfo(self, humanise=True):
        args = "\n".join(["Name", "Email", "Phone", "AvatarURL", "AccountType", "UsedQuota", "TotalQuota", "__typename"])
        func = f"getUserInfo(Token: $Token) {{ {args} }}"
        query = f"query GetUserInfo($Token: String!) {{ {func} }}"
        
        request = { "operationName": "GetUserInfo",
                    "variables": { "Token": self.KEYS["Token"] },
                    "query": query
                   }
        
        header = {"x-api-key": self.KEYS["x-api-key"]}
        
        response = requests.post(URL_API, headers=header, data=json.dumps(request))
        
        if response.ok:
            rd = json.loads(response.text)
            properties = rd["data"]["getUserInfo"]
            
            if properties:
                if humanise:
                    properties['AccountType'] = self.PLANS.get(properties['AccountType'], properties['AccountType'])
                    properties['UsedQuota'] = humanize.naturalsize(int(properties['UsedQuota']))
                    properties['TotalQuota'] = humanize.naturalsize(int(properties['TotalQuota']))
                
                return properties
            else:
                return {}
        else:
            raise DegooError(f"getUserInfo failed with: {response}")
    
    def getOverlay3(self, degoo_id):
        args = f"{self.PROPERTIES}"
        func = f"getOverlay3(Token: $Token, ID: $ID) {{ {args} }}"
        query = f"query GetOverlay3($Token: String!, $ID: IDType!) {{ {func} }}"
        
        request = { "operationName": "GetOverlay3",
                    "variables": {
                        "Token": self.KEYS["Token"],
                        "ID": {"FileID": degoo_id}
                        },
                    "query": query
                   }
        
        header = {"x-api-key": self.KEYS["x-api-key"]}
        
        response = requests.post(URL_API, headers=header, data=json.dumps(request))
        
        if response.ok:
            rd = json.loads(response.text)
            properties = rd["data"]["getOverlay3"]
            
            if properties:
                
                # Add the Category Name
                cat = self.CATS.get(properties['Category'], f"Category {properties['Category']}")
                properties["CategoryName"] = cat
                
                # Fix the FilePath from Degoo incompleteness to a complete path from root.
                if cat in ["Device", "Recycle Bin"]:
                    if cat == "Device":
                        properties["FilePath"] = f"{os.sep}{properties['Name']}"
                    elif cat == "Recycle Bin":
                        dns = device_names()
                        properties["FilePath"] = f"{os.sep}{dns[properties['DeviceID']]}{os.sep}Recycle Bin"
                else:
                    # FilePath includes neither the Device name nor Recylce Bin alas. We 
                    # patch those in here to provide a FilePath that is complete and 
                    # compariable with the web interface UX. 
                    binned = properties["IsInRecycleBin"]
                    dns = device_names()
                    prefix = dns[properties['DeviceID']]+os.sep+"Recycle Bin" if binned else dns[properties['DeviceID']] 
                    properties["FilePath"] = f"{os.sep}{prefix}{properties['FilePath'].replace('/',os.sep)}"
        
                # Convert ID and Sizeto an int. 
                properties["ID"] = int(properties["ID"])
                properties["ParentID"] = int(properties["ParentID"]) if properties["ParentID"] else 0
                properties["MetadataID"] = int(properties["MetadataID"])
                properties["Size"] = int(properties["Size"])

                # Add a set of Human Readable time stamps based om the less readable API timestamps
                times = self._human_readable_times(properties['CreationTime'], properties['LastModificationTime'], properties['LastUploadTime'])

                properties["Time_Created"]      = times[0]
                properties["Time_LastModified"] = times[1]
                properties["Time_LastUpload"]   = times[2]
                
                return properties
            else:
                return {}
        else:
            raise DegooError(f"getOverlay3 failed with: {response}")
    
    def getFileChildren3(self, dir_id):
        args = f"Items {{ {self.PROPERTIES} }} NextToken __typename"
        func = f"getFileChildren3(Token: $Token, ParentID: $ParentID, Limit: $Limit, Order: $Order, NextToken: $NextToken) {{ {args} }}"
        query = f"query GetFileChildren3($Token: String!, $ParentID: String!, $Limit: Int!, $Order: Int, $NextToken: String) {{ {func} }}"
        
        request = { "operationName": "GetFileChildren3",
                    "variables": {
                        "Token": self.KEYS["Token"],
                        "ParentID": f"{dir_id}",
                        "Limit": self.LIMIT_MAX,
                        "Order": 3
                        },
                    "query": query
                   }
        
        header = {"x-api-key": self.KEYS["x-api-key"]}
        
        response = requests.post(URL_API, headers=header, data=json.dumps(request))
        
        if response.ok:
            rd = json.loads(response.text)
            items = rd["data"]["getFileChildren3"]["Items"]
            
            if items:
                next = rd["data"]["getFileChildren3"]["NextToken"]  # @ReservedAssignment
                if next:
                    # TODO: Work out what to do in this case.
                    print(f"WARNING: PAGINATION ISSUE, NextToken={next}")
                    
                # Fix FilePath by prepending it with a Device name.and converting 
                # / to os.sep so it becomes a valid os path as well.
                if dir_id == 0:
                    for i in items:
                        i["FilePath"] = f"{os.sep}{i['Name']}"
                        i["CategoryName"] = self.CATS.get(i['Category'], i['Category'])
                else:
                    # Get the device names if we're not getting a root dir
                    # device_names calls back here (i.e. uses the getFileChildren3 API call)
                    # with dir_id==0, to get_file the devices. We only need device names to prepend 
                    # paths with if we're looking deeper than root.
                    dns = device_names()
                    
                    for i in items:
                        binned = i["IsInRecycleBin"]
                        cat = self.CATS.get(i['Category'], f"Category {i['Category']}")
                        i["CategoryName"] = cat 
                        
                        # Fix the FilePath from Degoo incompleteness to a complete path.
                        if cat in ["Device", "Recycle Bin"]:
                            if cat == "Device":
                                i["FilePath"] = f"{os.sep}{i['Name']}"
                            elif cat == "Recycle Bin":
                                i["FilePath"] = f"{os.sep}{dns[i['DeviceID']]}{os.sep}Recycle Bin"
                        else:
                            # FilePath includes neither the Device name nor Recylce Bin alas. We 
                            # patch those in here to provide a FilePath that is complete and 
                            # compariable with the web interface UX. 
                            binned = i["IsInRecycleBin"]
                            prefix = dns[i['DeviceID']]+os.sep+"Recycle Bin" if binned else dns[i['DeviceID']] 
                            i["FilePath"] = f"{os.sep}{prefix}{i['FilePath'].replace('/',os.sep)}"
        
                # Convert ID to an int. 
                for i in items:
                    i["ID"] = int(i["ID"])
                    i["ParentID"] = int(i["ParentID"]) if i["ParentID"] else 0
                    i["MetadataID"] = int(i["MetadataID"])
                    i["Size"] = int(i["Size"])
                    
                    # Add a set of Human Readable time stamps based om the less readable API timestamps
                    times = self._human_readable_times(i['CreationTime'], i['LastModificationTime'], i['LastUploadTime'])
    
                    i["Time_Created"]      = times[0]
                    i["Time_LastModified"] = times[1]
                    i["Time_LastUpload"]   = times[2]
                
                self.NAMELEN = max([len(i["Name"]) for i in items]) 
            
            return items
        else:
            raise DegooError(f"getFileChildren3 failed with: {response}")

    def setDeleteFile4(self, degoo_id):
        func = f"setDeleteFile4(Token: $Token, IsPermanent: $IsPermanent, IDs: $IDs)"
        query = f"mutation SetDeleteFile4($Token: String!, $IsPermanent: Boolean!, $IDs: [IDType]!) {{ {func} }}"
    
        request = { "operationName": "SetDeleteFile4",
                    "variables": {
                        "Token": self.KEYS["Token"],
                        "IDs": [{ "FileID": degoo_id }],
                        "IsPermanent": False,
                        },
                    "query": query
                   }
        
        header = {"x-api-key": self.KEYS["x-api-key"]}
        
        response = requests.post(URL_API, headers=header, data=json.dumps(request))
        
        if not response.ok:
            raise DegooError(f"setDeleteFile4 failed with: {response}")
        else:
            return response.text

    def setUploadFile2(self, name, parent_id, size="0", checksum="CgAQAg"):
        func = f"setUploadFile2(Token: $Token, FileInfos: $FileInfos)"
        query = f"mutation SetUploadFile2($Token: String!, $FileInfos: [FileInfoUpload2]!) {{ {func} }}"
    
        # The size is 0 and checksum is "CgAQAg" when creating folders. 
        #    This seems consistent.
        #
        # For file uploads we need Size and Checksum to be right.
        #    Size is easy (the size of the file should be supplued_
        #    Checksum is a little harder, but it should be supplied ready to plug un here.
        #
        # In practice it turns out Degoo use a SHA1 checksum seeded with what looks
        # like a hardcoded string, and then prefixing it and appending it with some
        # metadata and then encoding it base64. Phew. Hence we leave it to the caller 
        # to provide the checksum.
        request = { "operationName": "SetUploadFile2",
                    "variables": {
                        "Token": self.KEYS["Token"],
                        "FileInfos": [{
                            "Checksum": checksum,
                            "Name": name,
                            "CreationTime": int(1000*time.time()),
                            "ParentID": parent_id,
                            "Size": size
                        }]
                        },
                    "query": query
                   }
        
        header = {"x-api-key": self.KEYS["x-api-key"]}
        
        response = requests.post(URL_API, headers=header, data=json.dumps(request))
        
        if response.ok:
            contents = get_children(parent_id)
            ids = {f["Name"]: int(f["ID"]) for f in contents}
            if not name in ids:
                obj = get_item(parent_id)
                raise DegooError(f"setUploadFile2: Failed to find {name} in {obj['FilePath']}: {response}")
            return ids[name]
        
        else:
            raise DegooError(f"setUploadFile2 failed with: {response}")
        
    def getBucketWriteAuth2(self, dir_id):
        kv = " {Key Value __typename}"
        args = "\n".join(["PolicyBase64", "Signature", "BaseURL", "KeyPrefix", "AccessKey"+kv, "ACL",  "AdditionalBody"+kv, "__typename"])
        func = f"getBucketWriteAuth2(Token: $Token, ParentID: $ParentID, StorageUploadInfos: $StorageUploadInfos) {{ {args} }}"
        query = f"query GetBucketWriteAuth2($Token: String!, $ParentID: String!, $StorageUploadInfos: [StorageUploadInfo]) {{ {func} }}"
        
        request = { "operationName": "GetBucketWriteAuth2",
                    "variables": {
                        "Token": self.KEYS["Token"],
                        "ParentID": f"{dir_id}",
                        "StorageUploadInfos":[]
                        },
                    "query": query
                   }
        
        header = {"x-api-key": self.KEYS["x-api-key"]}
        
        response = requests.post(URL_API, headers=header, data=json.dumps(request))
        
        if response.ok:
            rd = json.loads(response.text)
            
            if rd.get("errors", None):
                raise DegooError(f"getBucketWriteAuth2 failed with: {rd['errors']}")
            
            # The index 0 suggests maybe if we upload multiple files we get_file mutipple WriteAuths back
            RD = rd["data"]["getBucketWriteAuth2"][0]
            
            return RD
        else:
            raise DegooError(f"getBucketWriteAuth2 failed with: {response}")
            
api = API()

def split_path(path):
    allparts = []
    while 1:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            allparts.insert(0, parts[0])
            break
        elif parts[1] == path: # sentinel for relative paths
            allparts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            allparts.insert(0, parts[1])
    return allparts

def userinfo():
    return api.getUserInfo()    

def mkpath(path, verbose=False):
    '''
    Analagous to Linux "mkdir -p", creaping all necessary parents along the way.
    
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
            
    return get_item(current_dir)["FilePath"]

def mkdir(name, parent_id=None, verbose=False):
    '''
    Makes a Degoo directory/folder or device
    
    :param name: The name of a directory/folder to make in the CWD or nominated (by id) parent
    :param parent_id: Optionally a the degoo ID of a parent. If not specified the CWD is used.
    '''
    if parent_id == None and "ID" in CWD:
        parent_id = CWD["ID"]
    
    contents = get_children(parent_id)
    existing_names = [f["Name"] for f in contents]
    ids = {f["Name"]: int(f["ID"]) for f in contents}
    if name in existing_names:
        if verbose:
            print(f"{name} already exists")
        return ids[name]
    else:
        ID = api.setUploadFile2(name, parent_id)
        if verbose:
            print(f"Created directory {name} with ID {ID}")

def rm(file):
    if isinstance(file, int):
        file_id = file
    elif isinstance(file, str):
        file_id = path_id(file)
    else:
        print(f"Error: Illegal file: {file}")
        exit(1)

    path = api.getOverlay3(file_id)["FilePath"]
    response = api.setDeleteFile4(file_id)  # @UnusedVariable

    return path

def cd(path):
    CWD = get_dir(path)
    with open(cwd_file, "w") as file:
        file.write(json.dumps(CWD))
    return CWD

def device_names():
    devices = {}
    root = get_children(0)
    for d in root:
        if d['CategoryName'] == "Device":
            devices[int(d['DeviceID'])] = d['Name']
    return devices 

def get_dir(path=None):
    item = get_item(path)
    return fsi_dict(item["ID"], item["FilePath"])

def get_parent(degoo_id):
    props = get_item(degoo_id)
    parent = props.get("ParentID", None)
    
    if not parent is None:
        parent_props = get_item(parent)
        return fsi_dict(parent_props.get("ID", None), parent_props.get("FilePath", None))
    else:
        return None    

def path_str(degoo_id):
    props = get_item(degoo_id)
    return props.get("FilePath", None)

def parent_id(degoo_id):
    props = get_item(degoo_id)
    return props.get("ParentID", None)

def path_id(path):
    '''
    Returns the Degoo ID of the object at path (Folder or File, or whatever). 
    
    If an int is passed just returns that ID but if a str is passed finds the ID and returns it.
    
    if no path is specified returns the ID of the Current Working Directory (CWD).
    
    :param path: An int or st or None
    '''
    return get_item(path)["ID"] 

def get_item(path):
    '''
    Return the Degoo object that path points to  (Folder or File, or whatever).
    
    This is just a dict of properties really.

    if no path is specified returns the ID of the Current Working Directory (CWD).

    :param path: An int or str or None
    '''
    def props(degoo_id):
        # The root is special, it returns no properties from the degoo API
        # We dummy some up for internal use:
        if degoo_id in __CACHE_ITEMS__:
            return __CACHE_ITEMS__[degoo_id]
        elif degoo_id == 0:
            properties = {
                "ID": 0,
                "ParentID": None,
                "Name": "/",
                "FilePath": "/",
                "Category": None,
                "CategoryName": "Root",
                }
        else:
            properties = api.getOverlay3(degoo_id)
        
        __CACHE_ITEMS__[degoo_id] = properties
        return properties

    if isinstance(path, int):
        return props(path)
    elif not isinstance(path, str) and not path is None:
        raise DegooError(f"Illegal path: {path}")
    
    if path != None:
        parts = split_path(path)
        
        if parts[0] == os.sep:
            current_part = 0
            parts.pop(0)
        else:
            current_part = CWD["ID"]
        
        previous_part = None

        for p in parts:
            # If the last char in path is os.sep then the last item in parts is empty
            if p:
                if p == "..":
                    if previous_part == None and not CWD["ID"]:
                        # .. only mkaes sense below the root dir. If we have no previous_part
                        # and tbere's not CWD that isn't root, we can't go up one directory.
                        raise DegooError(f"Directory '..' does not exist in {path_str(current_part)}")
                    elif previous_part != None:
                        # Previous dir in current path
                        current_part = previous_part
                    elif current_part:
                        parent = get_parent(current_part)
                        current_part = parent["ID"]
                    else:
                        raise DegooError(f"Directory '..' does not exist in {path_str(current_part)}")                         
                elif p!= ".":
                    contents = get_children(current_part)
                    ids = {f["Name"]: int(f["ID"]) for f in contents}
                    cat = {f["Name"]: f['CategoryName'] for f in contents}
                    if p in ids:
                        # For ".." suport
                        previous_part = current_part
                        current_part = ids[p]
                    else:
                        thing = f"{p}" if cat[p] == p else f"{cat[p]} {p}"
                        raise DegooError(f"{thing} does not exist in {path_str(current_part)}")
            
        return props(current_part)
    else:
        return props(CWD["ID"])

def get_children(directory=None):
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
        print(f"Error: Illegal directory: {directory}")
        exit(1)

    return api.getFileChildren3(dir_id)

def get_file(file, verbose=False):
    item = get_item(file)
    
    # If we landed here with a directiory rather thanm file, just redirect
    # to the approproate downloader.
    if item["CategoryName"] in api.folder_types:
        return get_directory(file)
    
    # Try the Optimized URL first I guess
    URL = item.get("OptimizedURL", None)
    if not URL:
        URL = item.get("URL", None)

    Name = item.get("Name", None)
    Path = item.get("FilePath", None)

    if URL and Name:
        # wget.download uses urllib.request.urlopen()
        # which sets the User-agent header to "Python-urllib/3.8"
        # Turns out degoo refuses requests from that user agent.
        # In fact testing reveals that "Python-urllib" is rejected
        # and that "Python-stupid" is accepted. They seem to check
        # explicitly for  "Python-urllib" and reject requests.
        # To fix that is a tad tricky with wget as it doesn't accept
        # headers (a bug IMHO), but there is a work around because
        # urllib allows installation of an opener that has the 
        # headers in an addheaders attribute. We can build an opener
        # install our own User-agent header and install the opener
        # and wget will end up using it. Roundabout but it works. 
        opener = request.build_opener()
        opener.addheaders = [('User-agent', "SomethingSafe")]
        request.install_opener(opener)
        
        if verbose:
            print(f"Downloading {Path}")
            
        # wget has a bug and while it has a nice progress bar it render garbage 
        # for Degoo because the download source fails to set the content-length 
        # header. We know the notent length from the Degoo metadata though and so
        # can specify it manually. wget does not support this but I've submitted
        # a PR here: https://github.com/jamiejackherer/python3-wget/pull/4
        _ = wget.download(URL, out=Name, size=item['Size'])
        
        # The default wqet progress bar leaves cursor at end of line.
        # It failes to print a new line. This causing glitchy printing
        # Easy fixed, 
        print("")
        
        return item["FilePath"]
    else:
        raise DegooError(f"{Path} has no URL to download from.")

def get_directory(folder, verbose=False):
    item = get_item(folder)
    
    # If we landed here with a file rather thanm folder, just redirect
    # to the approproate downloader.
    if not item["CategoryName"] in api.folder_types:
        return get_file(folder)
    
    dir_id = item['ID']

    # Remember the current working directory
    cwd = os.getcwd()
    
    # Make the target direcory if needed    
    try:
        os.mkdir(item['Name'])
    except FileExistsError:
        # Not a problem if the folder already exists
        pass
    
    # Step down into the new directory for the downloads
    os.chdir(item['Name'])
    
    # Fetch and classify all Degoo drive contents of this folder
    children = get_children(dir_id)

    files = [child for child in children if not child["CategoryName"] in api.folder_types]
    folders = [child for child in children if child["CategoryName"] in api.folder_types]

    # Download files
    for f in files:
        try:
            get_file(f['ID'], verbose)
        except DegooError as e:
            if verbose:
                print(e)

    # Make the local folders and download into them
    for f in folders:
        get_directory(f['ID'], verbose)

    # Having downloaded all the items in this folder chdir back up
    os.chdir(cwd)

def get(path, verbose=False):
    item = get_item(path)

    if item["CategoryName"] in api.folder_types:
        return get_directory(item['ID'], verbose)
    else:
        return get_file(item['ID'], verbose)

def put_file(file, path, verbose=False):
    # path better point to a folder not a file ... Not sure what happens if its a Device or Recycle Bin.
    dir_id = path_id(path)

    MimeTypeOfFile = magic.Magic(mime=True).from_file(file)

    # Get the Authorisation to write to this directory
    # Provides the metdata we need for the upload   
    result = api.getBucketWriteAuth2(dir_id)
    
    BaseURL = result["BaseURL"]

    # We now POST to BaseURL and the body is the file but all these fields too
    Signature =      result["Signature"]
    GoogleAccessId = result["AccessKey"]["Value"]
    CacheControl =   result["AdditionalBody"][0]["Value"]  # Only one item in list not sure why indexed
    Policy =         result["PolicyBase64"]
    ACL =            result["ACL"]
    KeyPrefix =      result["KeyPrefix"]  # Has a trailing / 
    
    # This one is a bit mysterious. The Key seems to be made up of 4 parts
    # separated by /. The first two are provided by getBucketWriteAuth2 as
    # as the KeyPrefix, the next appears to be the file extension, and the 
    # last an apparent filename that is consctucted as checksum.extension.
    # Odd, to say the least.
    Type = os.path.splitext(file)[1][1:]
    Checksum = api.check_sum(file)
    
    if Type:     
        Key = "{}{}/{}.{}".format(KeyPrefix, Type, Checksum, Type)
    else:
        # TODO: When there is no file extension, the Degoo webapp uses "unknown"
        # but this fails to produce a successful upload too. A Degoo API bug.
        Key = "{}/{}".format(KeyPrefix, Checksum)
    
    # We need filesize
    Size = os.path.getsize(file)
    # Now upload the file    
    parts = [
        ('key', (None, Key)),
        ('acl', (None, ACL)),
        ('policy', (None, Policy)),
        ('signature', (None, Signature)),
        ('GoogleAccessId', (None, GoogleAccessId)),
        ('Cache-control', (None, CacheControl)),
        ('Content-Type', (None, MimeTypeOfFile)),
        ('file', (os.path.basename(file), open(file, 'rb'), MimeTypeOfFile))
    ]
    
    heads = {"ngsw-bypass": "1", "x-client-data": "CIS2yQEIpbbJAQipncoBCLywygEI97TKAQiXtcoBCO21ygEIjrrKAQ=="}
    response = requests.post(BaseURL, files=parts, headers=heads)
    
    # We expect a 204 stats result, which is silent acknowleddgement of success.
    if response.ok and response.status_code == 204:
        # Theeres'a Google Upload ID returned in the headers. Not sure what use it is.
        google_id = response.headers["X-GUploader-UploadID"]  # @UnusedVariable
        
        # Empirically the download URL seems failry predictable from the inputs we have.
        # with two caveats:
        #
        # The expiry time is a little different. It's 14 days form now all right but
        # now being when setUploadFile2 on the server has as now not the now we have here.
        #
        # The Signature is new, it's NOT the Signature that getBucketWriteAuth2 returned
        # nor any obvious variation upon it (like base64 encoding)
        expiry = str(int((datetime.datetime.utcnow() + datetime.timedelta(days=14)).timestamp()))
        expected_URL = "".join([  # @UnusedVariable
                    BaseURL.replace("storage-upload.googleapis.com/",""),
                    Key,
                    "?GoogleAccessId=", GoogleAccessId,
                    "&Expires=", expiry,
                    "&Signature=", Signature,
                    "&use-cf-cache=true"])
        
        degoo_id = api.setUploadFile2(os.path.basename(file), dir_id, Size, Checksum)
        props = api.getOverlay3(degoo_id)
        
        if verbose:
            print(f"Download URL is: {props['URL']}")
            
        return (degoo_id, props['URL'])
    else:
        raise DegooError(f"Upload failed with: Failed with: {response}")

def put_directory(directory, path, verbose=False):
    IDs = {}
    
    target_dir = get_dir(path)
    (target_junk, target_name) = os.path.split(directory)
    
    if verbose:
        print(f"Creating directory {target_name} in {target_dir['Path']}")
        
    Root = target_name
    IDs[Root] = mkdir(target_name, target_dir['ID'])
    
    for root, dirs, files in os.walk(directory):
        # if directory contains a head that is included in root and we don't want
        # it when we're making dirs on the remote (cloud) drive and remembering the
        # IDs of thos edirs.
        relative_root = root.replace(target_junk+os.sep, "", 1)
        
        for name in dirs:
            Name = os.path.join(relative_root, name)
            if verbose:
                print(f"Creating directory {name} in {Name}")
            IDs[Name] = mkdir(name, IDs[relative_root])
            
        for name in files:
            Name = os.path.join(relative_root, name)
            if verbose:
                print(f"Uploading file {Name}")
            put_file(Name, IDs[relative_root])
    
    # TODO Work out how to hand Dir download URLs
    return (IDs[Root], "No URL implemented for directory downloads just yet.")
            
def put(source, path, verbose=False):
    isFile = os.path.isfile(source)
    isDirectory = os.path.isdir(source)
    
    if isDirectory:
        return put_directory(source, path, verbose)
    elif isFile:
        return put_file(source, path, verbose)
    else:
        return (None, None)
    
###########################################################################
# Text output functions

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
        
def tree(dir_id=0, show_times=False,_done=[]):
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

