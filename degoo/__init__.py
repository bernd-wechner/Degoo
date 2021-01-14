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
from dateutil import parser, tz
import os, sys, csv, json, time, datetime, requests, wget, magic, humanize, humanfriendly, hashlib, base64

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

# A string to prefix CLI commands with (configurable, and used by 
# build.py     - to make the commands and,
# commands.py  - to implement the commands 
command_prefix = "degoo_"

# The URLS that the Degoo API relies upon
URL_login = "https://rest-api.degoo.com/login"
URL_API   = "https://production-appsync.degoo.com/graphql"

# Local config and state files
cred_file  = os.path.join(user_config_dir("degoo"), "credentials.json")
cwd_file   = os.path.join(user_config_dir("degoo"), "cwd.json")
keys_file  = os.path.join(user_config_dir("degoo"), "keys.json")
DP_file    = os.path.join(user_config_dir("degoo"), "default_properties.txt")
sched_file = os.path.join(user_config_dir("degoo"), "schedule.json")

# A local cache of Degoo items and contents, to speed up successive queries for them
# BY convention we have Degoo ID 0 as the root directory and the API returns no 
# properties for that so we dummy some up for local use to give it the appearance 
# of a root directory.
__CACHE_ITEMS__ = {0: {
                        "ID": 0,
                        "ParentID": None,
                        "Name": "/",
                        "FilePath": "/",
                        "Category": None,
                        "CategoryName": "Root",
                        }
                    }
__CACHE_CONTENTS__ = {} 

###########################################################################
# Support functions

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
        elif parts[1] == path: # sentinel for relative paths
            allparts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            allparts.insert(0, parts[1])
    return allparts

def absolute_remote_path(path):
    '''
    Convert a give path strig to an absolute one (if it's relative).
     
    :param path: The path to convert.
    :returns: The absolute version of path 
    '''
    global CWD
    if path and path[0] == os.sep:
        return os.path.abspath(path.rstrip(os.sep))
    else:
        return os.path.abspath(os.path.join(CWD["Path"], path.rstrip(os.sep)))

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
        if diff < 0: return       # In case end_datetime was in past to begin with
        
        if verbose > 1:
            print(f"Waiting for {humanfriendly.format_timespan(diff/2)} seconds")
            
        time.sleep(diff/2)
        if diff <= 0.1: return

###########################################################################
# Load the current working directory, if available

CWD = ddd(0, "/")
if os.path.isfile(cwd_file):
    with open(cwd_file, "r") as file:
        CWD = json.loads(file.read())

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
if os.path.isfile(sched_file):
    with open(sched_file, "r") as file:
        SCHEDULE = json.loads(file.read())
else:
    with open(sched_file, "w") as file:
        file.write(json.dumps(DEFAULT_SCHEDULE))

###########################################################################
# Logging in is a prerequisite to using the API (a pre API step). The 
# login function reads from the configure cred_file and writes keys the
# API needs to the keys_file.

def login():
    '''
    Logs into a Degoo account. 
    
    The login is lasting, i.e. does not seem to expire (not subjet to any autologout)
    
    This function leans on:
    
    Degoo account credentials stored in a file `cred_file` 
    (by default ~/.config/degoo/credentials.json) This file shoud contain
    a JSON dict with elements Username and Password akin to:
        {"Username":"username","Password":"password"}
    This file should be secure readable by the user concerned, as it affords
    access to that users Degoo account.
        
    URL_login which is the URL it posts the credentials to.
    
    The reply provides a couple of keys that must be provided with each subsequent
    API call, as authentication, to prove we're logged in. These are written in JSON
    format to keys_file which is by default: ~/.config/degoo/keys.json
    
    TODO: Support logout as well (which will POST a logout request and remove these keys)
    
    :returns: True if successful, false if not
    '''
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
        
            return True
        else:
            return False
    else:
        with open(cred_file, "w") as file:
            file.write(json.dumps({"Username": "<your Degoo username here>", "Password": "<your Degoo password here>"}))
            
        print("No login credentials available. Please add account details to {cred_file}", file=sys.stderr)

###########################################################################
# Bundle all the API interactions into an API class

class API:
    # Empirically determined, largest value degoo supports for the Limit 
    # on the Limit parameter to the GetFileChildren3 operation. It's used
    # for paging, and if more items exist there'll be a NextToken returned.
    # TODO: Determine the syntax and use of that NextToken so that paged 
    # results can be fecthed reliably as well. For now we just make calls 
    # with the max limit and avoid dealing with paging. 
    LIMIT_MAX = int('1'*31, 2)-1

    # This appears to be an invariant key that the API expects in the header
    # x-api-key:
    API_KEY = "da2-vs6twz5vnjdavpqndtbzg3prra"
    
    # Keys needed to interact with the API. Provided during login.
    KEYS = None
    
    # Known Degoo API item categories
    CATS = {  0: "File",
              1: "Device",
              2: "Folder",
              3: "Image",
              4: "Video",
              5: "Music",
              6: "Document",
             10: "Recycle Bin",
           }
    
    # The types of folder we see on a Degoo account
    # These are characteristically different kinds of item to Degoo
    # But we will try to provide a more unifor folder style interface 
    # to them herein. 
    folder_types = ["Folder", "Device", "Recycle Bin"]
    
    # A guess at the plans available
    PLANS = { 0: "Free 100 GB",
              1: "Pro 500 GB",
              2: "Ultimate 10 TB",
              3: "Ultimate Stackcommerce offer 10 TB"
            }
    
    # Width of a Category field in text output we produce
    # Should be wide enough to handle the longest entry in CATS
    # Updated in __init_/
    CATLEN = 10
    
    # Width of Name field for text output we produce
    # Used when listing files, updated to teh width needed to display
    # the longest filename. Updated by getFileChildren3 when it returns 
    # a list of filenames.
    NAMELEN = 20
    
    # A list of Degoo Item properties. The three API calls:
    #    getOverlay3
    #    getFileChildren3
    #    getFilesFromPaths
    # all want a list of explicit propeties it seems, that they will 
    # return. We want them all basically, and the superset of all known 
    # properties that Degoo Items have should be stored in DP_file, 
    # which is by default:
    #
    # ~/.config/degoo/default_properties.txt
    #
    # A sample file should accompany this script. One property per
    # line in the file. 
    # 
    # TODO: Are there any further properties? It would be nice for 
    # example if we could ask for the checksum that is canculated
    # when the file is upladed and provided to SetUploadFile2.
    PROPERTIES = ""
    
    def __init__(self):
        '''
        Reads config and state files to intitialise the API.
        
        Specifically:
        
            Loads authentication keys from key_file if available (the product of 
            loging in)
            
            Loads the superset of known degoo item properties that we can ask 
            for when sending queries to the remote API.
            
            Sets CATLEN to the length fo the longest CAT name.
        '''
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
        '''
        Given three Degoo timestamps converts them to human readable 
        text strings. These three timestamps are provided for every
        Degoo item.
        
        :param creation:        The time of creation 
        :param modification:    The time of last modifciation 
        :param upload:          The time of last upload  

        :returns:               A tuple of 3 strings.        
        '''
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
        '''
        When uploading files Degoo uses a 2 step process:
            1) Get Authorisation from the Degoo API - provides metadate needed for step 2
            2) Upload the file to a nominated URL (in practice this appears to be Google Cloud Services)
            
        The upload to Google Cloud services wants a checksum for the file (for upload integrity assurance)
        
        This appears to a base64 encoded SHA1 hash of the file. Empircially this, with a little dressing
        appears to function. The SHA1 hash seems to use a hardcoded string as a seed (based on JS analysis)
                
        :param filename:    The name of the file (full path so it can be read)
        :param blocksize:   Optionally a block size used for reading the file 
        '''
        Seed = bytes([13, 7, 2, 2, 15, 40, 75, 117, 13, 10, 19, 16, 29, 23, 3, 36])
        Hash = hashlib.sha1(Seed)
        with open(filename, "rb") as f:
            for block in iter(lambda: f.read(blocksize), b""):
                Hash.update(block)
                
        cs = list(bytearray(Hash.digest()))
        
        # On one test file we now have:
        # [82, 130, 147, 14, 109, 84, 251, 153, 64, 39, 135, 7, 81, 9, 21, 80, 203, 120, 35, 150]
        # and need to encode this to:
        # [10, 20, 82, 130, 147, 14, 109, 84, 251, 153, 64, 39, 135, 7, 81, 9, 21, 80, 203, 120, 35, 150, 16, 0]
        # Which is four bytes longer, prepended by a word and appended by a word.
        # JS code inspection is non conclusive, it's well obfuscated Webpack JS alas.
        #
        # But a hypothesis is:
        #
        # 10, len(checksum), checksum, 16, type
        # And type is always 0 for file uploads.
        #
        # This passes all tests so far. But remains an hypthesis and not well understood.
        #
        # TODO: Can we move this from an hypothesis to a conclusion?
        CS = [10, len(cs)] + cs + [16, 0]
        
        # Finally, Degoo base64 encode is cehcksum.
        checksum = base64.b64encode(bytes(CS)).decode()
        
        return  checksum    
    
    def getUserInfo(self, humanise=True):
        '''
        A Degoo Graph API call: gets information about the logged in user.
         
        :param humanise:  If true converts some properties into a human readable format.
        '''
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
            
            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise DegooError(f"getUserInfo failed with: {message}")
            else: 
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
        '''
        A Degoo Graph API call: gets information about a degoo item identified by ID.
        
        A Degoo item can be a file or folder but may not be limited to that (see self.CATS). 
         
        :param degoo_id: The ID of the degoo item.
        '''
        #args = self.PROPERTIES.replace("Size\n", "Size\nHash\n")
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
            
            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise DegooError(f"getOverlay3 failed with: {message}")
            else: 
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
            raise DegooError(f"getOverlay3 failed with: {response.text}")
    
    def getFileChildren3(self, dir_id):
        '''
        A Degoo Graph API call: gets the contents of a Degoo directory (the children of a Degoo item that is a Folder)
        
        :param dir_id: The ID of a Degoo Folder item (might work for other items too?)
        
        :returns: A list of property dictionaries, one for each child, contianing the properties of that child.
        '''
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

            if 'errors' in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                if "Invalid input!" in messages:
                    print(f"WARNING: Degoo Directory with ID {dir_id} apparently does not to exist!", file=sys.stderr)
                    return []   
                else:
                    message = '\n'.join(messages)
                    raise DegooError(f"getFileChildren3 failed with: {message}")
            else:
                items = rd["data"]["getFileChildren3"]["Items"]
                
                if items:
                    next = rd["data"]["getFileChildren3"]["NextToken"]  # @ReservedAssignment
                    if next:
                        # TODO: Work out what to do in this case.
                        print(f"WARNING: PAGINATION ISSUE, NextToken={next}", file=sys.stderr)
                        
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

    def getFilesFromPaths(self, device_id, path=""):
        '''
        A Degoo Graph API call: Not sure what this API call is for to be honest. 
        
        It's called after an upload for some reason. But if seems to offer nothing of value.
        
        :param device_id: A Degoo device ID
        :param path:      Don't know what this is.
        '''
        args = f"{self.PROPERTIES}"
        func = f"getFilesFromPaths(Token: $Token, FileIDPaths: $FileIDPaths) {{ {args} }}"
        query = f"query GetFilesFromPaths($Token: String!, $FileIDPaths: [FileIDPath]!) {{ {func} }}"
        
        request = { "operationName": "GetFilesFromPaths",
                    "variables": {
                        "Token": self.KEYS["Token"],
                        "FileIDPaths": [{
                            "DeviceID": device_id,
                            "Path": path,
                            "IsInRecycleBin": False
                        }]
                        },
                    "query": query
                   }
        
        header = {"x-api-key": self.KEYS["x-api-key"]}
        
        response = requests.post(URL_API, headers=header, data=json.dumps(request))
        
        if response.ok:
            rd = json.loads(response.text)

            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise DegooError(f"getFilesFromPaths failed with: {message}")
            else:
                # Debugging output
                print("Response Headers:", file=sys.stderr)
                for h in response.headers:
                    print(f"\t{h}: {response.headers[h]}", file=sys.stderr)
                
                rd = json.loads(response.text)
                items = rd["data"]["getFilesFromPaths"]
                return items
        else:            
            raise DegooError(f"getFilesFromPaths failed with: {response}")

    def setDeleteFile4(self, degoo_id):
        '''
        A Degoo Graph API call: Deletes a Degoo item identified by ID. It is moved to the Recycle Bin 
        for the device it was on, and this is not a secure delete. It must be expolicitly deleted 
        from the Recylce bin to be a secure delete.
        
        #TODO: support an option to find and delete the file in the recycle bin,  
        
        :param degoo_id: The ID of a Degoo item to delete.
        '''
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
        
        if response.ok:
            rd = json.loads(response.text)

            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise DegooError(f"setDeleteFile4 failed with: {message}")
            else:
                return response.text
        else:
            raise DegooError(f"setDeleteFile4 failed with: {response}")

    def setUploadFile2(self, name, parent_id, size="0", checksum="CgAQAg"):
        '''
        A Degoo Graph API call: Appears to create a file in the Degoo filesystem. 
        
        Directories are created with this alone, but files it seems are not stored 
        on the Degoo filesystem at all, just their metadata is, the actual file contents
        are stored on a Google Cloud Service. 
        
        To Add a file means to call getBucketWriteAuth2 to start the process, then
        upload the file content, then finally, create the Degoo file item that then 
        points to the actual file data with a URL. setUploadFile2 does not return 
        that UTL, but getOverlay3 does.
        
        :param name:        The name of the file
        :param parent_id:   The Degoo ID of the Folder it will be placed in  
        :param size:        The files size
        :param checksum:    The files checksum (see self.check_sum)
        '''
        func = f"setUploadFile2(Token: $Token, FileInfos: $FileInfos)"
        query = f"mutation SetUploadFile2($Token: String!, $FileInfos: [FileInfoUpload2]!) {{ {func} }}"
    
        # The size is 0 and checksum is "CgAQAg" when creating folders. 
        #    This seems consistent.
        #
        # For file uploads we need Size and Checksum to be right.
        #    Size is easy (the size of the file should be supplied_
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
#             print("Degoo Response Headers:", file=sys.stderr)
#             for h in response.headers:
#                 print(f"\t{h}: {response.headers[h]}", file=sys.stderr)
#             print("Degoo Response Content:", file=sys.stderr)
#             print(json.dumps(json.loads(response.content), indent=4), file=sys.stderr)        
#             print("", file=sys.stderr)
            
            rd = json.loads(response.text)

            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise DegooError(f"setUploadFile2 failed with: {message}")
            else:
                contents = self.getFileChildren3(parent_id)
                ids = {f["Name"]: int(f["ID"]) for f in contents}
                if not name in ids:
                    obj = get_item(parent_id)
                    print(f"WARNING: Failed to find {name} in {obj['FilePath']} after upload.", file=sys.stderr)
                return ids[name]
        
        else:
            raise DegooError(f"setUploadFile2 failed with: {response}")
        
    def getBucketWriteAuth2(self, dir_id):
        '''
        A Degoo Graph API call: Appears to kick stat the file upload process.
        
        Returns crucial information for actually uploading the file. 
        Not least the URL to upload it to! 
        
        :param dir_id:
        '''
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

            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise DegooError(f"getBucketWriteAuth2 failed with: {message}")
            else:
                # The index 0 suggests maybe if we upload multiple files we get_file mutipple WriteAuths back
                RD = rd["data"]["getBucketWriteAuth2"][0]
                
                return RD
        else:
            raise DegooError(f"getBucketWriteAuth2 failed with: {response}")

    def getSchema(self): 
        '''
        Experimental effort to probe the GRaphQL Schema that Degoo provide.
        
        Not successful yet alas.
        '''
        
        # A normal request looks like:
        # {"operationName": "GetOverlay3", "variables": {"Token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOiIyMTU2Mjc4OCIsIm5iZiI6MTU5MTE1MjU1NCwiZXhwIjoxNjIyNjg4NTU0LCJpYXQiOjE1OTExNTI1NTR9.ZMbZ_Y_7bLQ_eerFssbKEr-QdujQ_p3LENeeKF-Niv4", "ID": {"FileID": 12589739461}}, "query": "query GetOverlay3($Token: String!, $ID: IDType!) { getOverlay3(Token: $Token, ID: $ID) { ID\\nMetadataID\\nUserID\\nDeviceID\\nMetadataKey\\nName\\nFilePath\\nLocalPath\\nURL\\nOptimizedURL\\nThumbnailURL\\nCreationTime\\nLastModificationTime\\nLastUploadTime\\nParentID\\nCategory\\nSize\\nPlatform\\nDistance\\nIsSelfLiked\\nLikes\\nIsHidden\\nIsInRecycleBin\\nDescription\\nCountry\\nProvince\\nPlace\\nLocation\\nLocation2 {Country Province Place __typename}\\nGeoLocation {Latitude Longitude __typename}\\nData\\nDataBlock\\nCompressionParameters\\nIsShared\\nShareTime\\nShareinfo {ShareTime __typename}\\n__typename\\n\\n } }"}
        
#         request = { "query": {
#                         "__schema": {
#                             "types": [
#                                 { "name" : "Query"}
#                              ]
#                         }
#                     }
#                    }
        
        request = '{"query":"{\n\t__schema: {\n queryType {\n fields{\n name\n }\n }\n }\n}"}'
        
        header = {"x-api-key": self.KEYS["x-api-key"]}
        
        response = requests.post(URL_API, headers=header, data=json.dumps(request))
        
        if not response.ok:
            raise DegooError(f"getSchema failed with: {response.text}")
        else:
            return response
    
api = API()

###########################################################################
# Command functions - these are entry points for the CLI tools

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
            
    return get_item(current_dir)["FilePath"]

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
            if verbose>0:
                print(f"{name} already exists")
            return ids[name]
        else:
            if not dry_run:
                ID = api.setUploadFile2(name, parent_id)
            else:
                # Dry run, no ID created
                ID = None 
                
            if verbose>0:
                print(f"Created directory {name} with ID {ID}")
                
            return ID
    else:
        raise DegooError(f"mkdir: No parent_id provided.")

def rm(file):
    '''
    Deletes (Removes) a nominated file from the Degoo filesystem.
    
    Unless the remote server deletes the actual file content from the cloud server this is not
    secure of course. In fact it supports trash and removing the file or folder, moves it to
    the Recycle Bin.
         
    :param file: Either a string which specifies a file or an int which provides A Degoo ID.
    '''
    if isinstance(file, int):
        file_id = file
    elif isinstance(file, str):
        file_id = path_id(file)
    else:
        raise DegooError(f"rm: Illegal file: {file}")

    path = api.getOverlay3(file_id)["FilePath"]
    response = api.setDeleteFile4(file_id)  # @UnusedVariable

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
    props = get_item(degoo_id)
    parent = props.get("ParentID", None)
    
    if not parent is None:
        parent_props = get_item(parent)
        return ddd(parent_props.get("ID", None), parent_props.get("FilePath", None))
    else:
        return None    

def path_str(degoo_id):
    '''
    Returns the FilePath property of a Degoo item.
    
    :param degoo_id: The Degoo ID of the item.
    '''
    props = get_item(degoo_id)
    return props.get("FilePath", None)

def parent_id(degoo_id):
    '''
    Returns the Degoo ID of the parent of a Degoo Item.
     
    :param degoo_id: The Degoo ID of the item concerned.
    '''
    props = get_item(degoo_id)
    return props.get("ParentID", None)

def path_id(path):
    '''
    Returns the Degoo ID of the object at path (Folder or File, or whatever). 
    
    If an int is passed just returns that ID but if a str is passed finds the ID and returns it.
    
    if no path is specified returns the ID of the Current Working Directory (CWD).
    
    :param path: An int or str or None (which ask for the current working directory)
    '''
    return get_item(path)["ID"] 

def is_folder(path):
    '''
    Returns true if the remote Degoo item referred to by path is a Folder
    
    :param path: An int or str or None (for the current working directory)
    '''
    return get_item(path)["CategoryName"] in api.folder_types

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
            path = CWD["ID"] # Current working directory if it exists
        else:
            path = 0 # Root directory if nowhere else
    elif isinstance(path, str):
        abs_path = absolute_remote_path(path)
        
        paths = {item["FilePath"]: item for _,item in __CACHE_ITEMS__.items()}
        
        if not recursive and abs_path in paths:
            return paths[abs_path]
        else: 
            parts = split_path(abs_path) # has no ".." parts thanks to absolute_remote_path
            
            if parts[0] == os.sep:
                part_id = 0
                parts.pop(0)
            else:
                part_id = CWD["ID"]
            
            for p in parts:
                if verbose>1:
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
                if verbose>1:
                    print(f"Recursive get_item descends to {item['FilePath']}")
                
                children = get_children(item)
                
                if verbose>1:
                    print(f"\tand finds {len(children)} children.")
                
                for child in children:
                    if child["CategoryName"] in api.folder_types:
                        items.update(get_item(child, verbose, recursive))
                    else:
                        items[child["FilePath"]] = child
            elif verbose>1:
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
    LastModificationTime = datetime.datetime.fromtimestamp(os.path.getmtime(local_filename)).astimezone(tz.tzlocal())

    # Get the files' properties either from the folder it's in or the file itself
    # Depending on what was specified in remote_path (the containing folder or the file)
    if is_folder(remote_path):
        files = get_children(remote_path)
        
        sizes = {f["Name"]: int(f["Size"]) for f in files}
        times = {f["Name"]: int(f["LastUploadTime"]) for f in files}

        if Name in sizes:
            Remote_Size = sizes[Name] 
            LastUploadTime = datetime.datetime.utcfromtimestamp(int(times[Name])/1000).replace(tzinfo=tz.UTC).astimezone(tz.tzlocal())
        else:
            Remote_Size = 0
            LastUploadTime = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=tz.UTC).astimezone(tz.tzlocal())
    else:
        props =  get_item(remote_path)
        
        if props:
            Remote_Size = props["Size"]
            LastUploadTime = datetime.datetime.utcfromtimestamp(int(props["LastUploadTime"])/1000).replace(tzinfo=tz.UTC).astimezone(tz.tzlocal())
        else:
            Remote_Size = 0
            LastUploadTime = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=tz.UTC).astimezone(tz.tzlocal())
        
    if verbose>0:
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
            if verbose>0:
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
                _ = wget.download(URL, out=Name, size=Size, headers={'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/83.0.4103.61 Chrome/83.0.4103.61 Safari/537.36"})
            
                # The default wqet progress bar leaves cursor at end of line.
                # It fails to print a new line. This causing glitchy printing
                # Easy fixed, 
                print("")
    
            # Having downloaded the file chdir back to where we started
            os.chdir(cwd)
            
            return item["FilePath"]
        else:
            if verbose>1:
                if dry_run:
                    print(f"Would NOT download {Path}")
                else:
                    print(f"Not downloading {Path}")
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
        pass # all good, just use cwd
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
            if verbose>0:
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
    
    TODO: See if we can get a progress bar on this, like we do for the download wget.
    TODO: https://medium.com/google-cloud/google-cloud-storage-signedurl-resumable-upload-with-curl-74f99e41f0a2
    
    :param local_file:     The local file ((full or relative remote_folder)
    :param remote_folder:  The Degoo folder upload it to (must be a Folder, either relative or abolute path)
    :param verbose:        Print useful tracking/diagnostic information
    :param if_changed:     Only upload the local_file if it's changed
    :param dry_run:        Don't actually upload the local_file ... 
    :param schedule:       Respect the configured schedule (i.e upload only when schedule permits) 
    
    :returns: A tuple containing the Degoo ID, Remote file path and the download URL of the local_file.
    '''
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
    
    if verbose>1:
        print(f"Asked to upload {local_file} to {dir_path}: {if_changed=} {dry_run=}")
        
    # Upload only if:
    #    if_changed is False and dry_run is False (neither is true)
    #    if_changed is True and has_changed is true and dry_run is False
    if (not if_changed or has_changed(local_file, remote_folder, verbose-1)):
        if dry_run:
            if verbose>0:
                print(f"Would upload {local_file} to {dir_path}")
        else:
            if verbose>0:
                print(f"Uploading {local_file} to {dir_path}")
            # The steps involved in an upload are 4 and as follows:
            #
            # 1. Call getBucketWriteAuth2 to get the URL and parameters we need for upload
            # 2. Post to the BaseURL provided by that
            # 3. Call setUploadFile2 to inform Degoo it worked and create the Degoo item that maps to it
            # 4. Call getOverlay3 to fetch the Degoo item this created so we can see that worked (and return the download URL)
            
            MimeTypeOfFile = magic.Magic(mime=True).from_file(local_file)
        
            #################################################################
            ## STEP 1: getBucketWriteAuth2
            
            # Get the Authorisation to write to this directory
            # Provides the metdata we need for the upload   
            result = api.getBucketWriteAuth2(dir_id)
            
            #################################################################
            ## STEP 2: POST to BaseURL

            # Then upload the local_file to the nominated URL
            BaseURL = result["BaseURL"]
        
            # We now POST to BaseURL and the body is the local_file but all these fields too
            Signature =      result["Signature"]
            GoogleAccessId = result["AccessKey"]["Value"]
            CacheControl =   result["AdditionalBody"][0]["Value"]  # Only one item in list not sure why indexed
            Policy =         result["PolicyBase64"]
            ACL =            result["ACL"]
            KeyPrefix =      result["KeyPrefix"]  # Has a trailing / 
            
            # This one is a bit mysterious. The Key seems to be made up of 4 parts
            # separated by /. The first two are provided by getBucketWriteAuth2 as
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
            
            heads = {"ngsw-bypass": "1", "x-client-data": "CIS2yQEIpbbJAQipncoBCLywygEI97TKAQiXtcoBCO21ygEIjrrKAQ=="}
            
            # Perform the upload
            # TODO: Can we get a progress bar on the this? Web app has one.  
            response = requests.post(BaseURL, files=parts, headers=heads)

            # We expect a 204 status result, which is silent acknowledgement of success.
            if response.ok and response.status_code == 204:
                # Theeres'a Google Upload ID returned in the headers. Not sure what use it is.
                google_id = response.headers["X-GUploader-UploadID"]  # @UnusedVariable
                
                if verbose>1:
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
#                 # now being when setUploadFile2 on the server has as now and not the now we have 
#                 # here.
#                 #
#                 # The Signature is new, it's NOT the Signature that getBucketWriteAuth2 returned
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
#                 # I'd bet that setUploadFile2 given he checksum can build that string and
#                 # using the Degoo private key generate a signature. But alas it doestn't 
#                 # return one and so we need to use getOverlay3 to fetch it explicitly.
#                 expiry = str(int((datetime.datetime.utcnow() + datetime.timedelta(days=14)).timestamp()))
#                 expected_URL = "".join([  
#                             BaseURL.replace("storage-upload.googleapis.com/",""),
#                             Key,
#                             "?GoogleAccessId=", GoogleAccessId,
#                             "&Expires=", expiry,
#                             "&Signature=", Signature,
#                             "&use-cf-cache=true"]) # @UnusedVariable

                #################################################################
                ## STEP 3: setUploadFile2
                
                degoo_id = api.setUploadFile2(os.path.basename(local_file), dir_id, Size, Checksum)

                #################################################################
                ## STEP 4: getOverlay3

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
    IDs[Root] = mkdir(target_name, target_dir['ID'], verbose-1, dry_run)
    
    for root, dirs, files in os.walk(local_directory):
        # if local directory contains a head that is included in root then we don't want
        # it when we're making dirs on the remote (cloud) drive and remembering the
        # IDs of those dirs.
        if target_junk:
            relative_root = root.replace(target_junk+os.sep, "", 1)
        else:
            relative_root = root
        
        for name in dirs:
            Name = os.path.join(relative_root, name)
            
            IDs[Name] = mkdir(name, IDs[relative_root], verbose-1, dry_run)
            
        for name in files:
            Name = os.path.join(relative_root, name)
            
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

###########################################################################
# A Test hook

def test():
    localfile = "/home/bernd/workspace/Degoo/test_data/Image/Image 2.jpg"
    remotepath= "/Web/Test2"
    return has_changed(localfile, remotepath, verbose=1)
#     api.getSchema()
    
#     device_id = device_ids()["Web"]
#     path = ""
#     api.getFilesFromPaths(device_id, path)
