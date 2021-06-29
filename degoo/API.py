###########################################################################
# A Python front end for the Degoo GraphQL API

import os
import sys
import json
import time
import datetime
import hashlib
import base64
import requests
import humanize

from shutil import copyfile
from appdirs import user_config_dir
from dateutil import parser


class API:
    ###########################################################################
    # URL configuration
    #
    # The URL the API is found at
    URL = "https://production-appsync.degoo.com/graphql"

    # The URLS used for logging in
    URL_login = "https://rest-api.degoo.com/login"

    # The URL used for register
    URL_REGISTER = "https://rest-api.degoo.com/register"

    ###########################################################################
    # Local files configuration
    #
    # Class properties, that can be altered on the class
    conf_dir = user_config_dir("degoo")

    cred_file = os.path.join(conf_dir, "credentials.json")
    keys_file = os.path.join(conf_dir, "keys.json")
    DP_file = os.path.join(conf_dir, "default_properties.txt")

    # Ensure the user configuration directory exists
    if not os.path.exists(conf_dir):
        os.makedirs(conf_dir)

    ###########################################################################
    # Empirically determined, largest value degoo supports for the Limit
    # on the Limit parameter to the GetFileChildren3 operation. It's used
    # for paging, and if more items exist there'll be a NextToken returned.
    # TODO: Determine the syntax and use of that NextToken so that paged
    # results can be fecthed reliably as well. For now we just make calls
    # with the max limit and avoid dealing with paging.
    LIMIT_MAX = int('1' * 31, 2) - 1

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
    # Updated in __init__
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

    class Error(Exception):
        '''Generic exception to raise and log different fatal errors.'''

        def __init__(self, msg):
            super().__init__(type(self))
            self.msg = msg

        def __str__(self):
            return self.msg

        def __unicode__(self):
            return self.msg

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
        if os.path.isfile(self.keys_file):
            with open(self.keys_file, "r") as file:
                keys = json.loads(file.read())

        self.KEYS = keys

        if os.path.isfile(self.DP_file):
            with open(self.DP_file, "r") as file:
                self.PROPERTIES = file.read()

        self.CATLEN = max([len(n) for _, n in self.CATS.items()])  # @ReservedAssignment

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
        date_format = "%Y-%m-%d %H:%M:%S"
        no_date = "Unavailable"

        # Add a set of Human Readable timestamps
        if creation:
            c_time = creation
            c_datetime = parser.parse(c_time)
            c_dt = c_datetime.strftime(date_format)
        else:
            c_dt = no_date

        if modification:
            m_secs = int(modification) / 1000
            m_datetime = datetime.datetime.utcfromtimestamp(m_secs)
            m_dt = m_datetime.strftime(date_format)
        else:
            m_dt = no_date

        if upload:
            u_secs = int(upload) / 1000
            u_datetime = datetime.datetime.utcfromtimestamp(u_secs)
            u_dt = u_datetime.strftime(date_format)
        else:
            u_dt = no_date

        return (c_dt, m_dt, u_dt)

    def check_sum(self, filename, blocksize=65536):
        '''
        When uploading files Degoo uses a 2 step process:
            1) Get Authorisation from the Degoo API - provides metadate needed for step 2
            2) Upload the file to a nominated URL (in practice this appears to be Google Cloud Services)

        The upload to Google Cloud services wants a checksum for the file (for upload integrity assurance)

        This appears to a base64 encoded SHA1 hash of the file. Empirically this, with a little dressing
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

    __devices__ = None

    @property
    def devices(self):
        '''
        Returns a dictionary of devices, keyed on Degoo ID, containing the name of the device.

        Top level folders in the Degoo Filesystem are called devices.

        TODO: Degoo's web interface does not currently allow creation of devices even when licensed to.
        Thus we have no way of working out an API call that does so and we're stuck with devices they
        give us (even when licensed to have as many as you like).
        '''
        if self.__devices__ is None:
            devices = {}
            root = self.getFileChildren3(0)
            for d in root:
                if d['CategoryName'] == "Device":
                    devices[int(d['DeviceID'])] = d['Name']

            self.__devices__ = devices
            return devices
        else:
            return self.__devices__

    ###########################################################################
    # # Login

    def login(self, username=None, password=None):
        '''
        Logs into a Degoo account.

        The login is lasting, i.e. does not seem to expire (not subject to any autologout)

        The reply provides a couple of keys that must be provided with each subsequent
        API call, as authentication, to prove we're logged in. These are written in JSON
        format to keys_file which is by default: ~/.config/degoo/keys.json

        TODO: Support logout as well (which will POST a logout request and remove these keys)

        :returns: True if successful, False if not
        '''
        CREDS = {}
        if username and password:
            CREDS = {"Username": username, "Password": password}
        elif os.path.isfile(self.cred_file):
            with open(self.cred_file, "r") as file:
                CREDS = json.loads(file.read())

        if CREDS:
            response = requests.post(self.URL_login, data=json.dumps(CREDS))

            if response.ok:
                rd = json.loads(response.text)

                keys = {"Token": rd["Token"], "x-api-key": self.API_KEY}

                # Store the token and API key for later use
                with open(self.keys_file, "w") as file:
                    file.write(json.dumps(keys) + '\n')

                # If a username/password were provided, remember them for future use
                if username and password:
                    with open(self.cred_file, "w") as file:
                        CREDS = file.write(json.dumps(CREDS) + '\n')

                # Once logged in, make sure self.DP_file exists
                if not os.path.isfile(self.DP_file):
                    source_file = os.path.basename(self.DP_file)
                    if os.path.isfile(source_file):
                        copyfile(source_file, self.DP_file)
                    else:
                        print(f"No properties are configured or available. If you can find the supplied file '{source_file}' copy it to '{self.DP_file}' and try again.")

                return True
            else:
                return False
        else:
            with open(self.cred_file, "w") as file:
                file.write(json.dumps({"Username": "<your Degoo username here>", "Password": "<your Degoo password here>"}) + '\n')

            print(f"No login credentials available. Please provide some or add account details to {self.cred_file}", file=sys.stderr)
    

    ###########################################################################
    # # Register

    def register(self, username=None, password=None):
        '''
        Register a new Degoo account 

        This API call will create a new degoo account with provided credentials and automatically logins to your account and
        returns a login token

        Note:
           If the provided email is already registered with an exciting Degoo account , It will directly logins to your existing Degoo account.
           So we can use it as a login method if we have an account already 😉

        '''
        CREDS = {}
        if username and password:
            CREDS = {"Username":username,"Password":password,"LanguageCode":"en-US","CountryCode":"US","Source":"Web App"}
        elif os.path.isfile(self.cred_file):
            with open(self.cred_file, "r") as file:
                CREDS = json.loads(file.read())

        if CREDS:
            response = requests.post(self.URL_REGISTER, data=json.dumps(CREDS))

            if response.ok:
                rd = json.loads(response.text)

                keys = {"Token": rd["Token"], "x-api-key": self.API_KEY}

                # Store the token and API key for later use
                with open(self.keys_file, "w") as file:
                    file.write(json.dumps(keys) + '\n')

                # If a username/password were provided, remember them for future use
                if username and password:
                    with open(self.cred_file, "w") as file:
                        CREDS = file.write(json.dumps(CREDS) + '\n')

                # Once Register and logged in, make sure self.DP_file exists
                if not os.path.isfile(self.DP_file):
                    source_file = os.path.basename(self.DP_file)
                    if os.path.isfile(source_file):
                        copyfile(source_file, self.DP_file)
                    else:
                        print(f"No properties are configured or available. If you can find the supplied file '{source_file}' copy it to '{self.DP_file}' and try again.")

                return True
            else:
                return False
        else:
            with open(self.cred_file, "w") as file:
                file.write(json.dumps({"Username": "<your Degoo username here>", "Password": "<your Degoo password here>"}) + '\n')

            print(f"No credentials available. Please provide some or add account details to {self.cred_file}", file=sys.stderr)


    ###########################################################################
    # # GRAPHQL Wrappers

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

        response = requests.post(self.URL, headers=header, data=json.dumps(request))

        if response.ok:
            rd = json.loads(response.text)

            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise self.Error(f"getUserInfo failed with: {message}")
            else:
                properties = rd["data"]["getUserInfo"]

            if properties:
                if humanise:
                    properties['AccountType'] = self.PLANS.get(properties['AccountType'], properties['AccountType'])
                    properties['UsedQuota'] = humanize.naturalsize(int(properties['UsedQuota']))
                    properties['TotalQuota'] = humanize.naturalsize(int(properties['TotalQuota']))
                    del properties['__typename']

                return properties
            else:
                return {}
        else:
            raise self.Error(f"getUserInfo failed with: {response}")

    def getOverlay3(self, degoo_id):
        '''
        A Degoo Graph API call: gets information about a degoo item identified by ID.

        A Degoo item can be a file or folder but may not be limited to that (see self.CATS).

        :param degoo_id: The ID of the degoo item.
        '''
        # args = self.PROPERTIES.replace("Size\n", "Size\nHash\n")
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

        response = requests.post(self.URL, headers=header, data=json.dumps(request))

        if response.ok:
            rd = json.loads(response.text)

            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise self.Error(f"getOverlay3 failed with: {message}")
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
                        properties["FilePath"] = f"{os.sep}{self.devices[properties['DeviceID']]}{os.sep}Recycle Bin"
                else:
                    # FilePath includes neither the Device name nor Recylce Bin alas. We
                    # patch those in here to provide a FilePath that is complete and
                    # compariable with the web interface UX.
                    binned = properties["IsInRecycleBin"]
                    prefix = self.devices[properties['DeviceID']] + os.sep + "Recycle Bin" if binned else self.devices[properties['DeviceID']]
                    properties["FilePath"] = f"{os.sep}{prefix}{properties['FilePath'].replace('/',os.sep)}"

                # Convert ID and Sizeto an int.
                properties["ID"] = int(properties["ID"])
                properties["ParentID"] = int(properties["ParentID"]) if properties["ParentID"] else 0
                properties["MetadataID"] = int(properties["MetadataID"])
                properties["Size"] = int(properties["Size"])

                # Add a set of Human Readable time stamps based om the less readable API timestamps
                times = self._human_readable_times(properties['CreationTime'], properties['LastModificationTime'], properties['LastUploadTime'])

                properties["Time_Created"] = times[0]
                properties["Time_LastModified"] = times[1]
                properties["Time_LastUpload"] = times[2]

                return properties
            else:
                return {}
        else:
            raise self.Error(f"getOverlay3 failed with: {response.text}")

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

        response = requests.post(self.URL, headers=header, data=json.dumps(request))

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
                    raise self.Error(f"getFileChildren3 failed with: {message}")
            else:
                items = rd["data"]["getFileChildren3"]["Items"]

                if items:
                    next_token = rd["data"]["getFileChildren3"]["NextToken"]
                    if next_token:
                        # TODO: Work out what to do in this case.
                        print(f"WARNING: PAGINATION ISSUE, NextToken={next_token}", file=sys.stderr)

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
                        for i in items:
                            binned = i["IsInRecycleBin"]
                            cat = self.CATS.get(i['Category'], f"Category {i['Category']}")
                            i["CategoryName"] = cat

                            # Fix the FilePath from Degoo incompleteness to a complete path.
                            if cat in ["Device", "Recycle Bin"]:
                                if cat == "Device":
                                    i["FilePath"] = f"{os.sep}{i['Name']}"
                                elif cat == "Recycle Bin":
                                    i["FilePath"] = f"{os.sep}{self.devices[i['DeviceID']]}{os.sep}Recycle Bin"
                            else:
                                # FilePath includes neither the Device name nor Recylce Bin alas. We
                                # patch those in here to provide a FilePath that is complete and
                                # compariable with the web interface UX.
                                binned = i["IsInRecycleBin"]
                                prefix = self.devices[i['DeviceID']] + os.sep + "Recycle Bin" if binned else self.devices[i['DeviceID']]
                                i["FilePath"] = f"{os.sep}{prefix}{i['FilePath'].replace('/',os.sep)}"

                    # Convert ID to an int.
                    for i in items:
                        i["ID"] = int(i["ID"])
                        i["ParentID"] = int(i["ParentID"]) if i["ParentID"] else 0
                        i["MetadataID"] = int(i["MetadataID"])
                        i["Size"] = int(i["Size"])

                        # Add a set of Human Readable time stamps based om the less readable API timestamps
                        times = self._human_readable_times(i['CreationTime'], i['LastModificationTime'], i['LastUploadTime'])

                        i["Time_Created"] = times[0]
                        i["Time_LastModified"] = times[1]
                        i["Time_LastUpload"] = times[2]

                    self.NAMELEN = max([len(i["Name"]) for i in items])

                return items
        else:
            raise self.Error(f"getFileChildren3 failed with: {response}")

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

        response = requests.post(self.URL, headers=header, data=json.dumps(request))

        if response.ok:
            rd = json.loads(response.text)

            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise self.Error(f"getFilesFromPaths failed with: {message}")
            else:
                # Debugging output
                print("Response Headers:", file=sys.stderr)
                for h in response.headers:
                    print(f"\t{h}: {response.headers[h]}", file=sys.stderr)

                rd = json.loads(response.text)
                items = rd["data"]["getFilesFromPaths"]
                return items
        else:
            raise self.Error(f"getFilesFromPaths failed with: {response}")

    def setDeleteFile5(self, degoo_id):
        '''
        A Degoo Graph API call: Deletes a Degoo item identified by ID. It is moved to the Recycle Bin
        for the device it was on, and this is not a secure delete. It must be expolicitly deleted
        from the Recylce bin to be a secure delete.

        #TODO: support an option to find and delete the file in the recycle bin,

        :param degoo_id: The ID of a Degoo item to delete.
        '''
        func = "setDeleteFile5(Token: $Token, IsInRecycleBin: $IsInRecycleBin, IDs: $IDs)"
        query = f"mutation SetDeleteFile5($Token: String!, $IsInRecycleBin: Boolean!, $IDs: [IDType]!) {{ {func} }}"

        request = { "operationName": "SetDeleteFile5",
                    "variables": {
                        "Token": self.KEYS["Token"],
                        "IDs": [{ "FileID": degoo_id }],
                        "IsInRecycleBin": False,
                        },
                    "query": query
                   }

        header = {"x-api-key": self.KEYS["x-api-key"]}

        response = requests.post(self.URL, headers=header, data=json.dumps(request))

        if response.ok:
            rd = json.loads(response.text)

            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise self.Error(f"setDeleteFile5 failed with: {message}")
            else:
                return response.text
        else:
            raise self.Error(f"setDeleteFile5 failed with: {response}")

    def setRenameFile(self, file_id, new_name):
        ''''
        Rename a file or folder

        :param file_id Id of file or directory
        :param new_name: New name of file or folder
        :return: Message with result of operation
        '''

        func = "setRenameFile(Token: $Token, FileRenames: $FileRenames)"
        query = f"mutation SetRenameFile($Token: String!, $FileRenames: [FileRenameInfo]!) {{ {func} }}"

        request = {"operationName": "SetRenameFile",
                   "variables": {
                       "Token": self.KEYS["Token"],
                       "FileRenames": [{
                           "ID": file_id,
                           "NewName": new_name
                       }]
                   },
                   "query": query
                   }

        header = {"x-api-key": self.KEYS["x-api-key"]}

        response = requests.post(self.URL, headers=header, data=json.dumps(request))

        if response.ok:
            rd = json.loads(response.text)

            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise self.Error(f"setRenameFile failed with: {message}")
            elif 'data' in rd and rd['data'].get('setRenameFile', False):
                return file_id  # The ID of the renamed file (its ID does not change)
            else:
                raise self.Error(f"setRenameFile failed with unknwon reasons: {rd}")
        else:
            raise self.Error(f"setRenameFile failed with: {response}")

    def setMoveFile(self, file_id, new_parent_id):
        """
        Move a file or folder to new destination

        :param file_id Id of file or directory
        :param new_parent_id: Id of destination path
        :return: Message with result of operation
        """
        func = "setMoveFile(Token: $Token, Copy: $Copy, NewParentID: $NewParentID, FileIDs: $FileIDs)"
        query = f"mutation SetMoveFile($Token: String!, $Copy: Boolean, $NewParentID: String!, $FileIDs: [String]!) {{ {func} }}"

        request = {"operationName": "SetMoveFile",
                   "variables": {
                       "Token": self.KEYS["Token"],
                       "NewParentID": new_parent_id,
                       "FileIDs": [
                           file_id
                       ]
                   },
                   "query": query
                   }

        header = {"x-api-key": self.KEYS["x-api-key"]}

        response = requests.post(self.URL, headers=header, data=json.dumps(request))

        if response.ok:
            rd = json.loads(response.text)

            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise self.Error(f"setMoveFile failed with: {message}")
            elif 'data' in rd and rd['data'].get('setMoveFile', False):
                return file_id  # The ID of the moved file (its ID does not change)
            else:
                raise self.Error(f"setMoveFile failed with unknwon reasons: {rd}")
        else:
            raise self.Error(f"setMoveFile failed with: {response}")

    def setUploadFile3(self, name, parent_id, size="0", checksum="CgAQAg"):
        '''
        A Degoo Graph API call: Appears to create a file in the Degoo filesystem.

        Directories are created with this alone, but files it seems are not stored
        on the Degoo filesystem at all, just their metadata is, the actual file contents
        are stored on a Google Cloud Service.

        To Add a file means to call getBucketWriteAuth4 to start the process, then
        upload the file content, then finally, create the Degoo file item that then
        points to the actual file data with a URL. setUploadFile3 does not return
        that URL, but getOverlay3 does.

        :param name:        The name of the file
        :param parent_id:   The Degoo ID of the Folder it will be placed in
        :param size:        The files size
        :param checksum:    The files checksum (see self.check_sum)
        '''
        func = "setUploadFile3(Token: $Token, FileInfos: $FileInfos)"
        query = f"mutation SetUploadFile3($Token: String!, $FileInfos: [FileInfoUpload3]!) {{ {func} }}"

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
        request = { "operationName": "SetUploadFile3",
                    "variables": {
                        "Token": self.KEYS["Token"],
                        "FileInfos": [{
                            "Checksum": checksum,
                            "Name": name,
                            "CreationTime": int(1000 * time.time()),
                            "ParentID": parent_id,
                            "Size": size
                        }]
                        },
                    "query": query
                   }

        header = {"x-api-key": self.KEYS["x-api-key"]}

        response = requests.post(self.URL, headers=header, data=json.dumps(request))

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
                raise self.Error(f"setUploadFile3 failed with: {message}")
            else:
                contents = self.getFileChildren3(parent_id)
                ids = {f["Name"]: int(f["ID"]) for f in contents}
                if not name in ids:
                    parent = self.getOverlay3(parent_id)
                    print(f"WARNING: Failed to find {name} in {parent['FilePath']} after upload.", file=sys.stderr)
                return ids[name]

        else:
            raise self.Error(f"setUploadFile3 failed with: {response}")

    def getBucketWriteAuth4(self, dir_id):
        '''
        A Degoo Graph API call: Appears to kick stat the file upload process.

        Returns crucial information for actually uploading the file.
        Not least the URL to upload it to!

        :param dir_id:
        '''
        kv = " {Key Value __typename}"
        args = "\n".join(["PolicyBase64", "Signature", "BaseURL", "KeyPrefix", "AccessKey" + kv, "ACL", "AdditionalBody" + kv, "__typename"])
        func = f"getBucketWriteAuth4(Token: $Token, ParentID: $ParentID, StorageUploadInfos: $StorageUploadInfos) {{ AuthData {{ {args} }} }}"
        query = f"query GetBucketWriteAuth4($Token: String!, $ParentID: String!, $StorageUploadInfos: [StorageUploadInfo2]) {{ {func} }}"

        request = { "operationName": "GetBucketWriteAuth4",
                    "variables": {
                        "Token": self.KEYS["Token"],
                        "ParentID": f"{dir_id}",
                        "StorageUploadInfos":[]
                        },
                    "query": query
                   }

        header = {"x-api-key": self.KEYS["x-api-key"]}

        response = requests.post(self.URL, headers=header, data=json.dumps(request))

        if response.ok:
            rd = json.loads(response.text)

            if "errors" in rd:
                messages = []
                for error in rd["errors"]:
                    messages.append(error["message"])
                message = '\n'.join(messages)
                raise self.Error(f"getBucketWriteAuth4 failed with: {message}")
            else:
                # The index 0 suggests maybe if we upload multiple files we get_file mutipple WriteAuths back
                RD = rd["data"]["getBucketWriteAuth4"][0]

                return RD
        else:
            raise self.Error(f"getBucketWriteAuth4 failed with: {response}")

    def getSchema(self):
        '''
        Experimental effort to probe the GraphQL Schema that Degoo provide.

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

        response = requests.post(self.URL, headers=header, data=json.dumps(request))

        if not response.ok:
            raise self.Error(f"getSchema failed with: {response.text}")
        else:
            return response
