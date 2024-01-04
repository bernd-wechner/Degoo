###########################################################################
# A Python front end for the Degoo GraphQL API

import os
import re
import sys
import json
import time
import datetime
import hashlib
import base64
import humanize
import requests

from requests import Request, Session
from shutil import copyfile
from appdirs import user_config_dir
from dateutil import parser
from collections import OrderedDict
from curl_cffi import requests


class API:
    ###########################################################################
    # URL configuration
    #
    # The URL the API is found at
    URL = "https://production-appsync.degoo.com/graphql"

    # The URLS used for logging in
    URL_login = "https://rest-api.degoo.com/login"
    
    # The URL used for register
    URL_register = "https://rest-api.degoo.com/register"
    
    # The URL used to get an access token
    URL_token = "https://rest-api.degoo.com/access-token/v2"

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
    # The USER Agent to use on web requests. Degoo can be quite picky about
    # this rejecting attempts to connect or interact if it's wrong. A point
    # of weakness in the API.
    USER_AGENT = 'Degoo-client/0.4'
    USER_AGENT_FIREFOX = 'Mozilla/5.0 (X11; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0'
    USER_AGENT_CHROME = 'Mozilla/5.0 Slackware/13.37 (X11; U; Linux x86_64; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/11.0.696.50'

    ###########################################################################
    # Empirically determined, largest value degoo supports for the Limit
    # on the Limit parameter to the GetFileChildren5 operation. It's used
    # for paging, and if more items exist there'll be a NextToken returned.
    #
    # They did support this:
    # LIMIT_MAX = int('1' * 31, 2) - 1
    # And now only support a max of 1000.
    # Anything higher sees this error returned: getFileChildren5 failed with: Too large input!
    LIMIT_MAX = 1000

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
    # Used when listing files, updated to the width needed to display
    # the longest filename. Updated by getFileChildren5 when it returns
    # a list of filenames.
    NAMELEN = 20

    # Width of Size field for text output we produce
    # Used when listing files
    SIZELEN = 10

    # A string used to mark redacted output
    REDACTED = "<redacted>"

    # A list of Degoo Item properties. The three API calls:
    #    getOverlay4
    #    getFileChildren5
    #    getFilesFromPaths
    # all want a list of explicit properties it seems, that they will
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
    PROPERTIES_ALL = ""

    @classmethod
    def report_config(cls):
        print(f"Degoo configurations are stored in: {cls.conf_dir}")
        print(f"After successful login:")
        print(f"\tyour login credentials are stored in: {cls.cred_file}")
        print(f"\tyour API access keys are stored in: {cls.keys_file}")
        print(f"Default properties requested when getting remote file information are stored in: {cls.DP_file}")
        print("API configurations:")
        print(f"\tlogin URL is: {cls.URL_login}")
        print(f"\tGraphQL URL is: {cls.URL}")
        print(f"\tUser-Agent is: {cls.USER_AGENT}")
        print(f"\tAPI key: {cls.API_KEY}")
        print("")  # Blank line

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
                self.PROPERTIES_ALL = file.read()
                
                # GetOverlay seems to barf with FieldUndefined on the following properties
                GetOverlay4_Unsupported = ("Distance", "OptimizedURL", "Country", "Province", "Place", "Location", "GeoLocation", "IsShared", "ShareTime")
                # The props listed can be listed with subprops in { }  
                # so we need to cull them pretty brute force splitting 
                # and checking first word.
                ol4_props = []
                for prop in self.PROPERTIES_ALL.split("\n"):
                    if not re.split(r'\W', prop)[0] in GetOverlay4_Unsupported:
                        ol4_props.append(prop)
                self.PROPERTIES_GetOverlay4 = "\n".join(ol4_props)

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
            root = self.getAllFileChildren5(0)
            for d in root:
                if d['CategoryName'] == "Device":
                    devices[int(d['DeviceID'])] = d['Name']

            self.__devices__ = devices
            return devices
        else:
            return self.__devices__

    ###########################################################################
    # # Login

    def login(self, username=None, password=None, register=False, verbose=0, redacted=False):
        '''
        Logs into a Degoo account.

        The login is lasting, i.e. does not seem to expire (not subject to any autologout)

        The reply provides a couple of keys that must be provided with each subsequent
        API call, as authentication, to prove we're logged in. These are written in JSON
        format to keys_file which is by default: ~/.config/degoo/keys.json

        Note: The register URL provides functionality that is a superset of the login URL.
        meaning if the username and password are already registered it simply logs in. Some
        folk report more success using this URL than the login URL. Login issues continue
        to plague us alas and so this method isriddled with debgging code,

        TODO: Support logout as well (which will POST a logout request and remove these keys)

        :param username: A string being a username which is the form af an email address
        :param password: A string which contains the Degoo passsword for the username
        :param register: Use the register URL not the login URL
        :param verbose: An int describing bhte level of verbosity
        :param redacted: redacts the verbose output to hide passwords
        :returns: True if successful, False if not
        '''
        CREDS = {}
        if username and password:
            CREDS = {"Username": username, "Password": password}
        elif os.path.isfile(self.cred_file):
            with open(self.cred_file, "r") as file:
                CREDS = json.loads(file.read())

        if CREDS:
            # Last Firefox login submmission observation (from successful login 30/3/2002):
            #
            # POST /login HTTP/1.1
            # Host: rest-api.degoo.com
            # User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0
            # Accept: */*
            # Accept-Language: en-US,en;q=0.5
            # Accept-Encoding: gzip, deflate, br
            # Content-Type: application/json
            # Content-Length: 82
            # Referer: https://app.degoo.com/
            # Origin: https://app.degoo.com
            # Sec-Fetch-Dest: empty
            # Sec-Fetch-Mode: cors
            # Sec-Fetch-Site: same-site
            # DNT: 1
            # Sec-GPC: 1
            # Connection: keep-alive
            #
            # BODY:
            # {
            #     "GenerateToken": true,
            #     "Password": "mypassword",
            #     "Username": "myemail"
            # }

            headers = OrderedDict([
                ('User-Agent', self.USER_AGENT_CHROME),
                ('Accept', '*/*'),
                ('Accept-Language', 'en-US,en;q=0.5'),
                ('Accept-Encoding', 'gzip, deflate, br'),
                ('Content-Type', 'application/json'),
                ('Referer', 'https://app.degoo.com/'),
                ('Origin', 'degoo.com/CgQxMjE1'),
                ('x-amz-content', 'https://app.degoo.com'),
                ('x-api-authentication', 'iNDNjhDZzYWMxQTNtM2Y0IWLxIjY00COzcDNtYGN2Y2Y1gzM'),
                ('x-version', 'DegooWebClient/1.0:2022.11.11'),
                ("Sec-Fetch-Dest", "empty"),
                ("Sec-Fetch-Mode", "cors"),
                ("Sec-Fetch-Site", "same-site"),
                ("DNT", "1"),
                ("Sec-GPC", "1"),
                ("Connection", "keep-alive")
            ])

            URL = self.URL_register if register else self.URL_login

            # An effort to replicate what Firefox sees as a the body precisely (json.dumps adds spaces after the colons and comma)
            username = CREDS["Username"]
            password = CREDS["Password"]
            body = f'{{"GenerateToken":true,"Username":"{username}","Password":"{password}"}}'

            
            fiddler = False # A little test option using a local insall of fiddler to watch the traffic
            impersonate = "chrome110" # Use curl_cffi to impersonate a given browser's JA3 signature
            if fiddler:
                proxies = {"http": "http://127.0.0.1:8866", "https":"http:127.0.0.1:8866"}
                response = requests.post(URL, headers=headers, data=json.dumps(CREDS), proxies=proxies, verify=False)
            elif impersonate:
                s = requests.Session()
                response = requests.post(URL, data=body, headers=headers, impersonate=impersonate)
            else:
                s = Session()
                r = Request('POST', URL, data=body, headers=headers)
                R = r.prepare()
                response = s.send(R)

                # breakpoint()
                # response = requests.post(self.URL_login, headers=headers, data=json.dumps(CREDS))

            if verbose > 2:
                print(f"Request:")
                print(f"\tURL: {response.request.url}", file=sys.stderr)
                print(f"\tmethod: {response.request.method}", file=sys.stderr)
                print(f"\theaders:", file=sys.stderr)
                for k, i in response.request.headers.items():
                    print(f"\t\t{k}: {i}", file=sys.stderr)
                print(f"\tbody:", file=sys.stderr)
                if hasattr(response.request, "body"):
                    body = response.request.body   
                if redacted:
                    body = re.sub('"(Username|Password)":".*?"', fr'"\1":"{self.REDACTED}"', body)            
                print(f"\t\t{body}", file=sys.stderr)

                print(f"Response:", file=sys.stderr)
                print(f"\tstatus: {response.status_code}", file=sys.stderr)
                print(f"\treason: {response.reason}", file=sys.stderr)
                print(f"\theaders:", file=sys.stderr)
                for k, i in response.headers.items():
                    print(f"\t\t{k}: {i}", file=sys.stderr)
                print(f"\tcontent:", file=sys.stderr)
                content = response.content if not redacted else re.sub('"Token":".*?"', f'"Token":"{self.REDACTED}"', str(response.content))
                print(f"\t\t{content}", file=sys.stderr)

            if response.ok:
                rd = json.loads(response.text)

                # Degoo login returned a Token, but now it seems to return a RegfreshToken
                # Requiring a second request for an API token! 
                if "Token" in rd:
                    token = rd["Token"]
                elif "RefreshToken" in rd:
                    URL = self.URL_token
                    refresh_token = rd["RefreshToken"]
                    body = f'{{"RefreshToken":"{refresh_token}"}}'
                    if impersonate:
                        response = requests.post(URL, data=body, headers=headers, impersonate=impersonate)
                    else:
                        r = Request('POST', URL, data=body, headers=headers)
                        R = r.prepare()
                        response = s.send(R)
                        
                    rd = json.loads(response.text)
                    if "AccessToken" in rd:
                        token = rd["AccessToken"]
                    else: 
                        token = None
                
                keys = {"Token": token, "x-api-key": self.API_KEY}

                if not keys["Token"]:
                    raise self.Error(f"Login failed to retrieve a session token.")

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
                print(f"Login failed with: {response.status_code}: {response.reason}", file=sys.stderr)
                return False
        else:
            with open(self.cred_file, "w") as file:
                file.write(json.dumps({"Username": "<your Degoo username here>", "Password": "<your Degoo password here>"}) + '\n')

            print(f"No login credentials available. Please provide some or add account details to {self.cred_file}", file=sys.stderr)

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

    def getOverlay4(self, degoo_id):
        '''
        A Degoo Graph API call: gets information about a degoo item identified by ID.

        A Degoo item can be a file or folder but may not be limited to that (see self.CATS).

        :param degoo_id: The ID of the degoo item.
        '''
        # args = self.PROPERTIES_ALL.replace("Size\n", "Size\nHash\n")
        args = f"{self.PROPERTIES_GetOverlay4}"
        func = f"getOverlay4(Token: $Token, ID: $ID) {{ {args} }}"
        query = f"query GetOverlay4($Token: String!, $ID: IDType!) {{ {func} }}"

        request = { "operationName": "GetOverlay4",
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
                raise self.Error(f"getOverlay4 failed with: {message}")
            else:
                properties = rd["data"]["getOverlay4"]

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
            raise self.Error(f"getOverlay4 failed with: {response.text}")

    def getFileChildren5(self, dir_id, pagination_token=None):
        '''
        A Degoo Graph API call: gets the contents of a Degoo directory (the children of a Degoo item that is a Folder)

        :param dir_id: The ID of a Degoo Folder item (might work for other items too?)
        :param pagination_token: Returned by a previous call that hit the pagination limit. Continues the enumeration of children.

        :returns: A tuple of (
            a list of property dictionaries, one for each child, contianing the properties of that child
            | an optional token string that should be passed to a subsequent call in order to continue the enumeration
        )
        '''
        args = f"Items {{ {self.PROPERTIES_ALL} }} NextToken"
        func = f"getFileChildren5(Token: $Token ParentID: $ParentID AllParentIDs: $AllParentIDs Limit: $Limit Order: $Order NextToken: $NextToken) {{ {args} }}"
        query = f"query GetFileChildren5($Token: String! $ParentID: String $AllParentIDs: [String] $Limit: Int! $Order: Int! $NextToken: String  ) {{ {func} }}"

        variables = {
                        "Token": self.KEYS["Token"],
                        "ParentID": f"{dir_id}",
                        "Limit": self.LIMIT_MAX,
                        "Order": 3
        }
        
        if pagination_token:
            # It always seems to be the last filename returned, prepended by a constant string, but it seems better
            #  if we just pass exactly what the last call returned
            variables["NextToken"] = pagination_token
            
        request = { "operationName": "GetFileChildren5",
                    "variables": variables,
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
                    return ([], None)
                else:
                    message = '\n'.join(messages)
                    raise self.Error(f"getFileChildren5 failed with: {message}")
            else:
                items = rd["data"]["getFileChildren5"]["Items"]
                next_pagination_token = None

                if items:
                    next_token = rd["data"]["getFileChildren5"]["NextToken"]
                    if next_token:
                        next_pagination_token = next_token

                    # Fix FilePath by prepending it with a Device name.and converting
                    # / to os.sep so it becomes a valid os path as well.
                    if dir_id == 0:
                        for i in items:
                            i["FilePath"] = f"{os.sep}{i['Name']}"
                            i["CategoryName"] = self.CATS.get(i['Category'], i['Category'])
                    else:
                        # Get the device names if we're not getting a root dir
                        # device_names calls back here (i.e. uses the getFileChildren5 API call)
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

                return (items, next_pagination_token)
        else:
            raise self.Error(f"getFileChildren5 failed with: {response}")

    def getAllFileChildren5(self, dir_id):
        '''
        Almost identical to getFileChildren5: handles pagination and returns all the children.
        '''
        items = []
        next_token = None
        while True:
            (next_items, next_token) = self.getFileChildren5(dir_id, next_token)
            if next_items:
                items.extend(next_items)
            if not next_token:
                break
        return items

    def getFilesFromPaths(self, device_id, path=""):
        '''
        A Degoo Graph API call: Not sure what this API call is for to be honest.

        It's called after an upload for some reason. But if seems to offer nothing of value.

        :param device_id: A Degoo device ID
        :param path:      Don't know what this is.
        '''
        args = f"{self.PROPERTIES_ALL}"
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
        that URL, but getOverlay4 does.

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
                contents = self.getAllFileChildren5(parent_id)
                ids = {f["Name"]: int(f["ID"]) for f in contents}
                if not name in ids:
                    parent = self.getOverlay4(parent_id)
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
        # {"operationName": "GetOverlay4", "variables": {"Token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOiIyMTU2Mjc4OCIsIm5iZiI6MTU5MTE1MjU1NCwiZXhwIjoxNjIyNjg4NTU0LCJpYXQiOjE1OTExNTI1NTR9.ZMbZ_Y_7bLQ_eerFssbKEr-QdujQ_p3LENeeKF-Niv4", "ID": {"FileID": 12589739461}}, "query": "query GetOverlay4($Token: String!, $ID: IDType!) { getOverlay4(Token: $Token, ID: $ID) { ID\\nMetadataID\\nUserID\\nDeviceID\\nMetadataKey\\nName\\nFilePath\\nLocalPath\\nURL\\nOptimizedURL\\nThumbnailURL\\nCreationTime\\nLastModificationTime\\nLastUploadTime\\nParentID\\nCategory\\nSize\\nPlatform\\nDistance\\nIsSelfLiked\\nLikes\\nIsHidden\\nIsInRecycleBin\\nDescription\\nCountry\\nProvince\\nPlace\\nLocation\\nLocation2 {Country Province Place __typename}\\nGeoLocation {Latitude Longitude __typename}\\nData\\nDataBlock\\nCompressionParameters\\nIsShared\\nShareTime\\nShareinfo {ShareTime __typename}\\n__typename\\n\\n } }"}

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
