#!/usr/bin/python3
# encoding: utf-8
'''
Degoo commands -- Some CLI commands to interact with a Degoo cloud drive

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

import sys, os, degoo

from argparse import ArgumentParser

__all__ = []
__version__ = 0.1
__date__ = '2020-06-03'
__updated__ = '2020-06-03'

DEBUG = 1
TESTRUN = 0
PROFILE = 0

P = degoo.command_prefix

class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super().__init__(type(self))
        self.msg = f"Eror: {msg}" 
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    command = os.path.basename(sys.argv[0])

    program_version = f"v{__version__}" 
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    
    program_license = f'''{program_shortdesc}

  Created by Bernd Wechner on {str(__date__)}.
  Copyright 2020. All rights reserved.

  Licensed under The Hippocratic License 2.1
  https://firstdonoharm.dev/

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
'''

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license)
        parser.add_argument("-v", "--verbose", dest="verbose", action="count", help="set verbosity level [default: %(default)s]")
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        
        if command == P+"ls" or command == P+"ll":
            parser.add_argument('-l', '--long', action='store_true')
            parser.add_argument('-R', '--recursive', action='store_true')
            parser.add_argument('folder', help='The folder/path to list', nargs='?', default=degoo.CWD)
            args = parser.parse_args()
            
            if command == P+"ll":
                args.long = True
            
            degoo.ls(args.folder, args.long, args.recursive)
        
        elif command == P+"pwd":
            print(f"Working Directory is {degoo.CWD['Path']}")

        elif command == P+"props":
            parser.add_argument('path', help='The name, path, or ID of degoo item to return properties of (can be a device, folder, file).')
            args = parser.parse_args()
                        
            props = degoo.get_item(args.path)
            
            print(f"Properties of {args.path}:")
            for key, value in props.items():
                print(f"\t{key}: {value}")

        elif command == P+"path":
            parser.add_argument('path', help='The path to test.')
            args = parser.parse_args()
            
            print(f"Path is: {degoo.get_dir(args.path)}")
            
        elif command == P+"cd":
            parser.add_argument('folder', help='The folder/path to makre current.')
            args = parser.parse_args()

            cwd = degoo.cd(args.folder)
            
            print(f"Working Directory is now {cwd['Path']}")

        elif command == P+"tree":
            parser.add_argument('-t', '--times', action='store_true', help="Show timestamps")
            parser.add_argument('folder', nargs='?', help='The folder to put_file it in')
            args = parser.parse_args()
            degoo.tree(args.folder, args.times)

        elif command == P+"mkdir":
            parser.add_argument('folder', help='The folder/path to list')
            args = parser.parse_args()
            
            path = degoo.mkpath(args.folder)
            print(f"Created folder {path}")

        elif command == P+"rm":
            parser.add_argument('file', help='The file/folder/path to remove')
            args = parser.parse_args()
            
            path = degoo.rm(args.file)
            print(f"Deleted {path}")

        elif command == P+"get":
            parser.add_argument('file', help='The file/folder/path to get')
            args = parser.parse_args()
            
            degoo.get(args.file, args.verbose)

        elif command == P+"put":
            parser.add_argument('file', help='The file/folder/path to put')
            parser.add_argument('folder', nargs='?', help='The folder to put_file it in')
            args = parser.parse_args()
            
            (ID, URL) = degoo.put(args.file, args.folder, args.verbose)
            
            if URL:
                print(f"Uploaded to Degoo ID: {ID}, with Download URL\n{URL}")
            elif ID:
                print(f"Error: Uploaded to Degoo ID: {ID}, but no download URL was provided!")
            else:
                print(f"Error: Cannot upload {args.file}, it is not a File or Directory.")
            
        elif command == P+"login":
            degoo.login()

        elif command == P+"user":
            props = degoo.userinfo()
            print(f"Logged in user:")
            for key, value in props.items():
                print(f"\t{key}: {value}")

        return 0
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception as e:
        if DEBUG or TESTRUN:
            raise(e)
        indent = len(command) * " "
        sys.stderr.write(command + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

if __name__ == "__main__":
    if DEBUG:
        sys.argv.append("-v")
    if TESTRUN:
        import doctest
        doctest.testmod()
    if PROFILE:
        import cProfile
        import pstats
        profile_filename = 'commands_profile.txt'
        cProfile.run('main()', profile_filename)
        statsfile = open("profile_stats.txt", "wb")
        p = pstats.Stats(profile_filename, stream=statsfile)
        stats = p.strip_dirs().sort_stats('cumulative')
        stats.print_stats()
        statsfile.close()
        sys.exit(0)
    sys.exit(main())