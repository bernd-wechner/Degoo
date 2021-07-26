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

import degoo
import os
import sys
import textwrap
import traceback

from argparse import ArgumentParser, HelpFormatter

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
        self.msg = f"Error: {msg}"

    def __str__(self):
        return self.msg

    def __unicode__(self):
        return self.msg


class RawFormatter(HelpFormatter):

    def _fill_text(self, text, width, indent):
        return "\n".join([textwrap.fill(line, width) for line in textwrap.indent(textwrap.dedent(text), indent).splitlines()])


def main(argv=None):  # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    command = os.path.basename(sys.argv[0])

    if not command.startswith(P):
        command = sys.argv[0] = P + sys.argv[1]
        sys.argv.pop(1)

    program_version = f"v{__version__}"
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]

    program_license = f'''
        {program_shortdesc}

        Created by Bernd Wechner on {str(__date__)}.
        Copyright 2020. All rights reserved.

        Licensed under The Hippocratic License 2.1
        https://firstdonoharm.dev/

        Distributed on an "AS IS" basis without warranties
        or conditions of any kind, either express or implied.

        USAGE:
        '''

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawFormatter)
        parser.add_argument("-v", "--verbose", action="count", default=0, help="set verbosity level [default: %(default)s]")
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        parser.add_argument('-r', '--redacted', action='store_true', help="When outputting deep verbose debugging (-vvv), redact out personal security details for reporting on forums.")

        if command == P + "ls" or command == P + "ll":
            parser.add_argument('-l', '--long', action='store_true', help="long listing format [default: %(default)s]")
            parser.add_argument('-H', '--human', action='store_true', help="human readable sizes [default: %(default)s]")
            parser.add_argument('-R', '--recursive', action='store_true', help="recursive listing of directories [default: %(default)s]")
            parser.add_argument('folder', help='The folder/path to list', nargs='?', default=degoo.CWD)
            args = parser.parse_args()

            if command == P + "ll":
                args.long = True

            degoo.ls(args.folder, args.long, args.human, args.recursive)

        elif command == P + "pwd":
            print(f"Working Directory is {degoo.CWD['Path']}")

        elif command == P + "props":
            parser.add_argument('path', help='The name, path, or ID of degoo item to return properties of (can be a device, folder, file).')
            parser.add_argument('-R', '--recursive', action='store_true')
            parser.add_argument('-b', '--brief', action='store_true')
            args = parser.parse_args()

            if args.path.isdigit():
                args.path = int(args.path)

            properties = degoo.get_item(args.path, args.verbose, args.recursive)

            brief_props = ["ID", "CategoryName"]

            if args.recursive:
                for path, props in properties.items():
                    print(f"Properties of {path}:")
                    for key, value in props.items():
                        if not args.brief or key in brief_props:
                            print(f"\t{key}: {value}")
                    print("")  # Blank line separating items
            else:
                print(f"Properties of {args.path}:")
                for key, value in properties.items():
                    if not args.brief or key in brief_props:
                        print(f"\t{key}: {value}")

        elif command == P + "path":
            parser.add_argument('path', help='The path to test.')
            args = parser.parse_args()

            print(f"Path is: {degoo.get_dir(args.path)}")

        elif command == P + "cd":
            parser.add_argument('folder', help='The folder/path to makre current.')
            args = parser.parse_args()

            cwd = degoo.cd(args.folder)

            print(f"Working Directory is now {cwd['Path']}")

        elif command == P + "tree":
            parser.add_argument('-t', '--times', action='store_true', help="Show timestamps")
            parser.add_argument('folder', nargs='?', help='The folder to put_file it in')
            args = parser.parse_args()
            degoo.tree(args.folder, args.times)

        elif command == P + "mkdir":
            parser.add_argument('folder', help='The folder/path to list')
            args = parser.parse_args()

            ID = degoo.mkpath(args.folder)
            path = degoo.get_item(ID)["FilePath"]
            print(f"Created folder {path}")

        elif command == P + "rm":
            parser.add_argument('file', help='The file/folder/path to remove')
            args = parser.parse_args()

            path = degoo.rm(args.file)
            print(f"Deleted {path}")

        elif command == P + "mv":
            parser.add_argument('source', help='The path of file/folder to be moved')
            parser.add_argument('target', help='Path where the file or directory will be moved')

            args = parser.parse_args()

            abs_from = degoo.util.path_str(args.source)

            try:
                ID = degoo.mv(args.source, args.target)

                if ID:
                    abs_to = degoo.util.path_str(ID)
                    print(f"Moved {abs_from} to {abs_to}")
            except Exception as e:
                print(e)

        elif command == P + "get":
            parser.add_argument('-d', '--dryrun', action='store_true', help="Show what would be uploaded but don't upload it.")
            parser.add_argument('-f', '--force', action='store_true', help="Force downloads, else only if local file missing.")
            parser.add_argument('-s', '--scheduled', action='store_true', help="Download only when the configured schedule allows.")
            parser.add_argument('remote', help='The file/folder/path to get')
            parser.add_argument('local', nargs='?', help='The directory to put it in (current working directory if not specified)')
            args = parser.parse_args()

            degoo.get(args.remote, args.local, args.verbose, not args.force, args.dryrun, args.scheduled)

        elif command == P + "put":
            parser.add_argument('-d', '--dryrun', action='store_true', help="Show what would be uploaded but don't upload it.")
            parser.add_argument('-f', '--force', action='store_true', help="Force uploads, else only upload if changed.")
            parser.add_argument('-s', '--scheduled', action='store_true', help="Upload only when the configured schedule allows.")
            parser.add_argument('local', help='The file/folder/path to put')
            parser.add_argument('remote', nargs='?', help='The remote folder to put it in')
            args = parser.parse_args()

            result = degoo.put(args.local, args.remote, args.verbose, not args.force, args.dryrun, args.scheduled)

            if not args.dryrun:
                if len(result) == 3:
                    ID, Path, URL = result
                    print(f"Uploaded {args.local} to {Path} with Degoo ID: {ID} and Download URL\n{URL}")
                elif len(result) == 2:
                    ID, Path = result
                    print(f"Uploaded {args.local} to {Path} with Degoo ID: {ID}")
                else:
                    print(f"WARNING: Cannot upload {args.local}, it is not a File or Directory.")

        elif command == P + "login":
            parser.add_argument('username', nargs='?', help="Your Degoo account username.")
            parser.add_argument('password', nargs='?', help="Your Degoo account password (we don't recommend passing passwords on the command line, for security reasons)")
            parser.add_argument('-f', '--file', action='store_true', help=f"Read credentials from {degoo.api.cred_file}.")
            args = parser.parse_args()

            if args.file:
                success = degoo.api.login(verbose=args.verbose, redacted=args.redacted)
            else:
                success = degoo.login(args.username, args.password, args.verbose, args.redacted)

            if success:
                print("Successfully logged in.")
            else:
                print("Login failed.")

        elif command == P + "user":
            props = degoo.userinfo()
            print(f"Logged in user:")
            for key, value in props.items():
                print(f"\t{key}: {value}")

        elif command == P + "test":
            degoo.test()

        return 0
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception as e:
        if DEBUG or TESTRUN:

            def format_exception(e):
                exception_list = traceback.format_stack()
                exception_list = exception_list[:-2]
                exception_list.extend(traceback.format_tb(sys.exc_info()[2]))
                exception_list.extend(traceback.format_exception_only(sys.exc_info()[0], sys.exc_info()[1]))

                exception_str = "Traceback (most recent call last):\n"
                exception_str += "".join(exception_list)

                return exception_str

            sys.stderr.write(format_exception(e))

        indent = len(command) * " "
        sys.stderr.write(command + ": " + str(e) + "\n")
        sys.stderr.write(indent + "  for help use --help\n")
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
