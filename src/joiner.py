"""
joiner.py

Script to extract all lines with `USER_TAG` from files,
and join all together in one `user-messages.txt` file.
"""
import argparse
import logging
import sys
from os.path import join, isfile
from typing import List

from utils.utils import configure_logging

USER_TAG = "[me]"
OTHERS_TAG = "[others]"


def run(files_directory: str, files_name: List[str], output_path: str):
    user_output_file = join(output_path, "user-messages.txt")
    all_output_file = join(output_path, "all-messages.txt")

    logging.info(f"files_directory:{files_directory} - files_name:{files_name}")
    user_messages = []
    all_messages = []
    for file_name in files_name:
        file_path = join(files_directory, file_name)
        if not isfile(file_path):
            logging.warning(f"File {file_path} provided but not found")
            continue
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                for line in file:
                    all_messages.append(line)
                    if USER_TAG in line:
                        line = line.replace(USER_TAG, '')
                        user_messages.append(line)
        except IOError as e:
            logging.error(f"Error reading file {file_path}: {str(e)}")
            continue

    logging.info(f"N° User messages - {len(user_messages)} messages found. Saving at: {user_output_file}")
    logging.info(f"N° Chat messages - {len(all_messages)} messages found. Saving at: {all_output_file}")
    try:
        with open(user_output_file, 'w', encoding='utf-8', errors='replace') as user_file:
            user_file.writelines(user_messages)
        with open(all_output_file, 'w', encoding='utf-8', errors='replace') as all_file:
            all_file.writelines(all_messages)
    except IOError as e:
        logging.error(f"Error writing output files: {str(e)}")
        return

    logging.info("Joiner finished")


def main(argv):
    parser = argparse.ArgumentParser(prog=argv[0])
    parser.add_argument("--files_directory", type=str, default='../data/chat_parsed/',
                        help="path to the folder with files to parse")
    parser.add_argument("--files_name", nargs='+',
                        default=['telegram-chats.txt', 'wa-chats.txt'],
                        help="list of files name to include in the joining process")
    parser.add_argument("--output_path", type=str, default='../data/chat_parsed/',
                        help="Folder where store 'user-messages.txt' and 'all-messages.txt' ")

    configure_logging(argv)

    try:
        args = parser.parse_args(argv[1:])
        run(args.files_directory, args.files_name, args.output_path)
    except argparse.ArgumentError as e:
        print("Error parsing command-line arguments:", str(e))
        sys.exit(1)


if __name__ == '__main__':
    main(sys.argv)
