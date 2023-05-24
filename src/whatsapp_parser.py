import argparse
import logging
import os
import sys
from datetime import datetime
from os.path import join, basename
from pathlib import Path
from typing import List, Tuple, Optional
from dateutil.parser import parse as date_parser

import parse

from utils.utils import configure_logging, get_dir_files, split_in_sessions

sys.path.append("./")

USER_TAG = "[me]"
OTHERS_TAG = "[others]"

# convert list to set for efficiency
WA_STOP_WORDS = set(word.replace('\n', '') for word in open('./data/resources/WhatsApp_stopwords.txt').readlines())

# Define configuration constants
DEFAULT_USER_NAME = "Jakob"
DEFAULT_CHATS_PATH = "./data/chat_raw/whatsapp/"
DEFAULT_OUTPUT_PATH = "./data/chat_parsed/"
DEFAULT_DELTA_H_THRESHOLD = 4
DEFAULT_TIME_FORMAT = "%d/%m/%Y, %I:%M:%S %p"


def parse_line(line: str, datetime_format: str) -> Tuple[Optional[datetime], str, str]:
    timestamp = None
    actor = 'invalid'
    text = ''

    # Changed pattern to fit the new format
    line_elements = parse.parse("[{date}, {time}] {actor}: {text}", line)
    if line_elements:
        message_datetime = f"{line_elements['date']}, {line_elements['time']}"
        try:
            timestamp = datetime.strptime(message_datetime, datetime_format)
        except ValueError:
            logging.warning(f"Invalid datetime format for line: {line.strip()}")
        else:
            actor = line_elements['actor']
            text = line_elements['text']
    return timestamp, actor, text


def stop_word_checker(actor, invalid_lines, text):
    for stop_word in WA_STOP_WORDS:
        if stop_word in text:
            invalid_lines.append(f"[STOP_WORD] {actor} - {text}")
            return True
    return False


def parse_date(date_string):
    """
    Parses a date from a string, trying several different formats.
    :param date_string: The string to parse.
    :return: A datetime object if the string could be parsed, otherwise None.
    """
    try:
        return date_parser(date_string)
    except ValueError:
        return None


def save_text(text_list: List[str], output_path: str):
    logging.info(f'Saving {output_path}')
    try:
        with open(output_path, "w", encoding='utf-8') as f:   # specify the encoding here
            f.writelines("\n".join(text_list))
    except IOError as e:
        logging.error(f"Error saving text: {str(e)}")
        return


def parse_chat(file_path, user_name, time_format, delta_h_threshold, session_token):
    chat_text = [session_token] if session_token else []
    invalid_lines = []

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        t_last = None
        for line in lines:
            t_current, actor, text = parse_line(line, time_format)

            if actor == 'invalid':
                invalid_lines.append(line)
                continue
            if stop_word_checker(actor, invalid_lines, text):
                continue

            split_in_sessions(t_current, t_last, chat_text, delta_h_threshold, session_token)
            t_last = t_current

            actor = USER_TAG if actor == user_name else OTHERS_TAG

            chat_text.append(f"{actor} {text}")

    chat_text = [line for line in chat_text if line not in invalid_lines]

    invalid_lines_file = f"./tmp/invalid_lines_{basename(file_path)}"
    invalid_lines_dir = os.path.dirname(invalid_lines_file)

    if not os.path.exists(invalid_lines_dir):
        os.makedirs(invalid_lines_dir)

    with open(invalid_lines_file, 'w', encoding='utf-8') as f:
        f.writelines(invalid_lines)

    return chat_text


def run(user_name: str,
        chats_path: str,
        output_path: str,
        time_format: str,
        delta_h_threshold: int,
        session_token: str = None):
    logging.info(f"WA_STOP_WORDS:{WA_STOP_WORDS}")
    Path("./tmp").mkdir(parents=True, exist_ok=True)
    txt_files_name, txt_files_paths = get_dir_files(dir_path=chats_path, extension_filter=".txt")
    logging.info(f"Found {len(txt_files_paths)} txt files in `{chats_path}` folder: {txt_files_paths}")

    wa_text = []
    for file_name, file_path in zip(txt_files_name, txt_files_paths):
        file_text_parsed = parse_chat(file_path, user_name, time_format, delta_h_threshold, session_token)
        wa_text.extend(file_text_parsed)

    chat_path = join(output_path, 'wa-chats.txt')
    save_text(wa_text, chat_path)


def main(argv):
    parser = argparse.ArgumentParser(prog=argv[0])
    parser.add_argument('--user_name', type=str, required=True,
                        help="The whatsapp user name of User. It could be read on the WhatsApp raws data.")
    parser.add_argument('--chats_path', type=str, default=DEFAULT_CHATS_PATH)
    parser.add_argument('--output_path', type=str, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument('--session_token', type=str,
                        help="Add a 'session_token' after 'delta_h_threshold' hours"
                             "are elapsed between two messages. This allows splitting in sessions"
                             "one chat based on messages timing.")
    parser.add_argument("--delta_h_threshold", type=int, default=DEFAULT_DELTA_H_THRESHOLD,
                        help="Hours between two messages to before add 'session_token'")
    parser.add_argument("--time_format", type=str, default=DEFAULT_TIME_FORMAT,
                        help="The WhatsApp datetime format.")
    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")

    try:
        args = parser.parse_args(argv[1:])
        configure_logging(args.verbose)
        run(args.user_name, args.chats_path, args.output_path, args.time_format,
            args.delta_h_threshold, args.session_token)
    except argparse.ArgumentError as e:
        print("Error parsing command-line arguments:", str(e))
        sys.exit(1)


if __name__ == '__main__':
    main(sys.argv)
