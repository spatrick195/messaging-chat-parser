import logging
import os
import sys
import json
import argparse
from datetime import datetime
from utils.utils import configure_logging, extract_dict_structure, split_in_sessions
from tqdm import tqdm

sys.path.append("./")

USER_TAG = "[me]"
OTHERS_TAG = "[others]"
TELEGRAM_STOP_WORDS = [word.replace('\n', '') for word in open('./data/resources/Telegram_stopwords.txt').readlines()]


def save_messages_parsed(output_path, user_messages):
    output_file = os.path.join(output_path, "telegram-chats.txt")
    try:
        with open(output_file, 'w') as f:
            f.writelines(user_messages)
    except IOError as e:
        logging.error(f"Error saving parsed messages: {str(e)}")
        return


def stop_word_checker(actor, invalid_lines, text):
    if text is None:
        return False
    if type(text) != str:  # Telegram save links under 'text' key, but they are dictionary / list
        invalid_lines.append(f"[STOP_WORD] {actor} - {text}")
        return True
    for stop_word in TELEGRAM_STOP_WORDS:
        if stop_word in text:
            invalid_lines.append(f"[STOP_WORD] {actor} - {text}")
            return True
    return False


def messages_parser(personal_chat, telegram_data, session_info: dict):
    datetime_format = session_info['time_format']
    usr_id = 'user' + str(telegram_data['personal_information']['user_id'])
    usr_messages = []
    invalid_lines = []

    for chat in tqdm(telegram_data['chats']['list']):
        if chat['type'] == 'saved_messages' and not personal_chat:
            continue  # Skip personal messages
        if chat['type'] != 'personal_chat':
            continue  # Skip everything but 1 to 1 messages
        logging.info(f"Processing chat with `{chat.get('name', 'personal messages')}`")
        t_last = None
        for message in chat['messages']:
            if message['type'] == "message" and message['text']:
                t_current = datetime.strptime(message['date'], datetime_format)
                split_in_sessions(t_current,
                                  t_last,
                                  usr_messages,
                                  session_info['delta_h_threshold'],
                                  session_info['session_token'])
                t_last = t_current
                if not stop_word_checker(message['from_id'], invalid_lines, message['text']):
                    msg_prefix = USER_TAG if message['from_id'] == usr_id else OTHERS_TAG
                    usr_messages.append(f"{msg_prefix} {message['text']}")
    logging.info(f"N° {len(invalid_lines)} invalid lines found, top 5: {invalid_lines[:5]}")
    return usr_messages


def load_data(json_path):
    try:
        with open(json_path, 'r', errors='ignore') as f:
            telegram_data = json.load(f)
        telegram_data_structure = extract_dict_structure(telegram_data)
        logging.info(f'Input json structure:\n{json.dumps(telegram_data_structure, indent=4, sort_keys=True)}')
        return telegram_data
    except FileNotFoundError:
        logging.error(f"JSON file not found: {json_path}")
        raise


def run(json_path: str,
        output_path: str,
        session_token: str,
        delta_h_threshold: int,
        time_format: str,
        personal_chat: bool = None):
    session_info = {"session_token": session_token,
                    "delta_h_threshold": delta_h_threshold,
                    "time_format": time_format}

    logging.info(f"Loading telegram user data at {json_path}...")
    try:
        telegram_data = load_data(json_path)
    except FileNotFoundError:
        return

    logging.info(f"Start parsing telegram messages...")
    try:
        user_messages = messages_parser(personal_chat, telegram_data, session_info)
    except Exception as e:
        logging.error("Error occurred while parsing messages:")
        logging.error(str(e))
        return

    logging.info(f"Saving {len(user_messages)}^ telegram messages...")
    try:
        save_messages_parsed(output_path, user_messages)
    except Exception as e:
        logging.error("Error occurred while saving parsed messages:")
        logging.error(str(e))
        return


def main(argv):
    parser = argparse.ArgumentParser(prog=argv[0])
    parser.add_argument('--json_path', type=str, required=False, default="./data/chat_raw/telegram/telegram_dump.json",
                        help="Path to the json created from Telegram exporter")
    parser.add_argument('--output_path', type=str, default="./data/chat_parsed/")
    parser.add_argument('--personal_chat', type=bool, default=False,
                        help="Include the telegram personal chats. Default is disabled.")
    parser.add_argument('--session_token', type=str,
                        help="Add a 'session_token' after 'delta_h_threshold' hours"
                             "are elapsed between two messages. This allows splitting in sessions"
                             "one chat based on messages timing.")
    parser.add_argument("--delta_h_threshold", type=int, default=4,
                        help="Hours between two messages to before add 'session_token'")
    parser.add_argument("--time_format", type=str, default="%Y-%m-%dT%H:%M:%S",
                        help="The Telegram format timestamp. Default is Italian format.")

    configure_logging(argv)

    try:
        args = parser.parse_args(argv[1:])
        run(args.json_path, args.output_path, args.session_token, args.delta_h_threshold,
            args.time_format, args.personal_chat)
    except argparse.ArgumentError as e:
        print("Error parsing command-line arguments:", str(e))
        sys.exit(1)


if __name__ == '__main__':
    main(sys.argv)
