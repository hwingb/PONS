
from pons.logging import LinkAnalyzer, MessageAnalyzer
from pons.sim_lib import get_filepath
import pickle

import time
import os
import signal
from enum import Enum

processing_aborted = False

def is_processing_aborted() -> bool:
    global processing_aborted
    return processing_aborted

def signal_handler(sig, frame):
    global processing_aborted
    print("Stopping simulation...")
    processing_aborted = True

def register_signal_handler():
    signal.signal(signal.SIGINT, signal_handler)

class ProcessingResult(Enum):
    SUCCESS = 0
    SKIPPED = 1
    ERROR = 2


def process_msg_analyzer(msg_pkl_filepath: str, file_base: str, base_dir: str) -> ProcessingResult:
    msg_stats_json_filename = f'{file_base}.msg_stats.json'
    msg_stats_json_filepath = get_filepath(msg_stats_json_filename, base=base_dir)
    if os.path.exists(msg_stats_json_filepath):
        return ProcessingResult.SKIPPED
    with open(msg_pkl_filepath, 'rb') as f:
        msg_analyzer : MessageAnalyzer = pickle.load(f)
    msg_stats_json = msg_analyzer.json()
    with open(msg_stats_json_filepath, "w") as outfile:
        outfile.write(msg_stats_json)
    return ProcessingResult.SUCCESS

def process_lnk_analyzer(lnk_pkl_filepath: str, file_base: str, base_dir: str) -> ProcessingResult:
    lnk_stats_json_filename = f'{file_base}.lnk_stats.json'
    lnk_tx_stats_csv_filename = f'{file_base}.lnk_tx_stats.csv'
    lnk_rx_stats_csv_filename = f'{file_base}.lnk_rx_stats.csv'

    lnk_stats_json_filepath = get_filepath(lnk_stats_json_filename, base=base_dir)
    lnk_tx_stats_csv_filepath = get_filepath(lnk_tx_stats_csv_filename, base=base_dir)
    lnk_rx_stats_csv_filepath = get_filepath(lnk_rx_stats_csv_filename, base=base_dir)

    lnk_stats_json_file_exists = os.path.exists(lnk_stats_json_filepath)
    lnk_tx_stats_csv_file_exists = os.path.exists(lnk_tx_stats_csv_filepath)
    lnk_rx_stats_csv_file_exists = os.path.exists(lnk_rx_stats_csv_filepath)

    if lnk_stats_json_file_exists and lnk_tx_stats_csv_file_exists and lnk_rx_stats_csv_file_exists:
        return ProcessingResult.SKIPPED

    with open(lnk_pkl_filepath, 'rb') as f:
        lnk_analyzer : LinkAnalyzer = pickle.load(f)
    # lnk_analyzer.json()
    lnk_tx_statistics_csv, lnk_rx_statistics_csv = lnk_analyzer.csv()
    if not lnk_tx_stats_csv_file_exists or not lnk_rx_stats_csv_file_exists:
        with open(lnk_tx_stats_csv_filepath, "w") as outfile:
            outfile.write(lnk_tx_statistics_csv)
        with open(lnk_rx_stats_csv_filepath, "w") as outfile:
            outfile.write(lnk_rx_statistics_csv)
    if not lnk_stats_json_file_exists:
        link_stats_json = lnk_analyzer.json()
        with open(lnk_stats_json_filepath, "w") as outfile:
            outfile.write(link_stats_json)

    return ProcessingResult.SUCCESS

msg_analyzer_suffix = "_msg_analyzer"
msg_analyzer_suffix_w_extension = msg_analyzer_suffix+".pkl"
lnk_analyzer_suffix = "_lnk_analyzer"
lnk_analyzer_suffix_w_extension = lnk_analyzer_suffix+".pkl"

def process_dir(root: str, dir: str):
    for file in os.listdir(os.path.join(root, dir)):
        started_time_stamp=time.strftime("%Y-%m-%d_%H%M%S")
        if file.endswith(msg_analyzer_suffix_w_extension):
            # print(f"-- Processing: {file}, started: {started_time_stamp}", end='', flush=True)
            # print(f"Processing: {dir}/{file} - started: {started_time_stamp}", flush=True)
            file_base = os.path.splitext(os.path.basename(file))[0].removesuffix(msg_analyzer_suffix)
            msg_pkl_filepath = os.path.join(root, dir, file)
                    # print(msg_pkl_filepath)
                    # print('')
                    # continue
            try:
                res = process_msg_analyzer(msg_pkl_filepath, file_base, os.path.join(root, dir))
                end_time_stamp=time.strftime("%Y-%m-%d_%H%M%S")
                if res == ProcessingResult.SUCCESS:
                # print(f", done: {end_time_stamp}")
                    print(f"Processed: {dir}/{file} - {end_time_stamp}")
            except Exception as e:
                # print(f' -ERROR- {repr(e)}')
                print(f'Processing: {dir}/{file} -ERROR- {repr(e)}')
        elif file.endswith(lnk_analyzer_suffix_w_extension):
            # print(f"-- Processing: {file}, started: {started_time_stamp}", end='', flush=True)
            # print(f"Processing: {dir}/{file} - started: {started_time_stamp}", flush=True)
            file_base = os.path.splitext(os.path.basename(file))[0].removesuffix(lnk_analyzer_suffix)
            lnk_pkl_filepath = os.path.join(root, dir, file)
                    # print(lnk_pkl_filepath)
                    # print('')
                    # continue
            try:
                res = process_lnk_analyzer(lnk_pkl_filepath, file_base, os.path.join(root, dir))
                end_time_stamp=time.strftime("%Y-%m-%d_%H%M%S")
                # print(f", done: {end_time_stamp}")
                if res == ProcessingResult.SUCCESS:
                    print(f"Processing: {dir}/{file} - done {end_time_stamp}")
            except Exception as e:
                # print(f' -ERROR- {repr(e)}')
                print(f'Processing: {dir}/{file} -ERROR- {repr(e)}')
    return