
# # Python Opportunistic Network Simulator

import random
import copy


import pons
import pons.routing as pr
from pons.simulation import is_aborted

import os
import pickle

from pons.sim_lib import get_filepath, get_dirpath, spawn_run, SimResult, SimRunResult, SimConfig, MessageGenConfig

import time

import time
from typing import Optional

####
#Imports only needed for processing analyzers
from multiprocessing import Pool
pool = Pool(processes=3)
from tools.process_analyzers.process_analyzer_lib import process_dir, register_signal_handler, is_processing_aborted
####


base = os.path.dirname(os.path.realpath(__file__))

tracefiles_path = get_dirpath('data', base=base)
simulations_path = get_dirpath('output', base=base)
traces = [
'movements.one',
]

RUNS = 10
RANDOM_SEED = 42

WIFI_RANGE = 50
WIFI_BANDWIDTH = 54_000_000

SAT_RANGE = 100_000_000 # essentially unlimited
SAT_BANDWIDTH = 9_600 # 9.6 kbits/s (e.g. Iridium)
# SAT_BANDWIDTH = 100_000_000 # 100 Mbits/s (e.g. Starlink)

CAPACITY = 10000

MSG_SIZE = 512
# MSG_SIZE = (150, 512)
MSG_GEN_INTERVAL = (20, 40)

ENABLE_EVENT_LOGGING = False #Caution can take up a lot of storage space

#ROUTERS=[pr.EpidemicRouter(), pr.SprayAndWaitRouter(copies=7), pr.SprayAndWaitRouter(copies=7, binary=True), pr.DirectDeliveryRouter(), pr.FirstContactRouter()]
ROUTERS=[pr.HypergossipRouter(capacity=CAPACITY), pr.EpidemicRouter()]
# ROUTERS=[pr.EpidemicRouter(), pr.SprayAndWaitRouter(copies=7)]
# ROUTERS=[pr.EpidemicRouter()]

def main():
    for trace in traces:
        if is_aborted():
            return
        run(trace=trace, no_sat=0, date_in_dirname=False)
        run(trace=trace, no_sat=2, date_in_dirname=False)
        run(trace=trace, no_sat=3, date_in_dirname=False)
        run(trace=trace, no_sat=4, date_in_dirname=False)
        run(trace=trace, no_sat=5, date_in_dirname=False)
    process_results()

def process_results():
    register_signal_handler()
    for root, dirs, _ in os.walk(simulations_path):
        if is_processing_aborted():
            break
        for dir in dirs:
            if is_processing_aborted():
                break
            if os.path.exists(os.path.join(root, dir, "result.pkl")):
                pool.apply_async(process_dir, [root, dir]) #Asynchronous
            else:
                print(f'{dir}: Simulation unfinished -> skipping')

    pool.close()
    pool.join()

def run(trace, no_sat = 3, sim_dirname: Optional[str]=None, date_in_dirname: bool = True):
    if is_aborted():
        return

    started_time_stamp=time.strftime("%Y-%m-%d_%H%M%S")
    print(f'Started {started_time_stamp}')
    # ## Load ONE Movement Data
    print('Loading ONE Movement Data ...', end='\r')
    one = pons.OneMovement.from_file(get_filepath(trace, base=tracefiles_path))
    print('\rLoading ONE Movement Data - Done.')

    print('Imported ONE Movement Data:')
    print(f'    duration: {one.duration}')
    print(f'    nodes: {one.num_nodes}')
    print(f'    width: {one.width}')
    print(f'    height: {one.height}')

    # ## Set Experiment Parameters
    random.seed(RANDOM_SEED)

    num_nodes_with_sat = no_sat

    routers = copy.deepcopy(ROUTERS)

    print(f'WiFi Bandwidth: {pons.format_network_bandwidth(WIFI_BANDWIDTH)}')
    print(f'Satelite Bandwidth: {pons.format_network_bandwidth(SAT_BANDWIDTH)}')

    # ## Run Experiments
    trace_name = trace.strip('.one')
    if sim_dirname is None:
        sim_dirname = f'{num_nodes_with_sat}SAT_{RUNS}RUNS_{CAPACITY}CAPACITY_{RANDOM_SEED}SEED_{trace_name}'
        if date_in_dirname:
            sim_dirname += f"-{started_time_stamp}"
        
    sim_dir = get_dirpath(sim_dirname, base=simulations_path)
    
    results_file_name = f'result.pkl'
    results_filepath = get_filepath(results_file_name, base=sim_dir)
    results_file_exists = os.path.exists(results_filepath)

    if results_file_exists:
        print(
            f"Results already exist, skipping simulation!"
        )
        return
    

    sim_config_file_name = f'sim_config.pkl'
    sim_config_file_path = get_filepath(sim_config_file_name, base=sim_dir)
    if os.path.exists(sim_config_file_path):
        with open(sim_config_file_path, 'rb') as f:
            sim_config : SimConfig = pickle.load(f)
        pass
    else:
        if ENABLE_EVENT_LOGGING:
            net_sim_config = {'verbose': False, "movement_logger": False, "peers_logger": False, "event_logging": ENABLE_EVENT_LOGGING}
        else:
            net_sim_config={"verbose": False, "movement_logger": False, "peers_logger": False, "event_logging": ENABLE_EVENT_LOGGING, "log_file": get_filepath('events.log', base=sim_dir)}

        sim_config = SimConfig(
                            sim_time=one.duration, 
                            world_size=(int(one.width), int(one.height)), 
                            num_nodes_total=one.num_nodes, 
                            wifi_range=WIFI_RANGE, 
                            wifi_bandwidth=WIFI_BANDWIDTH, 
                            num_nodes_with_sat=num_nodes_with_sat,
                            sat_range=SAT_RANGE,
                            sat_bandwidth=SAT_BANDWIDTH,
                            msg_gen_config=MessageGenConfig(
                                msg_size=MSG_SIZE,
                                gen_interval=MSG_GEN_INTERVAL,
                                msg_id='M',
                                msg_ttl=3600 # 1 Std
                            ),
                            net_sim_config=net_sim_config,
                            simulation_dir=sim_dir,
                            additional_parameters={
                                "capacity": CAPACITY
                            }
                            )

        with open(sim_config_file_path, mode='x+b') as sim_config_file:
            pickle.dump(sim_config, sim_config_file)

    simulation_start_time = time.perf_counter()
    net_stats = []
    routing_stats = []
    for router in routers:
        if is_aborted():
            return
        print("router: %s" % router)
        for run in range(RUNS):
            if is_aborted():
                return
            run_result = spawn_run(one=one, sim_config=sim_config, router=router, run=run)  # evaluate "spawn_run" synchronously
            net_stats.append(run_result.net_stats)
    if is_aborted():
        return
    sim_result = SimResult(net_stats=net_stats,routing_stats=routing_stats,sim_config=sim_config, runs=RUNS, routers=routers)

    simulation_end_time = time.perf_counter()

    total_simulated_time = sim_result.total_simulated_time
    real_time = simulation_end_time - simulation_start_time
    speedup = total_simulated_time / real_time

    timetaken_str = f'Simulation took {real_time:.2f} seconds while simulating a total of {total_simulated_time:.2f} seconds ({speedup:.2f} x real time)'
    print(timetaken_str)

    timetaken_file_path = get_filepath('sim_stats.txt', base=sim_dir)

    timestr = time.strftime("%Y-%m-%d_%H%M%S")
    finisehd_str = f'Finished: {timestr}'

    with open(results_filepath, mode='x+b') as result_file:
        pickle.dump(sim_result, result_file)
    
    with open(timetaken_file_path, mode='x') as timetaken_file:
        timetaken_file.write(timetaken_str+'\n'+finisehd_str+'\n')
    print(finisehd_str)

if __name__ == "__main__":
    main()