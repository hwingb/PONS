import os
from dataclasses import dataclass, field
import pons
import pons.routing as pr

from pons.event_log import register_analyzer, unregister_analyzer
from pons.logging import MessageAnalyzer, LinkAnalyzer
from pons.simulation import is_aborted

import random
import copy
import pickle

@dataclass
class SimRunResult():
    """Holds results of a single simulation run"""
    net_stats: pons.NetStats
    routing_stats: pons.RoutingStats

def get_filepath(filename, dir: str | None = None, base: str | None = None):
    dir_path = get_dirpath(dir, base)
    return os.sep.join([dir_path, filename])

def get_dirpath(dir: str | None = None, base: str | None = None) -> str:
    if base is None:
        base = os.getcwd()
    if dir is None:
        dir_path = base
    else:
        dir_path = os.sep.join([base, dir])
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    return dir_path


# @dataclass
# class NetSimConfig():
#     {"movement_logger": False, "peers_logger": False, "event_logging": True}

@dataclass
class MessageGenConfig():
    msg_size: int | tuple[int, int]
    gen_interval: int | tuple[int, int]
    msg_ttl: int
    msg_id: str = "M"

@dataclass
class SimConfig():
    """Holds config for a simulation"""
    sim_time: float
    world_size: tuple[int, int]
    num_nodes_total: int
    wifi_range: int
    wifi_bandwidth: int
    num_nodes_with_sat: int
    sat_range: int
    sat_bandwidth: int
    net_sim_config: dict #NetSimConfig
    msg_gen_config: MessageGenConfig
    simulation_dir: str
    additional_parameters: dict = field(default_factory=lambda: {})

    @property
    def num_nodes_no_sat(self) -> int:
        return int(self.num_nodes_total-self.num_nodes_with_sat)
    
@dataclass
class SimResult():
    """Holds results of a single simulation run"""
    net_stats: list[pons.NetStats]
    routing_stats: list[pons.RoutingStats]
    sim_config: SimConfig
    routers: list[pr.Router]
    runs: int
    
    @property
    def total_simulated_time(self):
        return self.runs * len(self.routers) * self.sim_config.sim_time

def spawn_run(one: pons.OneMovement, sim_config: SimConfig, router: pr.Router, run: int) -> SimRunResult :
    random.seed(run)
    print("run", run+1)

    lnk_analyzer_filepath = get_filepath(f'{str(router)}_RUN_{run}_lnk_analyzer.pkl', base=sim_config.simulation_dir)
    msg_analyzer_filepath = get_filepath(f'{str(router)}_RUN_{run}_msg_analyzer.pkl', base=sim_config.simulation_dir)
    sim_run_results_filepath = get_filepath(f'{str(router)}_RUN_{run}_results.pkl', base=sim_config.simulation_dir)
    
    lnk_analyzer_file_exists = os.path.exists(lnk_analyzer_filepath)
    msg_analyzer_file_exists = os.path.exists(msg_analyzer_filepath)
    sim_run_results_file_exists = os.path.exists(sim_run_results_filepath)

    if lnk_analyzer_file_exists and msg_analyzer_file_exists and sim_run_results_file_exists:
        print(
            f"Results already exist, skipping simulation!"
        )
        with open(sim_run_results_filepath, 'rb') as f:
            sim_run_results : SimRunResult = pickle.load(f)
            return sim_run_results

        # moves = pons.generate_randomwaypoint_movement(SIM_TIME, NUM_NODES_TOTAL, WORLD_SIZE[0], WORLD_SIZE[1], min_speed=1.0, max_speed=3.0, max_pause=60.0)
        
    net_wifi = pons.NetworkSettings("WIFI", bandwidth=sim_config.wifi_bandwidth, range=sim_config.wifi_range)
    net_sat = pons.NetworkSettings("SAT_CONSTANT", bandwidth=sim_config.sat_bandwidth, range=sim_config.sat_range)

    current_router = copy.deepcopy(router)

    msg_analyzer = MessageAnalyzer(one.num_nodes)
    lnk_analyzer = LinkAnalyzer()

    register_analyzer(msg_analyzer)
    register_analyzer(lnk_analyzer)

        # nodes = pons.generate_nodes(NUM_NODES_TOTAL, net=[net_wifi], router=copy.deepcopy(router))
        
    # nodes_wifi = pons.generate_nodes(sim_config.num_nodes_no_sat, net=[net_wifi], router=current_router, prefix="w")
    # nodes_sat = []
    # if sim_config.num_nodes_with_sat > 0:
    #     nodes_sat = pons.generate_nodes(sim_config.num_nodes_with_sat, net=[net_wifi, net_sat], router=current_router, offset=sim_config.num_nodes_no_sat, prefix="s")
    # nodes = nodes_wifi + nodes_sat

    nodes : list[pons.Node] = pons.generate_nodes(sim_config.num_nodes_total, net=[net_wifi], router=current_router, prefix='w')

    # Replace/reconfigure `sim_config.num_nodes_with_sat` number of nodes with additional sattelite network
    for i in range(sim_config.num_nodes_with_sat):
        sat_index = random.randint(0, (len(nodes) - 1 ))
        node = nodes[sat_index]
        nodes[sat_index] = pons.Node(node.id, net=copy.deepcopy([net_wifi, net_sat]), router=node.router, prefix='s')

    msggenconfig = {
            "interval": sim_config.msg_gen_config.gen_interval, 
            "src": (0, sim_config.num_nodes_total), 
            "dst": (0, sim_config.num_nodes_total), 
            "size": sim_config.msg_gen_config.msg_size, 
            "id": sim_config.msg_gen_config.msg_id,
            "ttl": sim_config.msg_gen_config.msg_ttl
            }
        
    config = sim_config.net_sim_config
    config["log_file"] = get_filepath(f'{str(router)}_RUN_{run}_events.log', base=sim_config.simulation_dir)
    
    netsim = pons.NetSim(sim_config.sim_time, nodes, world_size=sim_config.world_size, movements=one.moves, config=config, msggens=[msggenconfig])

    netsim.setup()
    
    print('Starting simulation')
    netsim.run()
    if is_aborted():
        return

    ns = copy.deepcopy(netsim.net_stats)
    ns['router'] = "" + str(router)
    # net_stats.append(ns)
    rs = copy.deepcopy(netsim.routing_stats)
    rs['router'] = "" + str(router)
    # routing_stats.append(rs)
    
    
    
    with open(lnk_analyzer_filepath, mode='x+b') as lnk_analyzer_file:
        pickle.dump(lnk_analyzer, lnk_analyzer_file)
    
    
    with open(msg_analyzer_filepath, mode='x+b') as msg_analyzer_file:
        pickle.dump(msg_analyzer, msg_analyzer_file)

    # lnk_tx_statistics_csv, lnk_rx_statistics_csv = lnk_analyzer.csv()
    # with open(get_filepath(f'{str(router)}_RUN_{run}_lnk_tx_stats.csv', base=sim_config.simulation_dir), "w") as outfile:
    #     outfile.write(lnk_tx_statistics_csv)
    # with open(get_filepath(f'{str(router)}_RUN_{run}_lnk_rx_stats.csv', base=sim_config.simulation_dir), "w") as outfile:
    #     outfile.write(lnk_rx_statistics_csv)

    # message_stats_json = msg_analyzer.json()
    # with open(get_filepath(f'{str(router)}_RUN_{run}_msg_stats.json', base=sim_config.simulation_dir), "w") as outfile:
    #     outfile.write(message_stats_json)

    # link_stats_json = lnk_analyzer.json()
    # with open(get_filepath(f'{str(router)}_RUN_{run}_lnk_stats.json', base=sim_config.simulation_dir), "w") as outfile:
    #     outfile.write(link_stats_json)

    unregister_analyzer(msg_analyzer)
    unregister_analyzer(lnk_analyzer)

    sim_run_results = SimRunResult(net_stats=ns, routing_stats=rs)
    with open(sim_run_results_filepath, mode='x+b') as sim_run_results_file:
        pickle.dump(sim_run_results, sim_run_results_file)

    return sim_run_results