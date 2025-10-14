from .event_analyzer import EventAnalyzer
from json import dumps

class LinkAnalyzer(EventAnalyzer):

    def __init__(self):#, network_settings: list[NetworkSettings]):
        super().__init__()
        # self.network_settings = deepcopy(network_settings)
        self.networks: dict[str, dict]= {}
        self.timeslots = set()

    def process(self, ts, category, event, **kwargs):
        ts_slot = int(ts)
        self.timeslots.add(ts_slot)
        if category == "NET":
            # ts_slot = ts_slot_from_ts(ts)
            net_name = event["net_name"]
            node_id = event["id"]
            net_uid = f'{node_id}_{net_name}'

            network = self.networks.get(net_uid, None)
            if network is None:
                network = {
                    "ts_slots": {},
                    "total_tx_transfer": 0,
                    "total_rx_transfer": 0,
                    "total_traffic": 0
                }
                self.networks[net_uid] = network

            
            slot_stats = network["ts_slots"].get(ts_slot, None)
            if slot_stats is None:
                slot_stats = {
                    # "total_time": args.step_size,
                    "total_tx_transfer": 0,
                    "total_tx_msgs": 0,
                    "total_rx_transfer": 0,
                    "total_rx_msgs": 0
                }
            network["ts_slots"][ts_slot] = slot_stats
            
            if event["event"] == "TX":
                slot_stats["total_tx_transfer"] += event["msg_size"]
                slot_stats["total_tx_msgs"] += 1
                slot_stats["total_tx_bitrate"] = slot_stats["total_tx_transfer"] / 1.0
            elif event["event"] == "RX":
                slot_stats["total_rx_transfer"] += event["msg_size"]
                slot_stats["total_rx_msgs"] += 1
                slot_stats["total_rx_bitrate"] = slot_stats["total_rx_transfer"] / 1.0
                # link = sorted([int(event["src"]), int(event["dst"])])
                # active_links.add(tuple(link))
        return super().process(ts, category, event, **kwargs)

    def json(self, **kw):
        for nuid, network in self.networks.items():
            total_tx_transfer=0
            total_rx_transfer=0
            
            for ts_stats in network['ts_slots'].values():
                # ts_stats = stats[timeslot]
                total_tx_transfer += ts_stats["total_tx_transfer"]
                total_rx_transfer += ts_stats["total_rx_transfer"]

                ts_stats["total_tx_bitrate"] = ts_stats["total_tx_transfer"] / 1.0
                ts_stats["total_rx_bitrate"] = ts_stats["total_rx_transfer"] / 1.0
            
            network["total_tx_transfer"] = total_tx_transfer
            network["total_rx_transfer"] = total_rx_transfer
            network["total_traffic"] = total_tx_transfer + total_rx_transfer
        return dumps(self.networks, **kw)
    

    def csv(self):
        # timeslots = set()
        # for network in self.networks.values():
        #     timeslots.update(network['ts_slots'].keys())
        
        max_time_slot = max(self.timeslots)
        # network_header_statistics_csv = '"node";"network_Name";'
        network_header_statistics_csv = '"network_Name";'
        # network_header_statistics_csv += ';'.join(str(x) for x in self.timeslots)
        network_header_statistics_csv += ';'.join(str(x) for x in range(0, max_time_slot, 1))
        network_header_statistics_csv += '\n'

        network_rx_statistics_csv = network_header_statistics_csv
        network_tx_statistics_csv = network_header_statistics_csv

        for network_uid, network in self.networks.items():
            # node = g.nodes[n]
            # network_id_csv = f'{n};"{network_uid}";'
            network_id_csv = f'"{network_uid}"'

            network_rx_statistics_csv += network_id_csv
            network_tx_statistics_csv += network_id_csv

            stats = network["ts_slots"]
            for slot in range(0, max_time_slot, 1):
                ts_stats = stats.get(slot, None)
                if ts_stats is not None:
                    network_rx_statistics_csv += f';{ts_stats.get("total_rx_bitrate", 0.0)}'
                    network_tx_statistics_csv += f';{ts_stats.get("total_tx_bitrate", 0.0)}'
                else:
                    network_rx_statistics_csv += f';0.0'
                    network_tx_statistics_csv += f';0.0'

            network_rx_statistics_csv += '\n'
            network_tx_statistics_csv += '\n'

            # network_rx_statistics_csv += ';'.join(str(x.get("total_rx_bitrate", 0.0)) for _,x in stats.items())
            # network_rx_statistics_csv += '\n'
            
            # network_tx_statistics_csv += ';'.join(str(x.get("total_tx_bitrate", 0.0)) for _,x in stats.items())
            # network_tx_statistics_csv += '\n'
        
        return (network_tx_statistics_csv, network_rx_statistics_csv)