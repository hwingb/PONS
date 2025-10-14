
from .event_analyzer import EventAnalyzer 
from json import dumps

class MessageAnalyzer(EventAnalyzer):

    def __init__(self, num_nodes: int):
        super().__init__()
        self.num_nodes = num_nodes
        self.messages = dict[str, dict]()


    def process(self, ts, category, event, **kwargs):
        if category == "ROUTER":
            # ts_slot = ts_slot_from_ts(ts)
            if event["event"] == "TX":
                msg_id = event['msg']
                node_from = event['from']
                dst = event['dst']
                message_stats = self.messages.get(msg_id, None)
                if message_stats == None:
                    self.messages[msg_id] = {
                        "origin" : node_from,
                        "dst": dst,
                        "created" : ts,
                        "reached_dst": None,
                        "reached_dst_after": None,
                        "duplicates_delivered_to_dst": 0,
                        "received" : {},
                        "known_to_num_nodes_after": {},
                        "permeation_after": {}
                    }
            elif event["event"] == "RX":
                msg_id = event['msg']
                node_to = event['to']
                message_stats = self.messages.get(msg_id, None)
                if message_stats != None:
                    if message_stats["received"].get(node_to, None) != None:
                        #print(f"Msg deceived duplicate of {msg_id} at node {node_to}")
                        if  message_stats["dst"] == node_to:
                            message_stats["duplicates_delivered_to_dst"] += 1
                        pass
                    else:
                        if  message_stats["dst"] == node_to:
                            message_stats["reached_dst"] = ts
                            message_stats["reached_dst_after"] = ts - message_stats["created"]
                        message_stats["received"][node_to] = ts
                        known_to_num_nodes = len(message_stats["received"].keys())
                        after = ts - message_stats["created"]
                        message_stats["known_to_num_nodes_after"][after] = known_to_num_nodes
                        message_stats["permeation_after"][after] = known_to_num_nodes / (self.num_nodes -1) #since the message is also known to the origin node (does produce > 100% when src gets msg delivered to it)

        return super().process(ts, category, event, **kwargs)

    def json(self, **kw) -> str:
        return dumps(self.messages, **kw)