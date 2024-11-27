from copy import copy
import pons
from pons import Router, PayloadMessage
from pons.event_log import event_log
from typing import override

HELLO_MSG_SIZE = 42

# https://simpy.readthedocs.io/en/latest/topical_guides/events.html#let-time-pass-by-the-timeout
# sub_proc = yield start_delayed(env, sub(env), delay=3)
# ret = yield sub_proc

"""
TODO 
- adapt call for _on_received
- new overhead message types
- adapt separation for hello messages 
- require new hello message? with cluster ID 
- call procedure of _on_msg_received and on_msg_received
- lists for neighbor ids and stuff

optional TODOs:
- adaptations for dynamic beacon/hello intervals
- 
"""


class Hypergossip(Router):
    def __init__(self, scan_interval=2.0, capacity=0, apps=None):
        super(Hypergossip, self).__init__(scan_interval, capacity, apps)
        self.store = []

    @override
    def __str__(self):
        return "HypergossipRouter"

    @override
    def on_scan_received(self, msg: pons.PayloadMessage, remote_id: int):
        pass

    @override
    def on_peer_discovered(self, peer_id):
        pass

    @override
    def on_msg_received(self, msg, remote_id):
        pass

    @override
    def on_receive(self, msg: pons.PayloadMessage, remote_node_id: int) -> None:
        # TODO switch for right messages
        # else
        super().on_receive(msg, remote_node_id)

    @override
    def on_duplicate_msg_received(self, msg: pons.PayloadMessage, remote_node_id: int) -> None:
        pass

