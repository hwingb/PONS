from copy import copy, deepcopy
import pons
from pons import Router, PayloadMessage, Hello
from pons.event_log import event_log
from typing import override

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
        # self.store = []
        self.next_beacon_contains_IDs = False

        self.broadcast_delay = 0.05 # TODO is this in seconds??
        self.broadcasting_from_queue = False
        self.broadcast_queue: list[pons.PayloadMessage] = list()

    @override
    def __str__(self):
        return "HypergossipRouter"

    @override
    def on_peer_discovered(self, peer_id):
        pass

    @override
    def on_msg_received(self, msg, remote_id):
        pass

    @override
    def on_receive(self, msg: pons.Message, remote_node_id: int) -> None:
        """
        Extend router on_receive function with a switch for hypergossip messages.
        """
        if isinstance(msg, IDsMessage):
            self.__parse_br_information(msg.get_IDs())
            self.__check_beacon_process(msg)
            self._on_scan_received(deepcopy(msg), remote_node_id)
        elif isinstance(msg, Beacon):
            self.__check_beacon_process(msg)
            self._on_scan_received(deepcopy(msg), remote_node_id)
        else:
            super().on_receive(msg, remote_node_id)

    def forward(self, msg: pons.PayloadMessage):
        """
        TODO gossip forward, add bc queue, start queue forward

        :param msg:
        :return:
        """
        raise NotImplementedError

    def __broadcast_from_queue(self):
        if len(self.broadcast_queue) == 0:
            self.broadcasting_from_queue = False
            return

        # simpy event process
        while True:

            while len(self.broadcast_queue) > 0:
                # retrieve a valid message from the beginning of the queue, directly drop invalid ones
                msg = self.broadcast_queue.pop(0)
                if isinstance(msg, pons.PayloadMessage) and not msg.is_expired(self.netsim.env.now):
                    break
            else:
                # no valid messages, terminate broadcast process
                self.broadcasting_from_queue = False
                return

            # found message, rebroadcast
            self.send(pons.BROADCAST_ADDR, msg)

            yield self.env.timeout(self.broadcast_delay)



    def __parse_br_information(self, br_info: list[str]):
        raise NotImplementedError

    def __check_beacon_process(self, msg: pons.Beacon) -> None:
        """
        Check if the next beacon should contain more detailed information on known messages, based on the
        known message hash of another node's received beacon.

        :param msg: beacon message
        """
        if msg.src not in self.peers:
            if not msg.cluster_hash == self.__local_buffer_hash():
                self.next_beacon_contains_IDs = True

    def __local_buffer_keys(self) -> list:
        # TODO
        raise NotImplementedError

    def __local_buffer_hash(self) -> int:
        # TODO
        raise NotImplementedError

    def __neighborhood_hash(self) -> int:
        # TODO
        raise NotImplementedError

    @override
    def on_duplicate_msg_received(self, msg: pons.PayloadMessage, remote_node_id: int) -> None:
        """
        Remove received duplicates from the broadcast queue (if scheduled).
        """
        self.log("Duplicated payload message received: %s from %d" % (msg, remote_node_id))

        if msg in self.broadcast_queue:
            self.log("Remove duplicated message from broadcast queue: %s" % msg)
            self.broadcast_queue.remove(msg)

    @override
    def scan(self):
        """
        Overwrite scan of router to allow sending beacons instead of hello messages.
        """
        super.last_peer_found = self.netsim.env.now

        while True:
            self.peers.clear()

            # send beacons only while not already transmitting
            if not self.broadcasting_from_queue:

                # Switch beacon type to contain message IDs when required
                if self.next_beacon_contains_IDs:
                    self.netsim.nodes[self.my_id].send(
                        self.netsim,
                        pons.BROADCAST_ADDR,
                        pons.IDsMessage(
                            self.my_id,
                            self.netsim.env.now,
                            self.__local_buffer_keys(),
                            self.__neighborhood_hash()
                        ),
                    )
                else:
                    self.netsim.nodes[self.my_id].send(
                        self.netsim,
                        pons.BROADCAST_ADDR,
                        pons.Beacon(
                            self.my_id,
                            self.netsim.env.now,
                            self.__local_buffer_hash(),
                            self.__neighborhood_hash()
                        ),
                    )
            yield self.env.timeout(self.scan_interval)




class Beacon(Hello):
    """HG Beacon with additional cluster information"""

    def __init__(self, src: int, created: float, message_hash: int, cluster_hash: int):
        super().__init__(src, created)
        self.cluster_hash = cluster_hash
        self.message_hash = message_hash

    @override
    def size(self) -> int:
        # add cluster ID
        # TODO
        print("check size of integer!", self.cluster_hash.__sizeof__())
        return super().size() + self.cluster_hash.__sizeof__() + self.message_hash.__sizeof__()


class IDsMessage(Beacon):
    """Extended beacon containing known message IDs of a node"""

    def __init__(self, src: int, created: float, message_ids: list[str], cluster_id: int):
        super().__init__(src, created, cluster_id)
        self.IDs: list[str] = deepcopy(message_ids)

    def get_IDs(self) -> list[str]:
        return deepcopy(self.IDs)

    @override
    def size(self) -> int:
        # add msg IDs
        # TODO could be improved by not using strings but UUIDs as identifiers
        s: int = super().size()
        for id in self.IDs:
            s += len(id.encode('utf-8'))
        return s
