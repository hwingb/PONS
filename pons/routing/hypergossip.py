from copy import copy, deepcopy
import pons
from pons import Router, PayloadMessage, Hello
from pons.event_log import event_log
from typing import override
import random
import hashlib

"""
TODO 
- requires to set random seed again?

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

        self.neighbors: dict[int, float] = {}

        self.neighborhood_removal_time = self.scan_interval * 2

    @override
    def __str__(self):
        return "HypergossipRouter"

    @override
    def add(self, msg: pons.PayloadMessage) -> None:
        """
        If possible, add a new message to the system. Add it directly as first element
        to the broadcast queue, activate transmission if required.
        """
        if self._store_add(msg):
            self.broadcast_queue.insert(0, msg)

            if not self.broadcasting_from_queue:
                self.__broadcast_from_queue()


    @override
    def on_msg_received(self, msg: pons.PayloadMessage, remote_node_id) -> None:
        """
        Handle incoming message. If explicitly for this node, router has
        already handled delivery to the upper layers. Only handle forwarding
        to others in this place.
        """
        self.log("msg received: %s from %d" % (msg, remote_node_id))
        if msg.dst != self.my_id:
            self._store_add(msg)
            self.forward(msg)


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


    def forward(self, msg: pons.PayloadMessage) -> None:
        """
        Forward a received message with a specific probability (aka gossiping!).
        Only for received messages! Messages from a node's application must use the add function
        for correct handling.
        """

        if random.random() <= self._gossip_probability():
            self.log("Gossip: Message %s added to broadcast queue." % msg)
            self.broadcast_queue.append(msg)

            # start broadcast if required
            if not self.broadcasting_from_queue:
                self.__broadcast_from_queue()
        else:
            self.log("Message %s was not forwarded because of gossip drop." % msg)


    def __broadcast_from_queue(self) -> None:
        """
        Broadcast process to send packets from the queue.
        """

        if len(self.broadcast_queue) == 0:
            self.broadcasting_from_queue = False
            return

        self.broadcasting_from_queue = True
        self.log("Start broadcast from queue.")

        # simpy event process
        while self.broadcasting_from_queue:

            while len(self.broadcast_queue) > 0:
                # retrieve a valid message from the beginning of the queue, directly drop invalid ones
                msg = self.broadcast_queue.pop(0)
                if isinstance(msg, pons.PayloadMessage) and not msg.is_expired(self.netsim.env.now):
                    break
                else:
                    self.log("Drop message %s from broadcast queue." % msg)
            else:
                # no valid messages, terminate broadcast process
                self.broadcasting_from_queue = False
                self.log("Stop broadcast from queue.")
                return

            # found message, rebroadcast
            self.log("Broadcast from queue.")
            self.send(pons.BROADCAST_ADDR, msg)

            yield self.env.timeout(self.broadcast_delay)



    def __parse_br_information(self, br_info: list[str]):
        """
        On reception of another node's broadcast received information table,
        a node filters out messages already known to both from its own message
        store and puts messages not known to the other node into its
        broadcasting queue for further spread.
        """

        # all unique ids of known messages
        delta_msgs: list[str] = self.__local_buffer_keys()
        # remove duplicates in both lists (already known to sender of br info)
        delta_msgs = [item for item in delta_msgs if item not in br_info]

        if len(delta_msgs) == 0:
            self.log("BR info received, no delta to known messages found")
            return

        # retrieve all messages from the store via their ID that are in delta_msgs
        msgs_to_send = [msg for msg in self.store if msg.unique_id() in delta_msgs]

        for msg in msgs_to_send:
            # filter messages already in the BC queue
            if msg not in self.broadcast_queue:
                self.broadcast_queue.append(msg)

        # start broadcast if required
        if not self.broadcasting_from_queue:
            self.__broadcast_from_queue()


    def __check_beacon_process(self, msg: pons.Beacon) -> None:
        """
        Check if the next beacon should contain more detailed information on known messages, based on the
        known message hash of another node's received beacon.

        :param msg: beacon message
        """
        if msg.src not in self.peers:
            if not msg.cluster_hash == self.__local_buffer_hash():
                self.next_beacon_contains_IDs = True


    def __local_buffer_keys(self) -> list[str]:
        """
        :return: List of unique IDs of messages inside the node's storage.
        """
        return [msg.unique_id() for msg in self.store]

    def __local_buffer_hash(self) -> int:
        """
        :return: Integer of (16 byte) md5 hash of local buffer message IDs
        """
        # sort set to ensure consistent order
        sorted_strings = sorted(self.__local_buffer_keys())
        # concatenate strings
        concatenated_string = ''.join(sorted_strings)
        # calculate 16 byte-hash
        hash_int = int(hashlib.md5(concatenated_string.encode()).digest())
        return hash_int


    def __neighborhood_hash(self) -> int:
        """
        :return: Integer of (16 byte) md5 hash of known neighbors / peers
        """
        # Sort the set to ensure consistent order
        sorted_integers = sorted(self.peers)
        # convert to string for hashing
        concatenated_integers = ''.join(map(str, sorted_integers))
        # calculate 16 byte-hash
        hash_int = int(hashlib.md5(concatenated_integers.encode()).digest())
        return hash_int


    def _gossip_probability(self):
        n = len(self.peers)
        return (
            0.3 if n >= 23 else
            0.5 if n >= 15 else
            0.6 if n >= 12 else
            0.7 if n >= 10 else
            0.8 if n >= 8 else
            1
        )

    @override
    def on_duplicate_msg_received(self, msg: pons.PayloadMessage, remote_node_id: int) -> None:
        """
        Duplicate payload message received.
        If scheduled, remove received duplicates from the broadcast queue to reduce load on the wireless medium.
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
            # self.peers.clear()
            # I think clearing all peers is not a good idea! Should be adapted to allow for missed beacons and more
            # adaptive neighborhood discovery approaches
            for k in self.neighbors.keys():
                # remove neighbor if have not seen within multiple consecutive intervals
                if self.neighbors.get(k) + self.neighborhood_removal_time < self.env.now:
                    self.neighbors.pop(k)
                    # also remove from router's peer list
                    self.peers.pop(k)

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

    def on_scan_received(self, msg: pons.Hello, remote_node_id: int):
        """
        Add the peer and the time of the reception to the neighborhood list.
        """
        self.log("[%s] scan received: %s from %d" % (self.my_id, msg, remote_node_id))
        self.neighbors[remote_node_id] = self.env.now



class Beacon(Hello):
    """HG Beacon with additional cluster information"""

    def __init__(self, src: int, created: float, message_hash: int, cluster_hash: int):
        super().__init__(src, created)
        self.cluster_hash = cluster_hash
        self.message_hash = message_hash

    @override
    def size(self) -> int:
        # add cluster and message hash (each 16 byte integers) to message size
        return super().size() + 32


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
        # TODO could probably be improved by not using strings but UUIDs as identifiers?
        s: int = super().size()
        for id in self.IDs:
            s += len(id.encode('utf-8'))
        return s
