from dataclasses import dataclass
import random
from typing import override

import pons


class Message:
    """Message generic type"""
    def __init__(self, id: str, src: int, dst: int, created: float):
        self.id = id
        self.src = src
        self.dst = dst
        self.created = created

    @override
    def __str__(self):
        return "Message(%s, src=%d, dst=%d, size=%d)" % (
            self.id,
            self.src,
            self.dst,
            self.size(),
        )

    def size(self) -> int:
        '''
        Message size in byte.

        TODO
        size of str/id
        size of src+dst+created (int+int+float)

        :return:
        '''
        return 42


@dataclass
class PayloadMessage(Message):
    """A message."""

    def __init__(self, id: str, src: int, dst: int,
                 created: float,
                 size: int|None = None, # TODO what is size? only used to set size???
                 ttl: int = 3600, # TODO why 3600 as standard?
                 src_service: int = 0,
                 dst_service: int = 0,
                 content: dict|None = None,
                 metadata = None):
        super().__init__(id, src, dst, created)
        self.size = size
        self.hops = 0
        self.ttl = ttl
        self.src_service = src_service
        self.dst_service = dst_service
        self.content = content
        self.metadata = metadata

    @override
    def size(self) -> int:
        if self.size is not None:
            return self.size
        else:
            # TODO add real message size
            return super().size() # TODO

    def __str__(self):
        return "Message(%s, src=%d.%d, dst=%d.%d, size=%d)" % (
            self.id,
            self.src,
            self.src_service,
            self.dst,
            self.dst_service,
            self.size,
        )

    def unique_id(self) -> str:
        return "%s-%d-%d" % (self.id, self.src, self.created)

    def is_expired(self, now):
        # print("is_expired: %d + %d > %d" % (self.created, self.ttl, now))
        return now - self.created > self.ttl


class Hello(Message):
    """Hello Beacon"""
    def __init__(self, src: int, created: float):
        super().__init__("HELLO", src, pons.BROADCAST_ADDR, created)


class Beacon(Hello):
    """Beacon with additional cluster information"""
    def __init__(self, src: int, created: float, cluster_id: int):
       super().__init__(src, created)
       self.cluster_id = cluster_id

    @override
    def size(self) -> int:
        # add cluster ID
        # TODO
        print("check size of integer!", self.cluster_id.__sizeof__())
        return super().size() + self.cluster_id.__sizeof__()

def message_event_generator(netsim, msggenconfig):
    """A message generator."""
    env = netsim.env
    counter = 0
    print("start message generator")
    while True:
        # check if interval is a tuple
        if isinstance(msggenconfig["interval"], tuple):
            yield env.timeout(
                random.randint(msggenconfig["interval"][0], msggenconfig["interval"][1])
            )
        else:
            yield env.timeout(msggenconfig["interval"])
        netsim.routing_stats["created"] += 1
        counter += 1
        if isinstance(msggenconfig["src"], tuple):
            src = random.randint(msggenconfig["src"][0], msggenconfig["src"][1] - 1)
        else:
            src = msggenconfig["src"]
        if isinstance(msggenconfig["dst"], tuple):
            dst = random.randint(msggenconfig["dst"][0], msggenconfig["dst"][1] - 1)
        else:
            dst = msggenconfig["dst"]
        if isinstance(msggenconfig["size"], tuple):
            size = random.randint(msggenconfig["size"][0], msggenconfig["size"][1])
        else:
            size = msggenconfig["size"]
        if "ttl" not in msggenconfig:
            ttl = 3600
        elif isinstance(msggenconfig["ttl"], tuple):
            ttl = random.randint(msggenconfig["ttl"][0], msggenconfig["ttl"][1])
        else:
            ttl = msggenconfig["ttl"]
        msgid = "%s%d" % (msggenconfig["id"], counter)
        msg = PayloadMessage(msgid, src, dst, size=size, created=env.now, ttl=ttl)
        netsim.nodes[src].router.add(msg)


def message_burst_generator(netsim, msggenconfig):
    """A message generator."""
    env = netsim.env
    counter = 0
    print("start message burst generator")
    while True:
        # check if interval is a tuple
        if isinstance(msggenconfig["interval"], tuple):
            yield env.timeout(
                random.randint(msggenconfig["interval"][0], msggenconfig["interval"][1])
            )
        else:
            yield env.timeout(msggenconfig["interval"])

        for src in range(msggenconfig["src"][0], msggenconfig["src"][1]):
            netsim.routing_stats["created"] += 1
            counter += 1
            if isinstance(msggenconfig["dst"], tuple):
                dst = random.randint(msggenconfig["dst"][0], msggenconfig["dst"][1] - 1)
            else:
                dst = msggenconfig["dst"]
            if isinstance(msggenconfig["size"], tuple):
                size = random.randint(msggenconfig["size"][0], msggenconfig["size"][1])
            else:
                size = msggenconfig["size"]
            if "ttl" not in msggenconfig:
                ttl = 3600
            elif isinstance(msggenconfig["ttl"], tuple):
                ttl = random.randint(msggenconfig["ttl"][0], msggenconfig["ttl"][1])
            else:
                ttl = msggenconfig["ttl"]
            msgid = "%s%d" % (msggenconfig["id"], counter)
            msg = PayloadMessage(msgid, src, dst, size=size, created=env.now, ttl=ttl)
            # print("create message %s (%d->%d)" % (msgid, src, dst))
            netsim.nodes[src].router.add(msg)
