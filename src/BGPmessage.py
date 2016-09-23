#!/usr/bin/python

class BGPmessage:
    def __init__(self,ts,type):
        self.next_hop = None
        self.source = None
        self.timestamp = ts
        self.type = type
        self.aspath = []
        self.announce = []
        self.withdraw = []

    def set_source(self, src):
        self.source = src

    def set_nexthop(self, hop):
        self.next_hop = hop

    def add_as_to_path(self, asn):
        self.aspath.append(asn)

    def add_announce(self, prefix):
        self.announce.append(prefix)

    def add_withdraw(self, prefix):
        self.withdraw.append(prefix)
