# environment
rbv_base_path = "."
validator_path = rbv_base_path + "/util/cli-validator"

validity_state = ['Valid','NotFound','Invalid','InvalidAS','InvalidLength']
validity_descr = [  "At least one VRP Matches the Route Prefix",
                    "No VRP Covers the Route Prefix",
                    "At least one VRP Matches the Route Prefix, but no VRP ASN or the Route Prefix length is greater than the maximum length allowed by VRP(s) matching this route origin ASN",
                    "At least one VRP Covers the Route Prefix, but no VRP ASN matches the route origin ASN",
                    "At least one VRP Covers the Route Prefix, but the Route Prefix length is greater than the maximum length allowed by VRP(s) matching this route origin ASN"]

# connections
default_cache_server = {"host": "rpki-validator.realmv6.org", "port": 8282}
default_bgpmon_server = {"host": "localhost", "port": 50001}

BULK_MAX_OPS = 10000
BULK_TIMEOUT = 30
PURGE_MAX_ITEMS = 30000
PURGE_TIMEOUT = 60
STATS_TIMEOUT = 60
MAX_COUNTER = 10000
