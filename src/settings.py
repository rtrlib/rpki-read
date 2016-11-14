# environment
BASE_PATH = "."
VALIDATOR_PATH = BASE_PATH + "/util/cli-validator"

VALIDITY_STATE = ['Valid','NotFound','Invalid','InvalidAS','InvalidLength']
VALIDITY_DESCR = [  "At least one VRP Matches the Route Prefix",
                    "No VRP Covers the Route Prefix",
                    "At least one VRP Matches the Route Prefix, but no VRP ASN or the Route Prefix length is greater than the maximum length allowed by VRP(s) matching this route origin ASN",
                    "At least one VRP Covers the Route Prefix, but no VRP ASN matches the route origin ASN",
                    "At least one VRP Covers the Route Prefix, but the Route Prefix length is greater than the maximum length allowed by VRP(s) matching this route origin ASN"]

# connections
DEFAULT_CACHE_SERVER = {"host": "rpki-validator.realmv6.org", "port": 8282}
DEFAULT_BGPMON_SERVER = {"host": "localhost", "uport": 50001, "rport": 50002}
DEFAULT_BGPSTREAM_COLLECTOR = "route-views.linx"
DEFAULT_MONGO_DATABASE = {"uri": "mongodb://localhost:27017/rpki-read"}
DEFAULT_WEB_SERVER = {"host": "0.0.0.0", "port": 5100}
DEFAULT_LOG_LEVEL = "CRITICAL"

WAIT_TO_SYNC_FILE = "/tmp/rpki-read.sync"
BULK_MAX_OPS = 10000
BULK_TIMEOUT = 30
RIB_TS_INTERVAL = 7200
RIB_TS_WAIT = 600
SERVICE_INTERVAL = 600
DOSTATS_INTERVAL = 600
MAX_COUNTER = 10000
QUEUE_LIMIT = (256 * 1024)
