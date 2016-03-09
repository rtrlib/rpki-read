# PROVR

__PRefix Origin Validation in Realtime__

The PRefix Origin Validation in Realtime (PROVR) framework aims to provide a
consistent (and live) view on the RPKI validation state of currently announced
IP prefixes. That is, it verifies association of an IP prefix and its BGP origin
AS (autonomous system). Resulting validation states are:

* _NotFound_, if no RPKI entry exists for a prefix
* _Valid_, if at least one matching entry (ROA) was found in the RPKI database
* _InvalidLength_, if there is an entry (ROA) for a prefix with matching origin AS, but the prefix length mismatches
* _InvalidAS_, if there is an entry (ROA) for a prefix, but the origin AS does not match

The PROVR monitoring system has two parts: the backend storing latest validation
results in a database, and the (web) frontend to display these results as well
as some statistics derived from them.
The backend connects to a live XML stream of a known BGPmon instance, parses
BGP update messages to extract IP prefixes and origin AS information. Prefix
origins are validated using RTRlib client to query a RPKI cache.
