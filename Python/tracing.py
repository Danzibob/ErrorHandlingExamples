from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
from couchbase.n1ql    import N1QLQuery, N1QLError
import couchbase.exceptions as CBErr
import json
from time import sleep, time
import couchbase, logging, sys
from opentracing_pyzipkin.tracer import Tracer
import requests, random

#[imports]

CONN_STR = 'couchbase://10.143.191.101,10.143.191.102,10.143.191.103'
cluster = Cluster(CONN_STR)
authenticator = PasswordAuthenticator('Danzibob', 'C0uchbase123')
cluster.authenticate(authenticator)
bucket = cluster.open_bucket('travel-sample')

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
couchbase.enable_logging() # allows us to see warnings for slow/orphaned operations
# set logging to INFO level so we can see everything
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logging.getLogger().addHandler(ch)

# Threshold logging
queue_size=100000000
bucket.tracing_threshold_kv=1 
bucket.threshold_logging_tracer_interval=100
bucket.tracing_threshold_queue_size=queue_size
bucket.tracing_orphaned_queue_size=queue_size
for j in range(1000):
    for i in range(0,10):
        keyname = "airline_1" + str(i)
        try:
            doc = bucket.get(keyname);
            if doc and doc.value:
                bucket.upsert(keyname, doc.value);
        except:
            continue

sleep(60)