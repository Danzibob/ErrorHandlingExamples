import couchbase, logging, json, random, time, sys
from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
from couchbase.n1ql    import N1QLQuery, N1QLError
import couchbase.exceptions as CBErr

# Cluster address(es) and credentials
CONN_STR = 'couchbase://10.143.191.101,10.143.191.102,10.143.191.103'
USERNAME = 'Danzibob'
PASSWORD = 'C0uchbase123'

# Enable logging for slow/orphaned operations
couchbase.enable_logging()
# Set logging to WARNING level so we can see warnings
ch = logging.StreamHandler()
ch.setLevel(logging.WARN)
logging.getLogger().addHandler(ch)

# --== Threshold logging configuration ==--
# Number of microseconds a kv operation has to take to be considered slow,
# thus adding it to the queue. Default is 500000 (500ms)
bucket.tracing_threshold_kv=1
# Process the collected spans every n ms. Default 10000 (10s)
bucket.threshold_logging_tracer_interval=10000
# Keep track of the slowest n items. Default is 10
bucket.tracing_threshold_queue_size=1000
# Keep track of up to n orphaned responses. Default is 10
bucket.tracing_orphaned_queue_size=1000

# Connect to the cluster and bucket
cluster = Cluster(CONN_STR)
authenticator = PasswordAuthenticator(USERNAME, PASSWORD)
cluster.authenticate(authenticator)
bucket = cluster.open_bucket('travel-sample')
print("Connected.")

def getHint(err):
    return str(err).split(", ")[1]

# Custom error class 
class RetriesExceededException(Exception):
    pass


# First tries a normal get, and if the request times out, tries to get the replica
def getNormalOrReplica(docID):
    try:
        result = bucket.get(docID)
    except (CBErr.TimeoutError, CBErr.CouchbaseNetworkError) as e:
        result = bucket.get(docID, replica=True)
    except Exception as e:
        raise e
    return result

# Gets with retries if first attempt fails.
# Backoff factor can somewhat mitigate overloading the server with too many Gets at once
def getOrRetry(docID, retries=2, delay=1000, backoff_factor=1):
    if retries <= -1: raise RetriesExceededException
    try:
        result = bucket.get(docID)
    except (CBErr.TimeoutError, CBErr.CouchbaseNetworkError, CBErr.TemporaryFailError) as e:
        time.sleep(delay/1000)
        return getOrRetry(docID, retries-1, delay*backoff_factor)
    except Exception as e:
        raise e
    return result

# Retries, then falls back to getting a replica
# Is VERY slow to time out. Can we decrease the timeout? check the node is up? etc.
def getRetryThenReplica(docID):
    try:
        result = getOrRetry(docID)
    except RetriesExceededException:
        result = bucket.get(docID, replica=True)
    return result

# Returns True/False if upsert was/wasn't completed
# In the event of an error, outcome is still unknown.
# In this case, retrying the whole operation is valid
def upsertAndCheck(docID, value):
    try:
        oldCAS = bucket.get(docID).cas
    except CBErr.NotFoundError:
        oldCAS = False
    try:
        bucket.upsert(docID, value)
        return True
    except (CBErr.TimeoutError, CBErr.CouchbaseNetworkError ,CBErr.TemporaryFailError):
        try:
            res = getOrRetry(docID, 4, 1000)
            # If there's no oldCAS and we got the document (ie: no error), insert successful
            # otherwise, an different CAS value means an operation was performed
            return (not oldCAS) or (res.cas != oldCAS)
        except RetriesExceededException:
            raise RetriesExceededException("Couldn't confirm or deny operation on key " + docID)
        except CBErr.NotFoundError:
            return False
        except Exception as e:
            raise e

# The ping method can be used to check which nodes are available
# And also gives per-service information
# This can be useful for diagnosing issues at the application level
def getNodes():
    print(bucket.ping())
    # What do the status numbers mean??
    return [x['server'].split(':')[0] for x in bucket.ping()['kv'] if x['status'] == 0 ]

# Gets N1QL data from replicas using a simpler n1ql query that uses only indexed info
def N1QLFetchAirports(search, field='airportname'):
    query = "SELECT airportname, city FROM `travel-sample` WHERE LOWER(airportname) LIKE $1"
    simple_query = "SELECT meta().id FROM `travel-sample` WHERE LOWER(airportname) LIKE $1"
    param = "%" + search.lower() + "%"
    res = False
    try:
        q = N1QLQuery(query, param)
        res = bucket.n1ql_query(q)
    except N1QLError as e:
        q = N1QLQuery(simple_query, param)
        docMetas = bucket.n1ql_query(q)
        ids = [meta['id'] for meta in docMetas]
        try:
            res = bucket.get_multi(ids)
        except CBErr.CouchbaseNetworkError as e:
            res = e.all_results
            failedKeys = [k for k,v in res.items() if v.value == None]
            failedReplicas = bucket.get_multi(failedKeys, replica=True)
            # TODO: Check for failed gets here too
            res.update(failedReplicas)
    return res
    

# Load a couple of docs and write them back
for j in range(1000):
    for i in range(0,10):
        keyname = "airline_1" + str(i)
        try:
            doc = bucket.get(keyname);
            if doc and doc.value:
                bucket.upsert(keyname, doc.value);
        except:
            continue
time.sleep(20)