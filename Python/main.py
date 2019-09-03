from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
from couchbase.n1ql    import N1QLQuery, N1QLError
import couchbase.exceptions as CBErr
import json
from time import sleep

import couchbase, logging
couchbase.enable_logging() # allows us to see warnings for slow/orphaned operations

# set logging to WARNING level so we can see warnings
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
logging.getLogger().addHandler(ch)

cluster = Cluster('couchbase://10.143.191.101/')
authenticator = PasswordAuthenticator('Danzibob', 'C0uchbase123')
cluster.authenticate(authenticator)
bucket = cluster.open_bucket('travel-sample')

def getHint(err):
    return str(err).split(", ")[1]

class RetriesExceededException(Exception):
    pass

"""
This section is largely copied from https://docs.couchbase.com/python-sdk/2.5/failure-considerations.html

Timeouts
Timeouts are caused by an unresponsive network or software system. This could mean that a node or the client is congested or the connection down altogether.
Warning: Timeout does not mean the operation failed - only that an acknowlegement was not recieved within the specified time limit.

From Python SDK docs (current - sdk 2.5):
The client library will generally not retry timed-out operations: by definition, a timed out operation is an operation which could not be completed within a given amount of time.
Likewise the application should not retry a timed-out operation. The recommended action is to report the failure up the stack.
If your application cannot simply return an error but must ensure that the operation completes, it is suggested that you wait a while until retrying (similar to the mechanism used when receiving a temporary failure).
[doesn't mention replica reads]

From CBServer docs (older - 4.x):
??? however replica reads can be a valid approach if getting the data is of utmost importance.
A replica read will retrieve a possibly stale version of the item from a server where it had a replica, if that server is still available.


Network Errors
Network errors are returned by the client if it cannot establish a network connection to the server, or if an existing network connection was terminated.
Warning: Network error does not indicate the failure of the operation.

The cause of a network error may be included in the error object itself (for example, couldn’t resolve host name, connection refused, no route to host).
Like timeout errors, network errors may be transient (indicative of a bad network connection). They may also be a result of a node being failed over.
Applications may retry operations which failed with network errors after waiting for a while (as in timeout errors). 
Retrying operations too soon may result in creating massive amounts of TIME_WAIT connections on the client, and in extreme cases, even cause system crashes or rendering it inaccessible via the network.
For cases where it is of the utmost importance to retrieve the item, a read from a replica can be performed. This will retrieve a potentially stale item.


Missing Node Error
A Missing Node error is reported when the cluster is in a degraded state. 
One of the cluster nodes has been failed over and no replica exists to take its place.
As a result, a portion of the cluster’s dataset becomes unavailable.
This kind of failure must be fixed by server side administrative actions

Temporary Fail Error
A Temporary Fail error is thrown when a server is running, but not able to service the request.
For example, the server may still be starting up, or shutting down.
...
The data requested is not available at all. Application should report the failure up the application stack.
"""
# First tries a normal get, and if the request times out, tries to get the replica
def getNormalOrReplica(docID):
    try:
        result = bucket.get(docID)
    # except (CBErr.TimeoutError, CBErr.CouchbaseNetworkError) as e:
    except CBErr.CouchbaseNetworkError as e:
        print("Unable to access node:", getHint(e))
        print("Trying a replica read")
        result = bucket.get(docID, replica=True)
    except Exception as e:
        print("Encountered unexpected exception:", getHint(e))
        raise e
    print("Got result: ", result.value['airportname'])
    return result

# Gets with retries if first attempt fails.
# Is blocking. Could be tweaked to use asyncio
def getOrRetry(docID, retries=2, delay=100):
    if retries <= -1: raise RetriesExceededException
    try:
        result = bucket.get(docID)
    except (CBErr.TimeoutError, CBErr.CouchbaseNetworkError, CBErr.TemporaryFailError) as e:
        print("Transient:", e.is_transient)
    # except CBErr.CouchbaseNetworkError as e:
        print("Unable to access node:", getHint(e))
        print("Waiting then trying again")
        sleep(delay/1000)
        return getOrRetry(docID, retries-1, delay)
    except Exception as e:
        print("Encountered unexpected exception:", getHint(e))
        raise e
    print("Got result: ", result.value['airportname'])
    return result

# Retries twice then falls back to getting a replica
def getRetryThenReplica(docID):
    try:
        result = getOrRetry(docID)
    except RetriesExceededException:
        print("No result after retries, falling back to replicas")
        result = bucket.get(docID, replica=True)
    return result

# print(getRetryThenReplica("airport_1255"))

"""
In this example we're using replica reads to get data about an airport (location, ffa and icao codes, etc.).
We know that for our application, this data is almost entirely static, so using replicas is unlikely to cause any consistency issues, 
while also allowing users to still have access to search the database for flights and destinations.
For other parts of the application (for example, actually booking a flight), consistency is more important than availability.
In that case, replica reads should be avoided in favour of ensuring absolute consistency.
However this brings up other issues. For timeouts and network errors, we need to check if an operation was actaully applied, 
and only retry if it was not (for example, make sure flight was booked and don't accidentally double book or overwrite a booking).

In server 6.5+ and sdk 3+ the new transactions feature is ideal for this purpose
"""

# These functions perform an operation, then
# return True or False if the action validated
# or raise an exception if validation fails

# Key may or may not pre-exist
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
            # otherwise, an increased CAS value means the operation was performed
            return (not oldCAS) or (res.cas > oldCAS)
        except RetriesExceededException:
            raise RetriesExceededException("Couldn't confirm or deny operation on key",docID)
        except CBErr.NotFoundError:
            print("Document not found on server, upsert failed")
            return False
        except Exception as e:
            print("Unexpected Exception:",e)
            raise e

# try:
#     success = upsertAndCheck("memes",{'top':'kek','my':'dude'})
#     print("Upsert succeeded:", success)
# except RetriesExceededException:
#     print("Failed to validate upsert")
# except Exception as e:
#     print("Unexpected Exception:",e)


"""
Working with N1QL queries

Using N1QL in the event of a node failure is less trivial than single document operations.
If a node has failed entirely, the data requested may still be available if the relevant GSI is available and only indexed data is requested.
e.g. we have the airportname index on node, but the city index is on node 2.

Trying to use an unavailable index will throw a HTTP 404 error, containing a message saying the index is missing.
While it would be possible to recreate the index through the sdk, this can take a very long time.
It may be better just to run a rebalance or restart the node - thus bringing the old index back.
Index replicas can also be used to prevent this problem (activated on index creation using WITH {"num_replica": 2})

Even if the index is available, the document(s) in question may not be. This can cause a n1ql.N1QLError (even if most of the documents are available).
    NB: THE ERROR ONLY HAPPENS WHEN YOU TRY TO UNPACK THE RESULTS

(This works sometimes but usually times out...)
        A work-around here is to simplify the query to get only the document IDs from the index, then fetch the documents manually.
        This allows the available docs to be fetched, but also allows replica reads for the unavailable documents.

But again, ideally this should be solved through administrative action to bring the cluster back up to full operation.

If only data contained in an available index is requested, the query should succeed with no issues.

When a node is down it is not uncommon for a N1QL query to time out, as it is trying and timing out on many connections internally.
For large or complex queries, it may be wise to fail gracefully and wait for the server to come back to full operation
"""
def N1QLFetchAirports(search, field='airportname'):
    query = "SELECT airportname, city FROM `travel-sample` WHERE LOWER(airportname) LIKE $1"
    simple_query = "SELECT meta().id FROM `travel-sample` WHERE LOWER(airportname) LIKE $1"
    param = "%" + search.lower() + "%"
    try:
        q = N1QLQuery(query, param)
        res = bucket.n1ql_query(q)
        for r in res:
            print(r)
    except N1QLError as e:
        print(e)
        q = N1QLQuery(simple_query, param)
        q.timeout = 300
        docIDs = bucket.n1ql_query(q)
        res = []
        ids = []
        # Unpacked seperately from handling to avoid query timing out (??)
        for docMeta in docIDs:
            ids.append(docMeta['id'])
        for x in ids:
            try:
                # If it's known which buckets are down, possible to use get_multi
                # Also valid to use anyway if we don't mind getting replicas even if an active copy is available
                res.append(getNormalOrReplica(x))
            except Exception as e:
                print("Failed to get " + x)
                print(e)
        maxlen = max([len(r.value["airportname"]) for r in res])
        for r in res:
            print("{0:{2}} ({1})".format(r.value['airportname'], r.value['city'], maxlen + 2))
    except Exception as e:
        print("Exited with error:", e)
    

N1QLFetchAirports("per")
#N1QLFetchAirports("per","city")













# Enabling LCB logging
# terminal$ LCB_LOGLEVEL=[1-5] node index.js
# Obviously can also log yourself

# Monitoring - ???
# Manually keep track of failures, operation times etc.??
# Manually parse logs ?????

""" Questions for sdk peeps

When else to use replica reads (other than timeouts?)
    - Programatically debug network failure
        - To determine whether to try again

How else to deal with timeouts?
  Timeout diagnosis? (Support question?)
    - Network congestion
    - Host up, CBServer not; Host down; Server overloaded; etc...
    - Conflicting info from server and sdk docs

Possible network issues - causes & resolutions
  Some nodes disconnect: What errors? Replica reads
  All nodes unavailable: Just give up? retry?
  Intermittent connection: How to diagnose? (timeouts?) Retries?

Logging & Monitoring
  Threshold logging exists
  Monitoring??????????????? (server side?)

Most common other errors (also ask support)

------====== Takeaways ======------

is_transient flag = retryable error
    Most sensible things are already retried at sdk level (including Document level ops - GET, INSERT etc.)
    Application level should probably only worry about things like longer network outages (seconds vs ms)

Timeouts - Either fail-fast or wait a few seconds (w/ some sort of back-off style function?)
    No point waiting in the order of ms, as a timeout may take a second worth of retries in itself

Network issues - check is_transient flag? handle similar to timeouts
    Alternatively if not transient, fail-fast

Other errors - Auth errors before cluster has fully started? (or sometimes if it's totally down)

"""