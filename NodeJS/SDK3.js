const couchbase = require('couchbase')
const CBErr = couchbase.errors
const N1qlQuery = couchbase.N1qlQuery

const cluster = new couchbase.Cluster(
    'couchbase://10.143.191.101,10.143.191.102,10.143.191.103', {
        username: 'Danzibob',
        password: 'C0uchbase123'
})

const bucket = cluster.bucket('travel-sample');
const collection = bucket.defaultCollection();

class RetriesExceededError extends Error {
    constructor(message) {
        super(message);
        this.name = "RetriesExceededError"
    }
}

function logErr(err){
    console.log(err)
    // Object.getOwnPropertyNames(err).map(x => console.log("\t", x, " => ", err[x]))
    for(let e of [couchbase.NetworkError, couchbase.TimeoutError, couchbase.KeyValueError, couchbase.QueryError]){
        console.log(e, err instanceof e)
    }
}

function getNormalOrReplica(key, callback){
    collection.get(key)
        .then(res => callback(null, res))
        .catch(err => {
            logErr(err)
            if(err.code == 23){
                collection.getReplica(key, callback)
            } else {
                callback(err)
            }
        })
}

// getNormalOrReplica("airport_1254", (err, res) => {
//     console.log(typeof err, err, res)
// })

async function getActiveOrReplicaAwait(key) {
    try {
        return await collection.get(key)
    } catch (err) {
        if(err.code == 23) {
            return await collection.getFromReplica(key, -1)
        } else {
            throw err
        }
    }
}

// getActiveOrReplicaAwait("airport_1254")
//     .then( res => console.log(res))
//     .catch(err => logErr(err))


function getOrRetry(key, callback, retries=2, delay=100, backoff_factor=1){
    collection.get(key)
        .then(res => callback(null, res))
        .catch(err => {
            logErr(err)
            if(err.code == 23) {
                if(retries){
                    setTimeout(() => getOrRetry(key, callback, retries-1, delay*backoff_factor, backoff_factor), delay)
                } else {
                    callback(new RetriesExceededError("Failed to get item in specified number of retries"))
                }
            } else {
                callback(err)
            }
        })
}

getOrRetry("airport_1254", (err, res) => {
    console.log(typeof err, err, res)
})

async function getOrRetryAwait(key, retries=2, delay=100, backoff_factor=1){
    try {
        return await collection.get(key)
    } catch (err) {
        if(err.code == 23) {
            if(retries){
                return await getOrRetryAwait(key, retries-1, delay*backoff_factor, backoff_factor)
            } else {
                throw new RetriesExceededError("Failed to get item in specified number of retries")
            }
        } else {
            throw err
        }
    }
}

// getOrRetryAwait("airport_1254")
//     .then( res => console.log(res))
//     .catch(err => logErr(err))

// getOrRetryAwait("airport_1254")
//     .then(console.log)
//     .catch(err => {
//         if (err instanceof RetriesExceededError){
//             collection.getFromReplica("airport_1254", 0)
//                 .then(console.log)
//         }
//     })

async function upsertAndCheck(key, value) {
    let oldCas
    try {
        oldCas = (await collection.get(key)).cas
    } catch (err) {
        if (err.code == 13){ // Key Not Found
            oldCas = false
        } else {
            throw err
        }
    }
    try {
        collection.upsert(key, value)
        return true
    } catch (err) {
        if (err.code == 23) { // Timeout, network or temporary
            try {
                let reGet = await getOrRetryAwait(key, 2, 2000, 1.5)
                return (oldCas === false) || (reGet.cas != oldCas)
            } catch (err) {
                if (err.code == 13) { // Key not found
                    return false
                }
            }
        } else {
            throw err
        }
    }
}

// upsertAndCheck('airport_1254', {abc: 123})
//     .then(console.log)
//     .catch(console.log)

const q = "SELECT airportname, city FROM `travel-sample` WHERE LOWER(airportname) LIKE $1"
const simple_q = "SELECT meta().id FROM `travel-sample` WHERE LOWER(airportname) LIKE $1"
async function N1QLFetchAirports(search, field='airportname'){
    let param = "%" + search.toLowerCase() + "%"
    try{
        let rows = (await collection.query(q, [param])).rows
        return rows
    } catch (err) {
        logErr(err)
        if(err.code != 0) throw err // anything other than N1QL index unavailable
        let IDRows = (await collection.query(simple_q, [param])).rows
        // Aaaaaand getMulti doesn't exist in sdk3. whoopsies.
    }
}

// N1QLFetchAirports('manc')
//     .then(console.log)
//     .catch(console.log)