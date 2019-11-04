const couchbase = require('couchbase')
const CBErr = couchbase.errors
const N1qlQuery = couchbase.N1qlQuery

const cluster = new couchbase.Cluster('couchbase://10.112.193.102')
cluster.authenticate('Administrator', 'password')
const bucket = cluster.openBucket('default')


class RetriesExceededError extends Error {
    constructor(message) {
        super(message);
        this.name = "RetriesExceededError"
    }
}

// All callbacks are of form callback(err, res) - as with couchbase callbacks

function getNormalOrReplica(key, callback){
    bucket.get(key, function(err, res){
        if(err.code == CBErr.timedOut || err.code == CBErr.networkError){
            bucket.getReplica(key, callback)
        } else {
            // Either we succeeded first time, or got a different error
            // In both instances we simply notify the callback
            callback(err, res)
        }
    })
}

function getOrRetry(key, callback, retries=2, delay=1000, backoff_factor=1){
    bucket.get(key, function(err, res){
        if(err && (err.code == CBErr.timedOut || err.code == CBErr.networkError || err.code == CBErr.temporaryError)){
            if(retries > 0){
                setTimeout(() => getOrRetry(key, callback, retries-1, delay*backoff_factor, backoff_factor), delay)
            } else {
                callback(new RetriesExceededError())
            }
        } else {
            callback(err, res)
        }
    })
}

function getRetryThenReplica(key, callback){
    getOrRetry(key, function(err, res){
        if(err instanceof RetriesExceededError){
            // Could manually log a failure here - notify an admin that replica had to be used
            getNormalOrReplica(key, callback)
        } else {
            callback(err, res)
        }
    }, 2, 1000, 2)
}

// This callback gets cb(err, res) where res is either true or false
// Signifying the upsert took place (true) or did not (false)
function upsertAndCheck(key, value, callback){
    // Make callback optional by wrapping in lambda function
    let _callback = (err, res) => typeof callback === 'function' && callback(err, res)
    bucket.get(key, function (err, res) {
        let oldCas
        if(err && err.code == CBErr.keyNotFound){
            oldCas = false
        } else if(!err) {
            oldCas = res.cas
        } else {
            // If we can't do a get without an error we probably shouldn't even attempt an upsert
            return callback(err)
        }
        bucket.upsert(key, value, function (err, res) {
            if(err && (err.code == CBErr.timedOut || err.code == CBErr.networkError || err.code == CBErr.temporaryError)){
                getOrRetry(key, function(err, res){
                    if(err && err.code == CBErr.keyNotFound){
                        callback(null, false)
                    } else if(err){
                        callback(err)
                    } else {
                        let success = !oldCas || res.cas != oldCas
                        callback(null, success)
                    }
                })
            } else if(err) {
                callback(err)
            } else {
                callback(null, true)
            }
        })
    })
}

// getRetryThenReplica('airport_1254', (err, res) => console.log(res.value))
// upsertAndCheck('123',{'abc':123},(err, res) => console.log(err, res))
const q = N1qlQuery.fromString("SELECT airportname, city FROM `travel-sample` WHERE LOWER(airportname) LIKE $1")
const simple_q = N1qlQuery.fromString("SELECT meta().id FROM `travel-sample` WHERE LOWER(airportname) LIKE $1")
function N1QLFetchAirports(search, callback, field='airportname') {
    let param = "%" + search.toLowerCase() + "%"
    let res = false
    bucket.query(q, [param], (err, rows) => {
        if(err){ // TODO: Actually identify the type of error here - 2 timeouts isn't ideal 
            bucket.query(simple_q, [param], (err, rows) => {
                if(err) console.log(err)
                let IDs = rows.map(x => x.id)
                bucket.getMulti(IDs, (num_errs, get_rows) => {
                    // Whittle down IDs to yet-to-be-got keys
                    IDs = IDs.filter(x => get_rows[x].error)
                    bucket.getMultiReplica(IDs, (num_errs, replica_rows) => {
                        console.log("Failed to get",num_errs,"documents")
                        get_rows = {...get_rows, ...replica_rows}
                        console.log(get_rows)
                        let results = Object.keys(get_rows).map(k => ({
                            airportname: get_rows[k].value.airportname,
                            city: get_rows[k].value.city
                        }))
                        callback(err, results)
                    })
                })
            })
        } else {
            callback(err, rows)
        }
    })
}

bucket.getMultiReplica = function(keys, options, callback) {
    if (options instanceof Function) {
        callback = arguments[1];
        options = {};
    }
  
    if (!Array.isArray(keys) || keys.length === 0) {
        throw new TypeError('First argument needs to be an array of length > 0.');
    }
    if (typeof options !== 'object') {
        throw new TypeError('Second argument needs to be an object or callback.');
    }
    if (typeof callback !== 'function') {
        throw new TypeError('Third argument needs to be a callback.');
    }
  
    if (!options.batch_size) {
        options.batch_size = keys.length;
    }
  
    var self = this;
    var outMap = {};
    var sentCount = 0;
    var resCount = 0;
    var errCount = 0;
  
    function getOne() {
        var key = keys[sentCount++];
        self.getReplica(key, function(err, res) {
            resCount++;
            if (err) {
                errCount++;
                outMap[key] = {
                error: err
                };
            } else {
                outMap[key] = res;
            }
            if (sentCount < keys.length) {
                getOne();
                return;
            } else if (resCount === keys.length) {
                callback(errCount, outMap);
                return;
            }
        });
    }
    for (var i = 0; i < options.batch_size; ++i) {
      getOne();
    }
}

// N1QLFetchAirports('spring', (err, rows) => {
//     console.log("---=== RESULT ===---")
//     console.log(err)
//     console.log(JSON.stringify(err))
//     console.log(rows)
//     //rows.forEach(row => console.log(row))
// })

// console.log(bucket.ping((a,b,c) => console.log(a,b,c)))

/*
In theory, durable writes returning ambiguous responses should be treated the same as a normal set being ambiguous
The difference is that durable writes will throw ambiguities where normal sets wouldn't - hence letting you react
    to ponential data loss. 
Where a normal set would have succeeded just by getting into memory, the node could then go down before it's replicated
But a durable write would throw a failure or ambiguous response in this case, allowing you to deal with this issue
*/

function durableFieldIncrement(key, field, amount, callback, retry=0){
    if (retry > 5) return callback(new RetriesExceededError())
    bucket.get(key, (err, oldRes) => {
        if(err) return callback(err)
        oldRes.value[field] += amount
        bucket.upsert(key, oldRes.value, {cas: oldRes.cas}, (err, res) => {
            if (err) {
                //console.log(err.code, err)
                // If CAS has changed or write failed then try again
                if (err.code == CBErr.keyAlreadyExists || err.code == CBErr.durabilityFailed)
                    return durableFieldIncrement(key, field, amount, callback, retry + 1)
                // If we get an ambiguous response then...
                if (err.code == CBErr.timedOut) { // TODO: check which error codes are ambiguous for durability
                    // After a short delay to let unfinished operations propegate...
                    setTimeout(() => {
                        // Retry the write with the same cas value (indicating no change took place)
                        bucket.upsert(key, oldRes.value, {cas: oldRes.cas}, callback)
                        /*  If another valid write operation took place on the document in this time,
                            we won't know if our changes propegated. But for a single client scenario
                            this is the simplest solution. E.g. if a user can only be logged in on one
                            client at a time */
                    }, 1000)
                }
            } else {
                // Durable write succeeded
                callback(err, res)
            }
        })
    })
}

bucket.upsert("SD_inc_balance",{
    "name": "John Smith",
    "balance": 5
}, () => {})
bucket.upsert("SD_inc_list",{
    "name": "John Smith",
    "transactions": [5],
    "total": []
}, () => {})
durableFieldIncrement("SD_inc_balance", "balance", 5, console.log)
durableFieldIncrement("SD_inc_balance", "balance", 4, console.log)
setTimeout(() => {
    durableFieldIncrement("SD_inc_balance", "balance", 3, console.log)
}, 1500)
