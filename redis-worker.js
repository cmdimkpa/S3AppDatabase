/*
redis-worker.js
About: Job Queue Manager for the S3 App Database API
Author: Monty Dimkpa (cmdimkpa@gmail.com)
Version: 2.0 (January 2020)
*/

// imports
const express = require('express');
const bodyParser = require('body-parser');
const AWS = require('aws-sdk');
const s3config = require("./s3config.json");
const s3 = new AWS.S3(s3config);
const kue = require('kue');
const zlib = require('zlib');
const axios = require('axios');
const redis = require('redis');

var iostreamer_events = 0;
var processed_jobs = [];

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const redis_client = redis.createClient(REDIS_URL);
const queue = kue.createQueue({
    redis: REDIS_URL
});
queue.setMaxListeners(1000)

// App Settings
const app = express();
app.use(bodyParser({ limit: '50mb' }));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
const PORT = process.env.PORT || 3066;

// worker functions

let rBlockPct = () => {
    minBlockPct = 0.9
    maxBlockPct = 0.95
    return minBlockPct + Math.random() * (maxBlockPct - minBlockPct)
}

let IOStreamerParseDo = async (iostreamer, session_key, chunk) => {
    return await axios.post(`${iostreamer}/iostreamer/parse/do`, {
        "session_key": session_key,
        "chunk": chunk
    }).then(response => {
        return response.data.code
    }).catch(error => { return "error" })
}

let IOStreamerStringifyDo = async (iostreamer, session_key, object_type, chunk) => {
    return await axios.post(`${iostreamer}/iostreamer/stringify/do`, {
        "session_key": session_key,
        "object_type": object_type,
        "chunk": chunk
    }).then(response => {
        return response.data.code
    }).catch(error => { return "error" })
}

let IOStreamerParseGet = async (iostreamer, session_key, object_type) => {
    return await axios.get(`${iostreamer}/iostreamer/parse/get?session_key=${session_key}&object_type=${object_type}`)
        .then(response => {
            return [response.data.code, response.data.data.chunk]
        }).catch(error => { return ["error", "error"] })
}

let IOStreamerStringifyGet = async (iostreamer, session_key) => {
    return await axios.get(`${iostreamer}/iostreamer/stringify/get?session_key=${session_key}`)
        .then(response => {
            return [response.data.code, response.data.data.chunk]
        }).catch(error => { return ["error", "error"] })
}

let IOStreamerStringifyCreateSession = async (iostreamer) => {
    return await axios.get(`${iostreamer}/iostreamer/stringify/createSession`)
        .then(response => {
            return response.data.data.session_key
        }).catch(error => { return "error" })
}

let IOStreamerParseCreateSession = async (iostreamer) => {
    return await axios.get(`${iostreamer}/iostreamer/parse/createSession`)
        .then(response => {
            return response.data.data.session_key
        }).catch(error => { return "error" })
}

let IOStreamerStringifyStart = async (iostreamer, session_key) => {
    return await axios.get(`${iostreamer}/iostreamer/stringify/initiate?session_key=${session_key}`)
        .then(response => {
            return [response.data.code, response.data.data.size]
        }).catch(error => { return ["error", "error"] })
}

let IOStreamerStringifyCleanup = async (iostreamer, session_key) => {
    return await axios.get(`${iostreamer}/iostreamer/stringify/cleanup?session_key=${session_key}`)
        .then(response => {
            return response.data.code
        }).catch(error => { return "error" })
}

let IOStreamerParseStart = async (iostreamer, session_key) => {
    return await axios.get(`${iostreamer}/iostreamer/parse/initiate?session_key=${session_key}`)
        .then(response => {
            return [response.data.code, response.data.data.size]
        }).catch(error => { return ["error", "error"] })
}

let IOStreamerParseCleanup = async (iostreamer, session_key) => {
    return await axios.get(`${iostreamer}/iostreamer/parse/cleanup?session_key=${session_key}`)
        .then(response => {
            return response.data.code
        }).catch(error => { return "error" })
}

let IOStreamerStringify = async (iostreamer, bundle) => {
    // step 1: split bundle
    register = bundle[0]
    table = bundle[1]
    index = bundle[2]
    // allocate <blockSize> units per chunk
    let blockPct = rBlockPct()
    // ... create table chunks
    let table_chunks = []
    let table_keys = Object.keys(table)
    let tableSize = table_keys.length
    let tableBlockSize = 1000//Math.floor(blockPct * tableSize)
    var whole = Math.floor(tableSize / tableBlockSize)
    var rem = tableSize % tableBlockSize
    var start; var stop
    for (let block = 0; block < whole; block++) {
        start = block * tableBlockSize
        stop = (block + 1) * tableBlockSize
        keys = table_keys.slice(start, stop)
        chunk = {}
        keys.forEach(key => {
            chunk[key] = table[key]
        })
        table_chunks.push(chunk)
    }
    if (rem > 0 && whole > 0) {
        keys = table_keys.slice(stop, stop + rem)
        chunk = {}
        keys.forEach(key => {
            chunk[key] = table[key]
        })
        table_chunks.push(chunk)
    }
    if (Boolean(rem > 0 && whole === 0) || Boolean(rem === 0 && whole === 0)) {
        table_chunks.push(table)
    }
    // ... create index chunks
    let index_chunks = []
    let index_keys = Object.keys(index)
    let indexSize = index_keys.length
    let indexBlockSize = 1000//Math.floor(blockPct * indexSize)
    whole = Math.floor(indexSize / indexBlockSize)
    rem = indexSize % indexBlockSize
    var start; var stop
    for (let block = 0; block < whole; block++) {
        start = block * indexBlockSize
        stop = (block + 1) * indexBlockSize
        keys = index_keys.slice(start, stop)
        chunk = {}
        keys.forEach(key => {
            chunk[key] = index[key]
        })
        index_chunks.push(chunk)
    }
    if (rem > 0 && whole > 0) {
        keys = index_keys.slice(stop, stop + rem)
        chunk = {}
        keys.forEach(key => {
            chunk[key] = index[key]
        })
        index_chunks.push(chunk)
    }
    if (Boolean(rem > 0 && whole === 0) || Boolean(rem === 0 && whole === 0)) {
        index_chunks.push(index)
    }
    // step 2: send objects
    // ... get session key
    var session_key = "error"
    while (session_key === "error") {
        session_key = await IOStreamerStringifyCreateSession(iostreamer)
    }
    // ... send register
    var session_response;
    session_response = "ready"
    while (session_response !== 200) {
        session_response = await IOStreamerStringifyDo(iostreamer, session_key, "register", register)
    }
    // ... send table chunks
    for (let i = 0; i < table_chunks.length; i++) {
        let chunk = table_chunks[i]
        session_response = "ready"
        while (session_response != 200) {
            session_response = await IOStreamerStringifyDo(iostreamer, session_key, "table", chunk)
        }
    }
    // ... send index chunks
    for (let i = 0; i < index_chunks.length; i++) {
        let chunk = index_chunks[i]
        session_response = "ready"
        while (session_response != 200) {
            session_response = await IOStreamerStringifyDo(iostreamer, session_key, "index", chunk)
        }
    }
    // initiate incoming string chunks
    session_response = ["error", "error"]
    let canProceed = false;
    let attempts = 0;
    while (session_response[0] !== 200 && attempts < 1000) {
        session_response = await IOStreamerStringifyStart(iostreamer, session_key)
        if (session_response[0] === 200) { canProceed = true  } else { attempts++ }
    }

    if (canProceed){
        // step 3: receive bundle string
        var bundle_string = ""
        let size = session_response[1]
        for (let i = 0; i < size; i++) {
            session_response = ["error", "error"]
            while (session_response[0] != 200) {
                session_response = await IOStreamerStringifyGet(iostreamer, session_key)
                chunk = session_response[1]
                if (chunk !== "error") {
                    bundle_string += chunk
                }
            }
        }
        // cleanup
        session_response = "error"
        retries = 0
        while (session_response !== 200 && retries < 1000) {
            session_response = await IOStreamerStringifyCleanup(iostreamer, session_key)
            retries++
        }
        // return bundle string
        return bundle_string
    } else {
        return ""
    }
}

let IOStreamerParse = async (iostreamer, str) => {
    // step 1: create <blockSize> chunks
    let text = str || ""
    let chunks = []
    let blockPct = rBlockPct()
    let blockSize = 1000000//Math.floor(blockPct * text.length)
    var whole = Math.floor(text.length / blockSize)
    var rem = text.length % blockSize
    var start; var stop
    for (let block = 0; block < whole; block++) {
        start = block * blockSize
        stop = (block + 1) * blockSize
        chunks.push(text.substring(start, stop))
    }
    if (rem > 0 && whole > 0) {
        chunks.push(text.substring(stop, stop + rem))
    }
    if (Boolean(rem > 0 && whole === 0) || Boolean(rem === 0 && whole === 0)) {
        chunks.push(text)
    }
    // step 2: send chunks
    // ... get session key
    var session_key = "error"
    while (session_key === "error") {
        session_key = await IOStreamerParseCreateSession(iostreamer)
    }
    // send chunks
    var session_response;
    for (let i = 0; i < chunks.length; i++) {
        let chunk = chunks[i]
        session_response = "ready"
        while (session_response !== 200) {
            session_response = await IOStreamerParseDo(iostreamer, session_key, chunk)
        }
    }
    // step 3: get and assemble objects

    let table = {};
    let index = {};
    var register = {};
    var chunk;

    // start incoming data chunks
    session_response = ["error", "error"]
    let canProceed = false;
    let attempts = 0;
    while (session_response[0] !== 200 && attempts < 1000) {
        session_response = await IOStreamerParseStart(iostreamer, session_key)
        if (session_response[0] === 200) { canProceed = true  } else { attempts++ }
    }
    if (canProceed){
        let size = session_response[1]
        //  ... get register
        session_response = ["error", "error"]
        while (session_response[0] !== 200) {
            session_response = await IOStreamerParseGet(iostreamer, session_key, "register")
        }
        register = session_response[1]
        //  ... populate table
        for (let i = 0; i < size.table; i++) {
            session_response = ["error", "error"]
            while (session_response[0] != 200) {
                session_response = await IOStreamerParseGet(iostreamer, session_key, "table")
                chunk = session_response[1]
                if (chunk !== "error") {
                    let keys = Object.keys(chunk)
                    keys.forEach(key => {
                        table[key] = chunk[key]
                    })
                }
            }
        }
        //  ... populate index
        for (let i = 0; i < size.index; i++) {
            session_response = ["error", "error"]
            while (session_response[0] != 200) {
                session_response = await IOStreamerParseGet(iostreamer, session_key, "index")
                chunk = session_response[1]
                if (chunk !== "error") {
                    let keys = Object.keys(chunk)
                    keys.forEach(key => {
                        index[key] = chunk[key]
                    })
                }
            }
        }
        // cleanup
        session_response = "error"
        retries = 0
        while (session_response !== 200 && retries < 1000) {
            session_response = await IOStreamerParseCleanup(iostreamer, session_key)
            retries++
        }
        // return bundle
        return [register, table, index]
    } else {
        return [register, table, index]
    }
}

let IOStreamer = () => {
    if (iostreamer_events > 10000){ iostreamer_events = 0 } else { iostreamer_events++ }
    return s3config.iostreamers[iostreamer_events % s3config.iostreamers.length]
}

let timestamp = () => {
    return parseInt(Date.now() / 1000);
}

const datatype = (x) => {
    let t = typeof (x)
    return t.includes('string') ? 0 : t.includes('number') ? 1 : 2
}

const delete_job = (job_id) => {
  // delete job
  redis_client.del(job_id, function (err, reply) {
     if (err) {
       console.log(`error deleting job: ${job_id}`);
     } else {
       console.log(`successfully deleted job: ${job_id}`);
     }
  });
}

const delete_related = (job_key) => {
  redis_client.keys('*', function (err, keys) {
    if (err) return console.log(err);
    keys.forEach(key => {
      if (key.indexOf(job_key) !== -1){
        delete_job(key);
      }
    });
  });
}

const delete_scraps = () => {
  redis_client.keys('*', function (err, keys) {
    if (err) return console.log(err);
    keys.forEach(key => {
      delete_job(key);
    });
  });
}

const manage_job = (job_id) => {
  // delete any scraps
  if (processed_jobs.length === 0){
    delete_scraps();
  }
  // update processed jobs
  processed_jobs.push(job_id);
  console.log(`successfully processed job: ${job_id}`);
  // deleted processed jobs
  for (var i=0;i<processed_jobs.length;i++){
    let job_key = processed_jobs.pop();
    delete_job(job_key);
    delete_related(job_key);
  }
}

const set_bundle = async (BUNDLE, prototype, job_id) => {
    let package = JSON.stringify(BUNDLE)//await IOStreamerStringify(IOStreamer(), BUNDLE)
    s3.putObject({ Key: `${prototype}.S3AppBundle`, Body: zlib.gzipSync(package), Bucket: s3config.bucket, ServerSideEncryption: "AES256", StorageClass: "STANDARD", ContentEncoding: 'gzip' }, (err, data) => {
        // job processed, move on
        manage_job(job_id);
    });
}

// process controller
let process_jobs = async () => {
    queue.inactive((err, ids) => {
        let use_keys = ids;
        let job_counter = -1;
        let next = () => {
            if (job_counter + 1 < use_keys.length) {
                job_counter++;
                kue.Job.get(use_keys[job_counter], (err, job) => {
                    if (job !== undefined) {
                        let job_id = job.type;
                        let job_data = job.data;
                        let job_type = job_id.split("_").slice(0, 2).join("_");
                        if (job_type == 'new_record') {
                            let prototype = job_data.tablename;
                            let new_data = job_data.data;
                            s3.getObject({ Key: `${prototype}.S3AppBundle`, Bucket: s3config.bucket }, async (err, data) => {
                                if (data) {
                                    var BUNDLE = JSON.parse(zlib.unzipSync(data.Body).toString())//await IOStreamerParse(IOStreamer(), zlib.unzipSync(data.Body).toString());
                                } else {
                                    var BUNDLE = [{ 'dataform': [], 'row_count': 0 }, {}, {}];
                                }
                                if (new_data) {
                                    let dataform = BUNDLE[0].dataform;
                                    let row_count = BUNDLE[0].row_count;
                                    row_count++;
                                    new_data['__created_at__'] = timestamp();
                                    new_data['__updated_at__'] = null;
                                    new_data['__private__'] = 0;
                                    if (Object.keys(new_data).indexOf('row_id') == -1) {
                                        new_data['row_id'] = row_count;
                                    }
                                    let common_fields = [];
                                    Object.keys(new_data).forEach((field) => {
                                        if (dataform.indexOf(field) != -1) {
                                            common_fields.push(field);
                                        }
                                    });
                                    BUNDLE[1][row_count] = {}
                                    common_fields.forEach((field) => {
                                        let value = new_data[field];
                                        BUNDLE[1][row_count][field] = value;
                                        if (datatype(value) == 2) {
                                            value = JSON.stringify(value);
                                        }
                                        if (Object.keys(BUNDLE[2]).indexOf(field) != -1 || Object.keys(BUNDLE[2]).indexOf(field.toString()) != -1) {
                                            if (Object.keys(BUNDLE[2][field]).indexOf(value) != -1 || Object.keys(BUNDLE[2][field]).indexOf(value.toString()) != -1) {
                                                BUNDLE[2][field][value].push(row_count);
                                            } else {
                                                BUNDLE[2][field][value] = [row_count];
                                            }
                                        } else {
                                            BUNDLE[2][field] = {}
                                            BUNDLE[2][field][value] = [row_count]
                                        }
                                    });
                                    BUNDLE[0].row_count = row_count;
                                }
                                let package = JSON.stringify(BUNDLE)//await IOStreamerStringify(IOStreamer(), BUNDLE)
                                s3.putObject({ Key: `${prototype}.S3AppBundle`, Body: zlib.gzipSync(package), Bucket: s3config.bucket, ServerSideEncryption: "AES256", StorageClass: "STANDARD", ContentEncoding: 'gzip' }, (err, data) => {
                                    // job processed, move on
                                    manage_job(job_id); job.complete(); next();
                                });
                            });
                        }
                        if (job_type == "update_rows") {
                            let INDEX = job_data.INDEX;
                            let TABLE = job_data.TABLE;
                            let REGISTER = job_data.REGISTER;
                            let row_ids = job_data.row_ids;
                            let prototype = job_data.tablename;
                            let value_dict = job_data.use_data;
                            let update_logical_row = (row_id, value_dict) => {
                                try {
                                    let dataform = REGISTER.dataform;
                                    let common_fields = [];
                                    Object.keys(value_dict).forEach((field) => {
                                        if (dataform.indexOf(field) != -1) {
                                            common_fields.push(field);
                                        }
                                    });
                                    common_fields.forEach((field) => {
                                        TABLE[row_id][field] = value_dict[field];
                                        try {
                                            for (let i = 0; i < Object.keys(INDEX[field]).length; i++) {
                                                let value = Object.keys(INDEX[field])[i];
                                                if (INDEX[field][value].indexOf(row_id) != -1) {
                                                    let lookup = INDEX[field][value]
                                                    INDEX[field][value].splice(lookup.indexOf(row_id), 1);
                                                }
                                            }
                                        } catch (err) { }
                                        let new_value = value_dict[field];
                                        if (datatype(new_value) === 2) {
                                            new_value = JSON.stringify(new_value);
                                        }
                                        if (Object.keys(INDEX).indexOf(field) != -1 || Object.keys(INDEX).indexOf(field.toString()) != -1) {
                                            if (Object.keys(INDEX[field]).indexOf(new_value) != -1 || Object.keys(INDEX[field]).indexOf(new_value.toString()) != -1) {
                                                INDEX[field][new_value].push(row_id);
                                            } else {
                                                INDEX[field][new_value] = [row_id];
                                            }
                                        } else {
                                            INDEX[field] = {};
                                            INDEX[field][new_value] = [row_id]
                                        }
                                    });
                                    TABLE[row_id]['__updated_at__'] = timestamp();
                                } catch (err) { console.log(`error: ${err}`) }
                            }
                            if (row_ids && value_dict) { row_ids.forEach((row_id) => { update_logical_row(row_id, value_dict) }); }
                            let BUNDLE = [REGISTER, TABLE, INDEX];
                            job.complete();
                            set_bundle(BUNDLE, prototype, job_id).then(response => {
                                next();
                            })
                        }
                    } else { console.log("undefined job"); next() }
                });
            } else {
                // restart process
                process_jobs()
            }
        }
        next()
    });
}

app.listen(PORT, () => {
    console.log(`@s3appdb redis-worker: now running on port ${PORT}`);
    process_jobs();
});
