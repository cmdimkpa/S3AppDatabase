/*
s3appdb.js
About: Node.js implementation of the S3 App Database API
Author: Monty Dimkpa (cmdimkpa@gmail.com)
Version: 2.0 (January 2020)
*/

// Imports
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const AWS = require('aws-sdk');
const compression = require('compression');
const s3config = require("./s3config.json");
const md5 = require('md5');
const kue = require('kue');
const zlib = require('zlib');
const axios = require('axios');
var iostreamer_events = 0

// App Settings
const app = express();
const s3 = new AWS.S3(s3config);
app.options('*', cors())
const PORT = process.env.PORT || 3000;
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const queue = kue.createQueue({
    redis: REDIS_URL
});
queue.setMaxListeners(1000)

// Middleware

// bodyParser
app.use(bodyParser({ limit: '50mb' }));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

//CORS
app.use((req, res, next) => {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});

// compression
app.use(compression())

// catch general errors
app.use((error, req, res, next) => {
    if (error instanceof SyntaxError) {
        // Syntax Error
        res.status(400);
        res.json({ code: 400, message: "Malformed request. Check and try again." });
    } else {
        // Other error
        res.status(400);
        res.json({ code: 400, message: "There was a problem with your request." });
        next();
    }
});

// functions

let rBlockPct = () => {
    minBlockPct = 0.9
    maxBlockPct = 0.95
    return minBlockPct + Math.random()*(maxBlockPct - minBlockPct)
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

let now = () => {
    return Date();
}

let paginate = (array, pageSize, thisPage) => {
    if (pageSize && thisPage) {
        let pageStartIndex;
        if (thisPage.toString().includes("-")) {
            pageStartIndex = array.length - Math.abs(thisPage) * pageSize;
        } else {
            pageStartIndex = pageSize * (thisPage - 1);
        }
        return array.slice(pageStartIndex, pageStartIndex + pageSize);
    } else { return array }
}

let new_id = () => {
    return md5(now() + Math.random().toString());
}

let to_queue = async (job_type, job_data) => {
    try {
        let job = queue.create(`${job_type}_${new_id()}`, job_data).priority('high').attempts(5).save();
    } catch (err) { }
}

let set_bundle = async (prototype, BUNDLE) => {
    let package = JSON.stringify(BUNDLE)//await IOStreamerStringify(IOStreamer(), BUNDLE)
    s3.putObject({ Key: `${prototype}.S3AppBundle`, Body: zlib.gzipSync(package), Bucket: s3config.bucket, ServerSideEncryption: "AES256", StorageClass: "STANDARD", ContentEncoding: 'gzip' }, (err, data) => { });
}

let update_prototype = async (prototype, dataform = []) => {
    let required = ['__created_at__', '__updated_at__', '__private__', 'row_id', `${prototype}_id`];
    required.forEach((req_element) => {
        if (dataform.indexOf(req_element) === -1) {
            dataform.push(req_element);
        }
    });
    s3.getObject({ Key: `${prototype}.S3AppBundle`, Bucket: s3config.bucket }, async (err, data) => {
        if (data) {
            var BUNDLE = JSON.parse(zlib.unzipSync(data.Body).toString())//await IOStreamerParse(IOStreamer(), zlib.unzipSync(data.Body).toString());
        } else {
            var BUNDLE = [{ 'dataform': [], 'row_count': 0 }, {}, {}];
        }
        let REGISTER = BUNDLE[0];
        if (JSON.stringify(REGISTER) != '{}') {
            dataform.forEach((element) => {
                if (REGISTER.dataform.indexOf(element) === -1) {
                    REGISTER.dataform.push(element);
                }
            });
        } else {
            REGISTER = {
                'dataform': dataform,
                row_count: 0
            }
        }
        BUNDLE[0] = REGISTER;
        await set_bundle(prototype, BUNDLE);
    });
}

const datatype = (x) => {
    var type;
    // float test
    try {
        if (!parseFloat(x)) { type = 0 } else { type = 1 }
    } catch (err) { }
    // JSON test
    try {
        if (typeof JSON.parse(x) === 'object') { type = 2 }
    } catch (err) {
        if (typeof x === 'object') { type = 2 }
    }
    return type
}

const clip = (x) => {
    if (!is_neg(x)) {
        return x
    } else {
        return `${x}`.substring(0, x.length - 4)
    }
}

const is_range_match = (element, vector, strict) => {
    let neg = is_neg(vector[0]); element = clip(element)
    vector = vector.map(x => { return clip(x) })
    if (!strict) {
        var a, b;
        if (vector[0] >= vector[1]) { a = vector[0]; b = vector[1] } else { b = vector[0]; a = vector[1] }
        if (element >= b && element <= a) { return neg ? false : true } else { return neg ? true : false }
    } else {
        let bool = Boolean(element === vector[0] || element === vector[1])
        return neg ? !bool : bool
    }
}

let is_partial_match = (string, options, strict) => {
    let neg = is_neg(options[0]); string = clip(string)
    options = options.map(x => { return clip(`${x}`) })
    if (!strict) {
        string = string.toLowerCase()
        let is_date = (string) => {
            var delim;
            if (string.includes('/')) {
                delim = '/';
            } else {
                if (string.includes('-')) {
                    delim = '-';
                } else {
                    return false;
                }
            }
            if (Boolean(string.split(delim).length == 3) && Boolean(string.length >= 10)) {
                let comps = string.slice(0, 10).split(delim);
                try {
                    let types = comps.map((e) => { return parseInt(e) });
                    if (types) {
                        return true;
                    } else {
                        return false;
                    }
                } catch (err) {
                    return false;
                }
            } else {
                return false;
            }
        }
        if (!is_date(string)) {
            try {
                let state = false;
                for (let j = 0; j < options.length; j++) {
                    let option = options[j];
                    if (string.includes(option) || option.includes(string)) {
                        state = true;
                        break
                    }
                }
                return neg ? !state : state;
            } catch (err) {
                return is_range_match(string, options, strict);
            }
        } else {
            return is_range_match(string, options, strict);
        }
    } else {
        let state = false;
        for (let j = 0; j < options.length; j++) {
            let option = options[j];
            if (string === option) {
                state = true;
                break
            }
        }
        return neg ? !state : state;
    }
}

let format_param = (param, strict, neg) => {
    let normalize = (i) => {
        var k;
        if (datatype(i) == 0) { k = !strict ? i.toLowerCase() : i } else { k = `${i}` }
        // check negation
        if (neg) {
            return `${k}_NOT`
        } else {
            return k
        }
    }
    try {
        if (param || param === 0) {
            if (datatype(param) == 2) {
                return param.map((e) => { return normalize(e) })
            } else {
                let j = normalize(param)
                return [j, j]
            }
        } else { return ['', ''] }
    } catch (err) {
        return ['', '']
    }
}

const array_count = (array, element) => {
    found = 0
    for (let i = 0; i < array.length; i++) {
        if (array[i] === element) { found++ }
    }
    return found
}

const apply_restrict = (obj, restrict) => {
    if (!restrict) {
        return obj
    } else {
        restricted = {}
        Object.keys(obj).forEach(key => {
            if (restrict.indexOf(key) !== -1) {
                restricted[key] = obj[key]
            }
        })
        return restricted
    }
}

const is_neg = (x) => {
    return `${x}`.substring(x.length - 4, x.length) === '_NOT'
}

const determine_matches = async (context, data, constraints, strict, operator, context_data = {}, page_size = null, this_page = null) => {
    try {
        let BUNDLE = JSON.parse(zlib.unzipSync(data.Body).toString())//await IOStreamerParse(IOStreamer(), zlib.unzipSync(data.Body).toString());
        let REGISTER = BUNDLE[0];
        let TABLE = BUNDLE[1];
        let INDEX = BUNDLE[2];
        let results = [];
        if (constraints == '*' || JSON.stringify(constraints) == '{}') {
            Object.keys(TABLE).forEach((row_id) => {
                if (TABLE[row_id]) {
                    if (TABLE[row_id]['__private__'] == 0) {
                        results.push(TABLE[row_id]);
                    }
                }
            });
        } else {
            let use_constraints = {}
            Object.keys(constraints).forEach((key) => {
                try {
                    use_constraints[clip(key)] = format_param(constraints[key], strict, is_neg(key));
                } catch (err) { }
            });
            let dataform = REGISTER.dataform;
            let common_fields = [];
            let candidates = []
            Object.keys(use_constraints).forEach((field) => {
                if (Boolean(dataform.indexOf(field) != -1) && Boolean(Object.keys(INDEX).indexOf(field) != -1)) {
                    common_fields.push(field);
                }
            });
            common_fields.forEach((field) => {
                let keys = Object.keys(INDEX[field]);
                let boundary = use_constraints[field];
                keys.forEach((key) => {
                    if (Boolean(datatype(key) == 0 && is_partial_match(key, boundary, strict)) || Boolean(datatype(key) == 1 && is_range_match(key, boundary, strict))) {
                        INDEX[field][key].forEach((e) => {
                            candidates.push(e);
                        });
                    }
                });
            });
            if (operator === 'AND') {
                matches = []
                candidates.forEach(candidate => {
                    if (array_count(candidates, candidate) === common_fields.length) {
                        matches.push(candidate)
                    }
                })
            } else {
                matches = [...new Set(candidates)]
            }
            try {
                matches = paginate(matches, page_size, this_page);
                matches = [...new Set(matches)]
                matches.forEach((row_id) => {
                    if (TABLE[row_id]) {
                        if (TABLE[row_id]['__private__'] == 0) {
                            results.push(TABLE[row_id]);
                        }
                    }
                });
            } catch (err) { }
        }
        if (context !== 0) { await to_queue('update_rows', { 'INDEX': INDEX, 'TABLE': TABLE, 'REGISTER': REGISTER, 'row_ids': results.map(x => { return x.row_id }), 'tablename': context_data.tablename, 'use_data': context_data.use_data }); }
        return results
    } catch (err) {
        return []
    }
}

//routes

app.get('/ods/get_register/:prototype', async (req, res) => {
    let prototype = req.params.prototype;
    s3.getObject({ Key: `${prototype}.S3AppBundle`, Bucket: s3config.bucket }, async (err, data) => {
        if (data) {
            let BUNDLE = JSON.parse(zlib.unzipSync(data.Body).toString())//await IOStreamerParse(IOStreamer(), zlib.unzipSync(data.Body).toString());
            res.status(200)
            res.json({
                code: 200,
                message: 'Success',
                data: BUNDLE[0]
            });
        } else {
            res.json({
                code: 200,
                message: 'Error: No data',
                data: {}
            });
        }
    });
});

app.post('/ods/new_table', async (req, res) => {
    let tablename = req.body.tablename;
    let fields = req.body.fields;
    if (!tablename || !fields) {
        res.status(400)
        res.json({
            code: 400,
            message: 'Error: One or more required fields missing (check: `tablename`, `fields`)',
            data: {}
        })
    };
    await update_prototype(tablename, fields);
    res.status(201)
    res.json({
        code: 201,
        message: `Success: Prototype updated for object: ${tablename.toUpperCase()}`,
        data: {}
    })
});

app.post('/ods/new_record', async (req, res) => {
    let tablename = req.body.tablename;
    let data = req.body.data;
    if (!tablename || !data) {
        res.status(400)
        res.json({
            code: 400,
            message: 'Error: One or more required fields missing (check: `tablename`, `data`)',
            data: {}
        })
    };
    // force prototype key
    data[`${tablename}_id`] = new_id();
    await to_queue('new_record', { 'tablename': tablename, 'data': data });
    res.status(201)
    res.json({
        code: 201,
        message: `Success: Record added to object: ${tablename.toUpperCase()}`,
        'data': data
    })
});

app.post('/ods/fetch_records', async (req, res) => {
    let tablename = req.body.tablename;
    let constraints = req.body.constraints;
    let page_size = req.body.page_size;
    let this_page = req.body.this_page;
    let strict = req.body.strict;
    let restrict = req.body.restrict;
    let operator = req.body.operator;
    if (!operator) { operator = 'AND' } else { operator = operator.toUpperCase() }
    if (!tablename || !constraints) {
        res.status(400)
        res.json({
            code: 400,
            message: 'Error: One or more required fields missing (check: `tablename`, `constraints`)',
            data: {}
        })
    };
    try {
        s3.getObject({ Key: `${tablename}.S3AppBundle`, Bucket: s3config.bucket }, async (err, data) => {
            if (data) {
                let results = await determine_matches(0, data, constraints, strict, operator, page_size, this_page)
                res.status(200)
                res.json({
                    code: 200,
                    message: `Success: ${results.length} records matched in object: ${tablename.toUpperCase()}`,
                    data: results.map(k => { return apply_restrict(k, restrict) })
                });
            } else {
                res.json({
                    code: 200,
                    message: `Error: no data`,
                    data: []
                });
            }
        });
    } catch (err) {
        res.json({
            code: 200,
            message: 'Error: no data',
            data: []
        })
    }
});

app.post('/ods/update_records', async (req, res) => {
    let tablename = req.body.tablename;
    let constraints = req.body.constraints;
    let use_data = req.body.data;
    let strict = req.body.strict;
    let operator = req.body.operator;
    if (!operator) { operator = 'AND' } else { operator = operator.toUpperCase() }
    if (!tablename || !constraints || !use_data) {
        res.status(400)
        res.json({
            code: 400,
            message: 'Error: One or more required fields missing (check: `tablename`, `constraints`, `data`)',
            data: {}
        })
    };
    s3.getObject({ Key: `${tablename}.S3AppBundle`, Bucket: s3config.bucket }, async (err, data) => {
        if (data) {
            let row_ids = await determine_matches(1, data, constraints, strict, operator, { 'tablename': tablename, 'use_data': use_data })
            res.json({
                code: 200,
                message: `Success: ${row_ids.length} records updated in object: ${tablename.toUpperCase()}`,
                data: {}
            });
        } else {
            res.json({
                code: 200,
                message: `Error: No data`,
                data: {}
            });
        }
    });
});

app.post('/ods/delete_records', async (req, res) => {
    let tablename = req.body.tablename;
    let constraints = req.body.constraints;
    let strict = req.body.strict;
    let operator = req.body.operator;
    if (!operator) { operator = 'AND' } else { operator = operator.toUpperCase() }
    if (!tablename || !constraints) {
        res.status(400)
        res.json({
            code: 400,
            message: 'Error: One or more required fields missing (check: `tablename`, `constraints`)',
            data: {}
        })
    };
    s3.getObject({ Key: `${tablename}.S3AppBundle`, Bucket: s3config.bucket }, async (err, data) => {
        if (data) {
            let row_ids = JSON.parse(zlib.unzipSync(data.Body).toString())//await determine_matches(1, data, constraints, strict, operator, { 'tablename': tablename, 'use_data': { '__private__': 1 } })
            res.json({
                code: 200,
                message: `Success: ${row_ids.length} records deleted in object: ${tablename.toUpperCase()}`,
                data: {}
            });
        } else {
            res.json({
                code: 200,
                message: `Error: No data`,
                data: {}
            });
        }
    });
});

app.post('/ods/get_rows', async (req, res) => {
    let tablename = req.body.tablename;
    let row_ids = req.body.row_ids;
    let restrict = req.body.restrict;
    if (!tablename || !row_ids) {
        res.status(400)
        res.json({
            code: 400,
            message: 'Error: One or more required fields missing (check: `tablename`, `row_ids`)',
            data: {}
        })
    };
    try {
        s3.getObject({ Key: `${tablename}.S3AppBundle`, Bucket: s3config.bucket }, async (err, data) => {
            if (data) {
                let BUNDLE = JSON.parse(zlib.unzipSync(data.Body).toString())//await IOStreamerParse(IOStreamer(), zlib.unzipSync(data.Body).toString());
                let TABLE = BUNDLE[1];
                let results = [];
                row_ids.forEach((row_id) => {
                    try {
                        let candidate = TABLE[row_id];
                        if (candidate['__private__'] == 0) {
                            results.push(candidate);
                        }
                    } catch (err) { }
                });
                res.status(200)
                res.json({
                    code: 200,
                    message: `Success: ${results.length} records fetched from object: ${tablename.toUpperCase()}`,
                    data: results.map(k => { return apply_restrict(k, restrict) })
                })
            } else {
                res.json({
                    code: 200,
                    message: 'Error: no data',
                    data: {}
                })
            }
        });
    } catch (err) {
        res.json({
            code: 200,
            message: 'Error: no data',
            data: {}
        })
    }
});

app.get('/ods/flush_table/:prototype', async (req, res) => {
    try {
        let tablename = req.params.prototype;
        s3.getObject({ Key: `${tablename}.S3AppBundle`, Bucket: s3config.bucket }, async (err, data) => {
            if (data) {
                let BUNDLE = JSON.parse(zlib.unzipSync(data.Body).toString())//await IOStreamerParse(IOStreamer(), zlib.unzipSync(data.Body).toString());
                try {
                    BUNDLE[0].row_count = 0;
                    BUNDLE[1] = {};
                    BUNDLE[2] = {};
                    await set_bundle(tablename, BUNDLE);
                } catch (err) { }
                res.json({
                    code: 200,
                    message: `Success: Table [${tablename.toUpperCase()}] was flushed`,
                    data: {}
                })
            }
        });
    } catch (err) {
        res.json({
            code: 200,
            message: 'Error: no data',
            data: {}
        })
    }
});

app.listen(PORT, () => {
    console.log(`@s3appdb router: now running on port ${PORT}`);
});
