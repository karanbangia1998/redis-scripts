#!/usr/bin/env node
const StreamArray = require('stream-json/streamers/StreamArray');
const fs = require('fs');
const mysql = require('mysql2');

const _ = require('lodash');


const {argv} = require('yargs')
    .default('h', 'dummyHost')
    .default('p', 3306)
    .default('user', 'dummu-user')
    .default('password', 'dummy-passworsd')
    .default('filename', 'dummyFile.json')
    .default('database', 'db');

const host = argv.h;
const port = argv.p;
const {user} = argv;
const {password} = argv;
const {filename} = argv;
const {database} = argv;
let sep = '';
let missedEntries = 0;

let pool = mysql.createPool({
    host: host, port: port, user: user, password: password, database: database
})
let missedInserts = "missedInserts.json";

if (fs.existsSync(missedInserts)) {
    fs.unlinkSync(missedInserts);
}
const fd = fs.openSync(missedInserts, 'a');
fs.appendFileSync(fd, '[', 'utf8');

function main() {
    const startTime = new Date();
    let keyCount = 0;
    const jsonStream = StreamArray.withParser();
    fs.createReadStream(filename).pipe(jsonStream.input);
    jsonStream.on('data', ({key, value}) => {
        console.log("\nIteration no:", key)
        let jsonString = JSON.stringify(value)
        jsonString = jsonString.replace(/lsagoraplatformId:/g, '');
        let jsonObject = JSON.parse(jsonString)
        let rows = []
        Object.keys(jsonObject).forEach(function (k) {
            rows.push([Number(k), Number(jsonObject[k])])
        });
        console.log(`Total Entries in key ${key}:`, rows.length)
        keyCount += rows.length
        console.log(`Number of keyValues found: ${keyCount}`);
        //insert in mysql
        jsonStream.pause()
        insertIntoMySQL(rows).then(() => {
                jsonStream.resume()
            }
        )


    });

    jsonStream.on('end', () => {
        console.log('\n*********** SCAN FINISHED ***********');
        console.log('All Done');
        fs.appendFileSync(fd, ']', 'utf8');
        if (fd !== undefined) {
            fs.closeSync(fd);
        }
        // Stop timer
        const executionTimeMs = new Date() - startTime;
        const executionTimeStr = millisecondsToStr(executionTimeMs);
        console.info(`Total Missed Inserts: ${missedEntries}`);
        console.info(`Execution time: ${executionTimeStr}`);
        process.exit();
    });
}

main();

async function main2() {
    const limit = 50000;
    let i = 1;
    while (true) {
        let offset = limit * (i - 1)
        let sql = `SELECT test_id FROM test_table WHERE test_id NOT IN (SELECT agora_id FROM livestream_agora_mapping_table WHERE agora_id BETWEEN 48000000 AND 51000000) LIMIT ${limit} OFFSET ${offset}`;
        const promisePool = pool.promise();
        const [rows] = await promisePool.query(sql)
        const keyValues = {};
        if (rows.length === 0) {
            fs.appendFileSync(fd, ']', 'utf8');
            if (fd !== undefined) {
                fs.closeSync(fd);
            }
            process.exit()
        }
        for (let j = 0; j < rows.length; j++) {
            keyValues[rows[j].test_id] = rows[j].test_id
        }
        fs.appendFileSync(fd, sep + JSON.stringify(keyValues), 'utf8');
        sep = ',';
        i++;
        console.log(`iteration: ${i} completed`)
    }
}

async function asyncInserts(rows) {
    let sql = "INSERT INTO livestream_agora_mapping_table (agora_id,user_id) VALUES ? ON DUPLICATE KEY UPDATE user_id = VALUES(user_id)";
    try {
        const promisePool = pool.promise();
        await promisePool.query(sql, [rows])
        console.log(`inserted ${rows.length} rows in db`)
    } catch (e) {
        console.log(e.code)
        const keyValues = {};
        for (let i = 0; i < rows.length; i++) {
            keyValues[`lsagoraplatformId:${rows[i][0]}`] = rows[i][1]
        }
        console.log('Write key-values to file');
        missedEntries += rows.length
        console.log(`missed inserts ${rows.length} rows in db`)
        fs.appendFileSync(fd, sep + JSON.stringify(keyValues), 'utf8');
        sep = ',';
    }
}

async function insertIntoMySQL(rows) {
    let chunks = _.chunk(rows, 1000)
    for (const chunk of chunks) {
       await asyncInserts(chunk)
    }
}

function millisecondsToStr(milliseconds) {
    function numberEnding(number) {
        return (number > 1) ? 's' : '';
    }

    let temp = Math.floor(milliseconds / 1000);

    const hours = Math.floor((temp %= 86400) / 3600);
    if (hours) {
        return `${hours} hour${numberEnding(hours)}`;
    }
    const minutes = Math.floor((temp %= 3600) / 60);
    if (minutes) {
        return `${minutes} minute${numberEnding(minutes)}`;
    }
    const seconds = temp % 60;
    if (seconds) {
        return `${seconds} second${numberEnding(seconds)}`;
    }
    return 'Less than a second';
}

