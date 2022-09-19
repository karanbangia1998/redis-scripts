#!/usr/bin/env node
const StreamArray = require('stream-json/streamers/StreamArray');
const fs = require('fs');

const Redis = require('ioredis');
const {argv} = require('yargs')
    .boolean('t')
    .default('h', 'dummyhost')
    .default('p', 1234)
    .default('a', 'dummypassword')
    .default('t', false)
    .default('pattern', '*')
    .default('filename', 'dummyfile');

const host = argv.h;
const port = argv.p;
const auth = argv.a;
const tls = argv.t;
const {filename} = argv;

let redisConfig = {
    host: host,
    port: port,
    password: auth
}

if (tls === true) (
    redisConfig.tls = {}
)

const redis = new Redis(redisConfig);

let sep = '';


if (fs.existsSync(filename)) {
    fs.unlinkSync(filename);
}
const fd = fs.openSync(filename, 'a');
fs.appendFileSync(fd, '[', 'utf8');

let readFile = "missedInserts1.json"

function main() {
    const startTime = new Date();
    const jsonStream = StreamArray.withParser();
    fs.createReadStream(readFile).pipe(jsonStream.input);
    jsonStream.on('data', ({key, value}) => {
        console.log("\nIteration no:", key)
        let rows = []
        Object.keys(value).forEach(function (k) {
            rows.push(`lsagoraplatformId:${k}`)
        });
        const keyValues = {};

        jsonStream.pause()
        redis.mget(rows)
            .then((resultValues) => {
                console.log(`Got ${resultValues.length} values`);
                for (let i = 0; i < rows.length; i++) {
                    if (resultValues[i]) {
                        keyValues[rows[i]] = resultValues[i];
                    }
                }
                // Write the object to file
                console.log('Write key-values to file');
                fs.appendFileSync(fd, sep + JSON.stringify(keyValues), 'utf8');
                sep = ',';
                // Resume scanning
                jsonStream.resume();

            }).catch((reason) => {
            console.log(`Error on mget: ${reason}`);
            process.exit(1);
        });

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

main()


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

