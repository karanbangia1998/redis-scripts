#!/usr/bin/env node

/* eslint no-console: "off" */

const Redis = require('ioredis');
const fs = require('fs');
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
const {pattern} = argv;
const {filename} = argv;

let roundCount = 0;
let keyCount = 0;
let sep = '';
const startTime = new Date();

let redisConfig = {
    host: host,
    port: port,
    password: auth
}

if (tls === true) (
    redisConfig.tls = {}
)

const redis = new Redis(redisConfig);

// Delete previous file
if (fs.existsSync(filename)) {
    fs.unlinkSync(filename);
}

// Create new fil with '['
const fd = fs.openSync(filename, 'a');

fs.appendFileSync(fd, '[', 'utf8');

// Start scanning
const stream = redis.scanStream({
    match: pattern,
    count: 10000
});

console.log(`\n*********** START SCANNING FOR PATTERN ${pattern} ***********`);

function extracted() {
    console.log('\n*********** SCAN FINISHED ***********');

    // Close file
    fs.appendFileSync(fd, ']', 'utf8');
    if (fd !== undefined) {
        fs.closeSync(fd);
    }

    // Stop timer
    const executionTimeMs = new Date() - startTime;
    const executionTimeStr = millisecondsToStr(executionTimeMs);

    // Summary
    console.log(`\nNumber of rounds: ${roundCount}`);
    console.log(`\nNumber of Key: ${roundCount}`);
    console.log(`Number of keyValues found: ${keyCount}`);
    console.log(`Filename: ${filename}`);
    console.info(`Execution time: ${executionTimeStr}`);
    process.exit();
}

stream.on('data', (resultKeys) => {
    roundCount += 1;
    console.log(`\nFound ${resultKeys.length} keys on this round. Round count: ${roundCount}`);

    // Check if we have something to get
    if (resultKeys.length > 0) {
        // Pause scanning
        stream.pause();
        const keyValues = {};
        console.log('Getting values');
        console.log(resultKeys);
        redis.mget(resultKeys)
            .then((resultValues) => {
                console.log(`Got ${resultValues.length} values`);
                for (let i = 0; i < resultKeys.length; i++) {
                    keyValues[resultKeys[i]] = resultValues[i];
                }
                keyCount += resultKeys.length;

                // Write the object to file
                console.log('Write key-values to file');
                fs.appendFileSync(fd, sep + JSON.stringify(keyValues), 'utf8');
                sep = ',';

                // Resume scanning
                stream.resume();
            })
            .catch((reason) => {
                console.log(`Error on mget: ${reason}`);
                process.exit(1);
            });
    }
});
stream.on('end', () => {
    extracted()
});

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
