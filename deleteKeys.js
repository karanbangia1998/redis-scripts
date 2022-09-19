#!/usr/bin/env node

/* eslint no-console: "off" */

const Redis = require('ioredis');
const {argv} = require('yargs')
    .boolean('t')
    .default('h', 'redis-14825.internal.c14278.asia-seast1-mz.gcp.cloud.rlrcp.com')
    .default('p', 14825)
    .default('a', '7f5pFGQiEPGtmKaFbOWiMvVDjYwBs6Ne')
    .default('t', false)
    .default('pattern', 'lsagoraplatformId:*')

const host = argv.h;
const port = argv.p;
const auth = argv.a;
const tls = argv.t;
const {pattern} = argv;
const {filename} = argv;

let roundCount = 0;
let keyCount = 0;
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


// Start scanning
const stream = redis.scanStream({
    match: pattern,
    count: 10000
});

console.log(`\n*********** START SCANNING FOR PATTERN ${pattern} ***********`);

function extracted() {
    console.log('\n*********** SCAN FINISHED ***********');

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

stream.on('data',async (resultKeys) => {
    roundCount += 1;
    console.log(`\nFound ${resultKeys.length} keys on this round. Round count: ${roundCount}`);

    // Check if we have something to get
    if (resultKeys.length > 0) {
        // Pause scanning
        stream.pause();
        const x= await redis.del(resultKeys);
        stream.resume();
        console.log(`deleted ${x} keys`)
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
