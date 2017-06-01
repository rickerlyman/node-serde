"use strict";

const bunyan = require('bunyan');

module.exports = bunyan.createLogger({
    name: 'NodeSerde',
    level: 'info',
    streams: [
        {
            level: 'info',
            stream: process.stdout            // log INFO and above to stdout
        },
        {
            level: 'error',
            path: './serde-error.log'  // log ERROR and above to a file
        }
    ],
});
