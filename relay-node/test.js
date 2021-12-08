const { RelayNodeServer } = require('./index');
const { inspect } = require('util');

// This needs to be global so it doesn't get GC'd
server = new RelayNodeServer();

server.start((_, str) => console.log(inspect(JSON.parse(str), false, null, true)));
