const Os = require('os');
const WebSocket = require('ws');
const Promise = require('bluebird');
const Redis = Promise.promisifyAll(require('redis'));
const { v4: uuid } = require('uuid');
const MessageQueue = require('./MessageQueue');

const PORT = process.env.PORT || 5433;
const REDIS_URL = process.env.REDIS_URL;
const MQ_CONNECTION_URL = process.env.MQ_CONNECTION_URL || 'amqp://localhost';
const MQ_QUEUE_NAME = process.env.MQ_QUEUE_NAME || 'updates';

const redis = Redis.createClient(REDIS_URL);
const mq = new MessageQueue(MQ_CONNECTION_URL, MQ_QUEUE_NAME);

const server = new WebSocket.Server({port: PORT});

const wsSend = (socket, msg) => {
	socket.send(msg);
	console.log(`${socket.id} << ${msg}`);
}

const wsTrySend = (socket, msg) => {
	try {
		wsSend(socket, msg);
	} catch {
		// Don't care.
	}
}

const wsHandle = (socket, message) => {
	console.log(`${socket.id} >> ${message}`);

	if (message.toLowerCase().startsWith('ping ')) {
		const pong = message.replace(/^PING\s+/i, '').replace(/\n/, '');
		wsSend(socket, `PONG ${pong}`);
	} else if (message.toLowerCase().startsWith('pong ')) {
		const ping = message.replace(/^PONG\s+/i, '').replace(/\n/, '');
		ackPing(socket, ping);
	} else if (message.toLowerCase() === 'disconnect') {
		wsSend(socket, '# Goodbye.');
		socket.close();
	} else if (message.toLowerCase() === 'id') {
		wsSend(socket, socket.id);
	} else if (message.toLowerCase() === 'retro_id') {
		wsSend(socket, socket.retro);
	}
}

const mqHandle = message => {
	[...server.clients].filter(({ retro }) => retro === message.retro)
		.forEach(socket => wsSend(socket, JSON.stringify(message)));
}

mq.onReceive(mqHandle);

server.on('connection', (socket, req) => {
	socket.id = uuid();
	const token = req.url.replace(/(^\/)|(\/$)/, '');

	redis.getAsync(token)
		.then(JSON.parse)
		.then(data => console.log(`Client ${socket.id} connected with token "${token}".`, data) || data)
		.then(({ retro }) => {
			socket.retro = retro;
			socket.ping = null;
			wsSend(socket, '👋');
			wsSend(socket, `# Connected to ${Os.hostname()}`);
		}).then(() =>
			socket.on('message', message => wsHandle(socket, message)))
		.catch(ex => {
			console.error(`Failed to authenticate client ${socket.id} with token "${token}".`, ex);
			wsSend(socket, 'ERROR ' + ex.message);
			socket.close();
		});
});

const terminateBrokenConnections = () => {
	// Ping sent
	[...server.clients].filter(({ ping }) => !!ping)
		// Ping not acknowledged
		.filter(({ ping: {ack}}) => !ack)
		// Terminate connection
		.forEach(brokenSocket => {
			console.warn(`Ping timeout. Closing connection for socket ${brokenSocket.id}.`);
			wsTrySend(brokenSocket, '# Ping timeout. Goodbye.');
			brokenSocket.terminate();
		});
}

const sendPings = () => {
	[...server.clients].forEach(socket => {
		const pingToken = uuid();
		socket.ping = {
			token: pingToken,
			ack: false
		};
		wsSend(socket, `PING ${pingToken}`);
	})
}

const ackPing = (socket, token) => {
	const expectedToken = socket.ping?.token;

	if (token !== expectedToken) {
		wsSend(socket, '# Wrong token.');
		return;
	}

	socket.ping.ack = true;
};

setInterval(() => {
	terminateBrokenConnections();
	sendPings();
}, 10_000);
