const WebSocket = require('ws');
const Promise = require('bluebird');
const Redis = Promise.promisifyAll(require('redis'));
const { v4: uuid } = require('uuid');
const MessageQueue = require('./MessageQueue');

const PORT = process.env.PORT || 5433;
const REDIS_URL = process.env.REDIS_URL;
const SQS_CONFIG = {
	access_key_id: process.env.SQS_ACCESS_KEY_ID || '*',
	secret_access_key: process.env.SQS_SECRET_ACCESS_KEY || '*',
	endpoint: process.env.SQS_ENDPOINT,
	queue: process.env.SQS_QUEUE_NAME
};

const redis = Redis.createClient(REDIS_URL);
const mq = new MessageQueue(SQS_CONFIG);

const server = new WebSocket.Server({port: PORT});

const wsSend = (socket, msg) => {
	socket.send(msg);
	console.log(`${socket.id} << ${msg}`);
}

const wsHandle = (socket, message) => {
	console.log(`${socket.id} >> ${message}`);

	if (message.toLowerCase().startsWith('ping ')) {
		const pong = message.replace(/^PING\s+/i, '').replace(/\n/, '');
		wsSend(socket, `PONG ${pong}`);
	} else if (message.toLowerCase() === 'disconnect') {
		wsSend(socket, 'Goodbye.');
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
			wsSend(socket, '👋');
		}).then(() =>
			socket.on('message', message => wsHandle(socket, message)))
		.catch(ex => {
			console.error(`Failed to authenticate client ${socket.id} with token "${token}".`, ex);
			wsSend(socket, 'ERROR ' + ex.message);
			socket.close();
		});
});
