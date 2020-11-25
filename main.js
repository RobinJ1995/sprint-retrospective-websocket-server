const Os = require('os');
const WebSocket = require('ws');
const Promise = require('bluebird');
const Redis = Promise.promisifyAll(require('redis'));
const { v4: uuid } = require('uuid');
const MessageQueue = require('./MessageQueue');
const { time } = require('./utils');

const PORT = process.env.PORT || 5433;
const REDIS_URL = process.env.REDIS_URL;
const MQ_CONNECTION_URL = process.env.MQ_CONNECTION_URL || 'amqp://localhost';
const MQ_QUEUE_NAME = process.env.MQ_QUEUE_NAME || 'updates';
const SESSION_TIMEOUT_MS = process.env.SESSION_TIMEOUT_MS || 10_000;

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
		removeParticipant(socket);
		socket.close();
	} else if (message.toLowerCase() === 'id') {
		wsSend(socket, socket.id);
	} else if (message.toLowerCase() === 'retro_id') {
		wsSend(socket, socket.retro);
	}
}

const getSocketsForRetro = retroId =>
	[...server.clients].filter(({ retro }) => retro === retroId);
const getNumberOfRetroParticipants = retroId => redis.hlenAsync(`participants::${retroId}`);
const broadcastToRetro = (retroId, message) =>
	getSocketsForRetro(retroId).forEach(socket => wsTrySend(socket, message));
const getAllActiveRetroIds = () =>
	[...[...server.clients].map(({ retro }) => retro).reduce((acc, cur) => acc.add(cur), new Set())];
const broadcastRetroParticipants = (retroId = null) => {
	if (retroId) {
		return getNumberOfRetroParticipants(retroId)
			.then(nParticipants => broadcastToRetro(retroId, `PARTICIPANTS ${nParticipants}`))
			.catch(console.error);
	}

	return Promise.all(getAllActiveRetroIds().map(retroId => broadcastRetroParticipants(retroId)));
}
const updateParticipation = ({ id, retro }) => {
	const redisHashName = `participants::${retro}`;

	return redis.hsetAsync(redisHashName, id, time())
		.then(() => redis.pexpireAsync(redisHashName, SESSION_TIMEOUT_MS))
		.catch(console.error);
}

const mqHandle = message => broadcastToRetro(message.retro, JSON.stringify(message));
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
			updateParticipation(socket)
				.then(() => wsSend(socket, `# Connected to ${Os.hostname()}`))
				.then(() => broadcastRetroParticipants(retro))
				.catch(console.error);
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
	return [...server.clients].filter(({ ping }) => !!ping)
		// Ping not acknowledged
		.filter(({ ping: {ack}}) => !ack)
		// Terminate connection
		.map(brokenSocket => {
			console.warn(`Ping timeout. Closing connection for socket ${brokenSocket.id}.`);
			removeParticipant(brokenSocket);
			wsTrySend(brokenSocket, '# Ping timeout. Goodbye.');
			brokenSocket.terminate();
		})
		.some(x => x); // So that if the list contains at least 1 item, this returns true (to indicate modifications), otherwise false
};
const removeParticipant = ({ id, retro }) => {
	console.log(`Removing participant ${id} to retro ${retro} from Redis.`);
	return redis.hdelAsync(`participants::${retro}`, id)
		.then(nModified => {
			broadcastRetroParticipants(retro);

			return nModified;
		})
}
const purgeTimedoutParticipants = () => Promise.all(getAllActiveRetroIds()
	.map(retroId => `participants::${retroId}`)
	.map(redisHashName => redis.hgetallAsync(redisHashName)
		.then(retroParticipants =>
			Object.entries(retroParticipants)
				.map(([ id, timestamp ]) => {
					const timestampDiff = (timestamp + SESSION_TIMEOUT_MS) - time();
					if (timestampDiff <= 0) {
						console.log(`${redisHashName}->${id} expired ${timestampDiff}ms ago. Removing key from Redis.`);
						return redis.hdelAsync(redisHashName, id); // returns number of keys deleted
					}

					return Promise.resolve(false);
				}))))
	.then(results => results.flatMap(x => x)
		.reduce((acc, cur) => acc || cur, false));

const sendPings = () => {
	[...server.clients].forEach(socket => {
		const pingToken = uuid();
		socket.ping = {
			token: pingToken,
			ack: false,
			sent: time()
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
	const latencyMs = time() - socket.ping.sent;

	updateParticipation(socket);
	wsTrySend(socket, `LATENCY ${latencyMs}`);
};

setInterval(() => {
	Promise.all([
		terminateBrokenConnections(),
		purgeTimedoutParticipants(),
		sendPings()
	]).then(() => broadcastRetroParticipants());
}, SESSION_TIMEOUT_MS);
