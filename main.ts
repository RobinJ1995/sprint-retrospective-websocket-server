require('console-stamp')(console, {});

import WebSocket from 'ws';
import Os from 'os';
import { v4 as uuid } from 'uuid';
import { time } from './utils';

const PORT : number = parseInt(process.env.PORT || '5433');
const PING_INTERVAL_MS = parseInt(process.env.PING_INTERVAL_MS || String(10_000));
const SESSION_TIMEOUT_MS : number = parseInt(process.env.SESSION_TIMEOUT_MS || String(PING_INTERVAL_MS * 2));

import redis from './redis';
import messageQueue from './message_queue';
import {IncomingMessage} from "http";
import WebSocketExtended from './type/WebSocketExtended';
import RedisAuthenticationEntity from './type/RedisAuthenticationEntity';

const server : WebSocket.Server = new WebSocket.Server({port: PORT});
console.info(`Started webserver on port ${PORT}.`);

const wsSend = (socket: WebSocketExtended, msg : string) => {
	socket.send(msg);
	console.log(`${socket.id} << ${msg}`);
}

const wsTrySend = (socket : WebSocketExtended, msg : string) => {
	try {
		wsSend(socket, msg);
	} catch {
		// Don't care.
	}
}

const wsHandle = (socket : WebSocketExtended, message : string) => {
	console.log(`${socket.id} >> ${message}`);

	if (message.toLowerCase().startsWith('ping ')) {
		const pong : string = message.replace(/^PING\s+/i, '')
			.replace(/\n/, '');
		updateParticipation(socket);
		wsSend(socket, `PONG ${pong}`);
	} else if (message.toLowerCase().startsWith('pong ')) {
		const ping : string = message.replace(/^PONG\s+/i, '')
			.replace(/\n/, '');
		ackPing(socket, ping);
	} else if (message.toLowerCase() === 'disconnect') {
		wsSend(socket, '# Goodbye.');
		removeParticipant(socket);
		socket.close();
	} else if (message.toLowerCase() === 'id') {
		wsSend(socket, socket.id);
	} else if (message.toLowerCase() === 'retro_id') {
		wsSend(socket, socket.retro);
	} else if (message.toLowerCase() === 'participants') {
		getNumberOfRetroParticipants(socket.retro)
			.then((nParticipants : number) => wsSend(socket, `PARTICIPANTS ${nParticipants}`));
	}
}

const getClients = () : WebSocketExtended[] => <WebSocketExtended[]>[...server.clients];
const getSocketsForRetro = (retroId : string) : WebSocketExtended[] =>
	getClients().filter(({ retro } : WebSocketExtended) => retro === retroId);
const getNumberOfRetroParticipants = (retroId : string) : Promise<number> =>
	redis.hgetallAsync(`participants::${retroId}`)
		.then(Object.entries)
		.then((x : any) => x.length)
		.catch((err : Error) => {
			console.warn(`Failed to get participant count for retro ${retroId}`, err);
			return 0;
		})
const broadcastToRetro = (retroId: string, message : string) : void =>
	getSocketsForRetro(retroId).forEach((socket : WebSocketExtended) => wsTrySend(socket, message));
const getAllActiveRetroIds = () : string[] =>
	[...getClients()
		.map(({ retro } : WebSocketExtended) => retro)
		.reduce((acc : Set<string>, cur : string) => acc.add(cur), new Set<string>())];
const broadcastRetroParticipants = (retroId? : string) : Promise<void> => {
	if (retroId) {
		return getNumberOfRetroParticipants(retroId)
			.then(nParticipants => broadcastToRetro(retroId, `PARTICIPANTS ${nParticipants}`))
			.catch(console.error);
	}

	return Promise.all(getAllActiveRetroIds().map(
		(retroId : string) => broadcastRetroParticipants(retroId)))
		.then(() => {});
}
const updateParticipation = ({ id, retro } : WebSocketExtended) => {
	const redisHashName : string = `participants::${retro}`;
	const redisHashExpirationMs : number = SESSION_TIMEOUT_MS + Math.max(5_000, SESSION_TIMEOUT_MS);

	return redis.hsetAsync(redisHashName, id, String(time()))
		.then(() => console.debug(`Updated participation for user ${id} in retro ${retro}.`))
		.then(() => redis.pexpireAsync(redisHashName, redisHashExpirationMs))
		.then(() => console.debug(`Set retro participation hash ${redisHashName} to expire in ${redisHashExpirationMs}ms.`))
		.catch(console.error);
}

const mqHandle = (message : any) => broadcastToRetro(message.retro, JSON.stringify(message));
messageQueue.onReceive(mqHandle);
console.info('Message queue handler installed.');

server.on('connection', (socket : WebSocketExtended, req : IncomingMessage) => {
	socket.id = uuid();
	const token : string | undefined = req.url?.replace(/(^\/)|(\/$)/, '');

	redis.getAsync(String(token))
		.then((data : string | null) : RedisAuthenticationEntity => {
			if (!data) {
				throw new Error(`Token "${token}" not found in Redis.`);
			}

			return JSON.parse(data);
		})
		.then((data : RedisAuthenticationEntity) : RedisAuthenticationEntity => {
			console.log(`Client ${socket.id} connected with token "${token}".`, data);
			return data;
		})
		.then(({ retro } : RedisAuthenticationEntity) => {
			socket.retro = retro;
			socket.lastPing = undefined;

			socket.on('close', () => {
				console.info(`Client ${socket.id} disconnected.`);
				removeParticipant(socket);
			});

			wsSend(socket, '👋');
			updateParticipation(socket)
				.then(() => wsSend(socket, `# Connected to ${Os.hostname()}`))
				.then(() => broadcastRetroParticipants(retro))
				.catch(console.error);
		}).then(() =>
			socket.on('message', (message : string) => wsHandle(socket, message)))
		.catch((ex : Error) => {
			console.error(`Failed to authenticate client ${socket.id} with token "${token}".`, ex);
			wsSend(socket, 'ERROR ' + ex.message);
			socket.close();
		});
});

const terminateBrokenConnections = () : boolean => {
	// Ping sent
	return getClients().filter(({ lastPing } : WebSocketExtended) => !!lastPing)
		// Ping not acknowledged
		.filter((socket : WebSocketExtended) => !socket?.lastPing?.ack)
		// Terminate connection
		.map((brokenSocket : WebSocketExtended) => {
			console.warn(`Ping timeout. Closing connection for socket ${brokenSocket.id}.`);
			removeParticipant(brokenSocket);
			wsTrySend(brokenSocket, '# Ping timeout. Goodbye.');
			brokenSocket.terminate();
		})
		.some(x => x); // So that if the list contains at least 1 item, this returns true (to indicate modifications), otherwise false
};
const removeParticipant = ({ id, retro } : WebSocketExtended) : Promise<number> => {
	console.log(`Removing participant ${id} to retro ${retro} from Redis.`);
	return redis.hdelAsync(`participants::${retro}`, id)
		.then((nModified : number) => {
			broadcastRetroParticipants(retro);

			return nModified;
		})
}

const purgeTimedoutParticipants = () : Promise<number> => Promise.all(getAllActiveRetroIds()
	.map((retroId : string) : string => `participants::${retroId}`)
	.map((redisHashName : string) : Promise<number[]> => redis.hgetallAsync(redisHashName)
		.then((retroParticipants? : Record<string, string>) : Promise<number[]> => {
			if (!retroParticipants) {
				return Promise.resolve([]);
			}

			return <Promise<number[]>>Promise.all(Object.entries(retroParticipants)
				.map(([id, timestamp]) : Promise<number> => {
					const timestampDiff : number = (parseInt(timestamp) + SESSION_TIMEOUT_MS) - time();
					if (timestampDiff <= 0) {
						console.log(`${redisHashName}->${id} expired ${-timestampDiff}ms ago. Removing key from Redis.`);
						return redis.hdelAsync(redisHashName, id); // returns number of keys deleted
					}

					return Promise.resolve(0);
				}));
		})
		.catch((err: Error) : number[] => {
			console.error(`Failed to clean up retro participants hash "${redisHashName}".`, err);
			return [];
		})))
	.then((results : number[][]) : number[] => results.flatMap((r : number[]) => r))
	.then((results : number[]) : number => results.reduce((acc : number, cur : number) : number => acc + cur, 0));


const sendPings = () => {
	getClients().forEach((socket : WebSocketExtended) => {
		const pingToken : string = uuid();
		socket.lastPing = {
			token: pingToken,
			ack: false,
			sent: time()
		};
		wsSend(socket, `PING ${pingToken}`);
	})
}

const ackPing = (socket : WebSocketExtended, token : string) => {
	const expectedToken : string | undefined = socket.lastPing?.token;

	if (token !== expectedToken || !socket.lastPing) {
		wsSend(socket, '# Wrong token.');
		return;
	}

	socket.lastPing.ack = true;
	const latencyMs : number = time() - socket.lastPing.sent;

	updateParticipation(socket);
	wsTrySend(socket, `LATENCY ${latencyMs}`);
};

setInterval(() => {
	terminateBrokenConnections();
	sendPings();
	purgeTimedoutParticipants()
		.then(() => broadcastRetroParticipants());
}, PING_INTERVAL_MS);
