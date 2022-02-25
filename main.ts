require('console-stamp')(console, {});

import WebSocket from 'ws';
import Os from 'os';
import { v4 as uuid } from 'uuid';
import { time } from './utils';
import randomItem from 'random-item';

const PORT : number = parseInt(process.env.PORT || '5433');
const PING_INTERVAL_MS = parseInt(process.env.PING_INTERVAL_MS || String(10_000));
const SESSION_TIMEOUT_MS : number = parseInt(process.env.SESSION_TIMEOUT_MS || String(PING_INTERVAL_MS * 2));

const AVATARS = ['🙎‍♂️', '🙍🏽‍♂️', '🙍🏼‍♀️', '🙎🏻‍♀️', '👩🏻‍🏭', '👨🏾‍🏭', '🤦‍♂️', '🤦‍♀️', '🙆‍♂️', '🙆🏻‍♀️', '👨‍🎨', '🧟‍♂️', '🧟‍♀️', '🧛‍♂️', '👨‍💻', '👩🏻‍💻', '👨‍💼', '👨🏽‍🍳', '👩🏾‍🍳', '👩🏻‍🍳', '👨🏻‍🔧', '👩🏽‍🔧', '👨‍🌾', '👨🏽‍🎓', '👩🏻‍🎓', '👨‍🚒', '🧖🏻‍♀️', '🧖🏿‍♀️', '🧖🏻‍♂️', '🧖🏾‍♂️', '🕵️‍♂️', '🕵🏻‍♀️', '🙋🏼‍♂️', '🙋🏼‍♀️', '🤹‍♂️', '🤹🏻‍♀️', '🧝🏽‍♂️', '🧝🏻‍♀️', '🤵🏼', '🦸🏻‍♀️', '🧑‍🔬', '🧑‍🎤', '👩‍🎤', '🧑‍🚀', '👩‍🚀', '💂‍♂️', '💂‍♀️', '🧛‍♀️'];
const SECTIONS = Object.freeze({
	GOOD: 'good',
	BAD: 'bad',
	ACTIONS: 'actions'
});

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
		getAvatarsForRetroParticipants(socket.retro)
			.then((avatars : string[]) => wsSend(socket, `AVATARS ${avatars.join(',')}`));
	} else if (message.toLowerCase().startsWith('typing ')) {
		const args : string[] = message.replace(/\n/, '')
			.split(/\s+/);
		if (args.length != 3) {
			console.warn(`Got TYPING message but ${args.length} arguments instead of the expected 3.`, { message });
			return;
		}
		const what = String(args[1]).toLowerCase();
		const where = String(args[2]).toLowerCase();

		if (!['start', 'stop', 'still'].includes(what)) {
			console.warn(`Got TYPING message with unexpected action.`, {
				expected: ['start', 'stop', 'still'],
				actual: what
			});
			return;
		} else if (!Object.values(SECTIONS).includes(where)) {
			console.warn(`Got TYPING message with unexpected section.`, {
				expected: Object.keys(SECTIONS),
				actual: where
			});
			return;
		}

		broadcastToRetro(socket.retro, `TYPING ${socket.id} ${what} ${where}`, [socket.id]);
	}
}

const getClients = () : WebSocketExtended[] => <WebSocketExtended[]>[...server.clients];
const getSocketsForRetro = (retroId : string) : WebSocketExtended[] =>
	getClients().filter(({ retro } : WebSocketExtended) => retro === retroId);
const getNumberOfRetroParticipants = (retroId : string) : Promise<number> =>
	redis.hgetallAsync(`participants::${retroId}`)
		.then(participantInfo => Object.keys(participantInfo || {}))
		.then(keys => keys.filter(k => !k.endsWith('::avatar')))
		.then((x : any) => <number>x.length)
		.catch((err : Error) => {
			console.warn(`Failed to get participant count for retro ${retroId}`, err);
			return 0;
		});
const getAvatarsForRetroParticipants = (retroId : string) : Promise<string[]> =>
	redis.hgetallAsync(`participants::${retroId}`)
		.then(participantInfo => Object.entries(participantInfo || {}))
		.then(entries => entries.filter(([k, v]) => k.endsWith('::avatar')).map(([k, v]) => v))
		.catch((err : Error) => {
			console.warn(`Failed to get participant avatars for retro ${retroId}`, err);
			return [];
		});
const broadcastToRetro = (retroId: string, message : string, exceptSocketIds: string[] = []) : void =>
	getSocketsForRetro(retroId)
		.filter(socket => !exceptSocketIds.includes(socket.id))
		.forEach((socket : WebSocketExtended) => wsTrySend(socket, message));
const getAllActiveRetroIds = () : string[] =>
	[...getClients()
		.map(({ retro } : WebSocketExtended) => retro)
		.reduce((acc : Set<string>, cur : string) => acc.add(cur), new Set<string>())];
const broadcastRetroParticipants = (retroId? : string) : Promise<void> => {
	if (retroId) {
		return Promise.all([
			getNumberOfRetroParticipants(retroId)
				.then((nParticipants : number) => broadcastToRetro(retroId, `PARTICIPANTS ${nParticipants}`))
				.catch(console.error),
			getAvatarsForRetroParticipants(retroId)
				.then((avatars : string[]) => broadcastToRetro(retroId, `AVATARS ${avatars.join(',')}`))
				.catch(console.error)
		]).then(() => {});
	}

	return Promise.all(getAllActiveRetroIds().map(
		(retroId : string) => broadcastRetroParticipants(retroId)))
		.then(() => {});
}
const updateParticipation = ({ id, retro, avatar } : WebSocketExtended) => {
	const redisHashName : string = `participants::${retro}`;
	const redisHashExpirationMs : number = SESSION_TIMEOUT_MS + Math.max(5_000, SESSION_TIMEOUT_MS);

	return redis.hsetAsync(redisHashName, id, String(time()))
		.then(() => redis.hsetAsync(redisHashName, `${id}::avatar`, avatar))
		.then(() => console.debug(`Updated participation for user ${id} in retro ${retro}.`))
		.then(() => redis.pexpireAsync(redisHashName, redisHashExpirationMs))
		.then(() => console.debug(`Set retro participation hash ${redisHashName} to expire in ${redisHashExpirationMs}ms.`))
		.catch(console.error);
}
const findAvailableAvatar = (retroId: string) : Promise<string> => {
	const redisHashName : string = `participants::${retroId}`;
	console.info(`Finding available avatar for retro ${retroId}...`);

	return redis.hgetallAsync(redisHashName)
		.then(participantInfo => participantInfo || {})
		.then(participantInfo => {
			const avatarsInUse = Object.entries(participantInfo)
				.filter(([k, v]) => String(k).endsWith('::avatar'))
				.map(([k, v]) => v);
			if (avatarsInUse.length >= AVATARS.length) {
				console.warn(`There are no more available avatar for retro ${retroId}: 🐣`);
				return '🐣';
			}

			let avatar = null;
			let attempts = 0;
			while (avatar === null || avatarsInUse.includes(avatar)) {
				if (attempts > 100) {
					// Because I'm paranoid.
					console.warn(`Could not find an available avatar for ${retroId} after ${attempts} attempts: 💩`);
					return '💩';
				}

				avatar = randomItem(AVATARS);
				attempts++;
			}

			console.info(`Found available avatar for retro ${retroId}: ${avatar}`);

			return avatar;
		});
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
			console.info(`Client ${socket.id} connected with token "${token}".`, data);
			return data;
		})
		.then(({ retro } : RedisAuthenticationEntity) => {
			socket.retro = retro;
			socket.lastPing = undefined;

			return findAvailableAvatar(retro)
				.then(avatar => {
					socket.avatar = avatar;
				});
		})
		.then(() => {
			socket.on('close', () => {
				console.info(`Client ${socket.id} disconnected.`);
				removeParticipant(socket);
			});

			wsSend(socket, '👋');
			updateParticipation(socket)
				.then(() => wsSend(socket, `CONNECTED_TO ${Os.hostname()}`))
				.then(() => broadcastRetroParticipants(socket.retro))
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
	return redis.hdelAsync(`participants::${retro}`, `${id}::avatar`)
		.then(() => redis.hdelAsync(`participants::${retro}`, id))
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
				.filter(([k, v]) => !k.endsWith('::avatar'))
				.map(([id, timestamp]) : Promise<number> => {
					const timestampDiff : number = (parseInt(timestamp) + SESSION_TIMEOUT_MS) - time();
					if (timestampDiff <= 0) {
						console.log(`${redisHashName}->${id} expired ${-timestampDiff}ms ago. Removing key from Redis.`);
						return redis.hdelAsync(redisHashName, `${id}::avatar`)
							.then(() => redis.hdelAsync(redisHashName, id)); // returns number of keys deleted
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
