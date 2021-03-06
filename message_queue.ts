import Bluebird from 'bluebird';
import Redis, {RedisClient} from 'redis';
import RedisClientPromisified from './type/RedisClientPromisified';
const RedisPromisified = Bluebird.promisifyAll(Redis);

// You can't use the same Redis client for pub/sub and other operations
const REDIS_URL: string = process.env.REDIS_URL || 'redis://localhost';
console.info(`Connecting to Redis (for pub/sub) at ${REDIS_URL}...`);
const redis: RedisClientPromisified = <RedisClientPromisified>RedisPromisified.createClient(REDIS_URL);

const REDIS_PUBSUB_TOPIC: string = process.env.REDIS_PUBSUB_TOPIC || 'updates';

const tryParseJson : any | string = (data: string) => {
	try {
		return JSON.parse(data);
	} catch {
		return data;
	}
}

class MessageQueue {
	send(message : string) : Promise<void> {
		return redis.publishAsync(REDIS_PUBSUB_TOPIC, JSON.stringify(message))
			.then(() => console.log(`<<<<< ${JSON.stringify(message)}`))
			.catch((err: Error) =>
				console.error(`Failed to send message ${JSON.stringify(message)}`, err));
	}

	onReceive(callback : Function) : RedisClientPromisified {
		redis.subscribe(REDIS_PUBSUB_TOPIC);
		console.info(`Subscribed to topic: ${REDIS_PUBSUB_TOPIC}`);

		return redis.on('message',
			(topic: string, message: string) => {
				if (topic !== REDIS_PUBSUB_TOPIC) {
					console.warn(`Received message on topic "${topic}", but only `
						+ `listening on topic ${REDIS_PUBSUB_TOPIC}. Ignoring message: ${message}`);
					return;
				}

				console.log(`>>>>> ${message}`);
				return Promise.resolve(callback(tryParseJson(message)))
					.then(() => console.log(`>ACK> ${message}`))
			});
	}
};

export default new MessageQueue();
