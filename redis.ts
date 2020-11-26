import Promise from 'bluebird';
import Redis from 'redis';
import RedisClientPromisified from "./type/RedisClientPromisified";
const RedisPromisified = Promise.promisifyAll(Redis);

const REDIS_URL: string = process.env.REDIS_URL || 'redis://localhost';
console.info(`Connecting to Redis at ${REDIS_URL}...`);
const redis: RedisClientPromisified = <RedisClientPromisified>RedisPromisified.createClient(REDIS_URL);

export default redis;
