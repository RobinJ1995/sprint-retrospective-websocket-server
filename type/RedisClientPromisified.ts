import {Callback, RedisClient} from 'redis';

interface RedisClientPromisified extends RedisClient {
	hgetallAsync(key: string) : Promise<{ [key: string]: string }>;
	hsetAsync(...args: string[]) : Promise<number>;
	pexpireAsync(key: string, milliseconds: number) : Promise<number>;
	getAsync(key: string) : Promise<string | null>;
	hdelAsync(...args: string[]) : Promise<number>;
	publishAsync(channel: string, value: string) : Promise<number>;
	existsAsync(...args: string[]) : Promise<number>;
};

export default RedisClientPromisified;
