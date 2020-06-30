const SQS = require('sqs');
const Promise = require('bluebird');

module.exports = class MessageQueue {
	constructor(sqsConfig) {
		this.sqsClient = new SQS({
			access: sqsConfig.access_key_id,
			secret: sqsConfig.secret_access_key,
			proxy: sqsConfig.endpoint
		});
		this.queue = sqsConfig.queue;
	}

	send = message => new Promise((resolve, reject) => {
		try {
			console.log(`MQ <<<<< ${message}`);
			this.sqsClient.push(this.queue, message, resolve);
		} catch (ex) {
			return reject(ex);
		}
	});

	onReceive = callback => this.sqsClient.pull(this.queue,
		(message, acknowledge) => {
			const strMessage = JSON.stringify(message);
			console.log(`MQ >>>>> ${strMessage}`);

			return Promise.resolve(callback(message))
				.then(() => acknowledge())
				.then(() => console.log(`MQ >ACK> ${strMessage}`))
				.catch(console.error);
		});
}
