const Amqplib = require('amqplib');
const Promise = require('bluebird');

const tryParseJson = data => {
	try {
		return JSON.parse(data);
	} catch {
		return data;
	}
}

module.exports = class MessageQueue {
	constructor(url, queueName) {
		this.client = Amqplib.connect(url)
		this.queue = queueName;
	}

	send = message => {
		return this.client.then(connection => connection.createChannel())
			.then(ch => ch.assertExchange(this.queue, 'fanout', {durable: false})
				.then(() => ch))
			.then(ch => ch.publish(
				this.queue, '', Buffer.from(JSON.stringify(message))))
			.then(() => console.log(`<<<<< ${JSON.stringify(message)}`))
			.catch(err => console.error(`Failed to send message ${JSON.stringify(message)}`, err));
	}

	onReceive = callback => this.client.then(connection => connection.createChannel())
		.then(ch => ch.assertExchange(this.queue, 'fanout', {durable: false})
			.then(() => ch))
		.then(ch => ch.assertQueue('', {exclusive: true})
			.then(q => ch.bindQueue(q.queue, this.queue, '')
				.then(() => [ch, q])))
		.then(([ch, q]) => ch.consume(q.queue, message => {
			const messageContent = message.content.toString();
			console.log(`>>>>> ${messageContent}`);

			return Promise.resolve(callback(tryParseJson(messageContent)))
				.then(() => console.log(`>ACK> ${messageContent}`))
		}, {noAck: true}))
		.catch(console.error);
}
