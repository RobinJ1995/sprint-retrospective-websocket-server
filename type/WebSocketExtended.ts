import WebSocket from 'ws';

interface WebSocketExtended extends WebSocket {
	id : string,
	retro : string,
	lastPing? : {
		token : string,
		ack : boolean,
		sent : number
	}
};

export default WebSocketExtended;
