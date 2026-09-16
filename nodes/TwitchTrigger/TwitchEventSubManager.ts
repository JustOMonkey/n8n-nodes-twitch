import { LoggerProxy, type ITriggerFunctions, type IDataObject, sleep } from 'n8n-workflow';
import { EventSubWebSocket } from './EventSubWebSocket';
import { Subscription } from './Subscription';

const MAX_RECONNECT_ATTEMPTS = 5;
const BASE_RECONNECT_DELAY_MS = 1000;

interface Subscriber {
	event: string;
	subscription: Subscription;
	onNotification: (data: IDataObject) => void;
	subscriptionId: string | null;
}

/**
 * Twitch limits EventSub WebSocket connections to 3 per (Client ID + User ID).
 * A single connection can carry many subscriptions, so all Twitch Trigger nodes
 * that share the same credential share one connection here instead of opening
 * one WebSocket each.
 */
class SharedEventSubConnection {
	private ws: EventSubWebSocket;
	private sessionId: string | null = null;
	private subscribers = new Map<string, Subscriber>();
	private closing = false;
	private reconnectAttempts = 0;
	private nextSubscriberId = 0;

	constructor(
		private readonly clientId: string,
		private readonly onEmpty: () => void,
	) {
		this.ws = this.createWebSocket();
	}

	async connect(): Promise<void> {
		await this.ws.connect();
	}

	async addSubscriber(
		trigger: ITriggerFunctions,
		event: string,
		onNotification: (data: IDataObject) => void,
	): Promise<string> {
		const id = `sub_${this.nextSubscriberId++}`;
		const subscriber: Subscriber = {
			event,
			subscription: new Subscription(trigger, event, this.clientId),
			onNotification,
			subscriptionId: null,
		};
		this.subscribers.set(id, subscriber);

		if (this.sessionId) {
			subscriber.subscriptionId = await subscriber.subscription.create(this.sessionId);
		}

		return id;
	}

	async removeSubscriber(id: string): Promise<void> {
		const subscriber = this.subscribers.get(id);
		if (!subscriber) {
			return;
		}

		if (subscriber.subscriptionId) {
			await subscriber.subscription.delete(subscriber.subscriptionId);
		}
		this.subscribers.delete(id);

		if (this.subscribers.size === 0) {
			this.closing = true;
			this.ws.close();
			this.onEmpty();
		}
	}

	private createWebSocket(): EventSubWebSocket {
		return new EventSubWebSocket(
			async (sessionId) => this.handleSessionWelcome(sessionId),
			(data, subscriptionId) => this.dispatchNotification(data, subscriptionId),
			(subscriptionId) => this.handleRevocation(subscriptionId),
			this.clientId,
			() => this.handleDisconnect(),
		);
	}

	private async handleSessionWelcome(sessionId: string): Promise<void> {
		this.sessionId = sessionId;
		this.reconnectAttempts = 0;

		// On (re)connect, re-create subscriptions for every subscriber already
		// registered on this connection (e.g. after a WebSocket reconnect).
		for (const subscriber of this.subscribers.values()) {
			try {
				subscriber.subscriptionId = await subscriber.subscription.create(sessionId);
			} catch (error) {
				LoggerProxy.error('Failed to re-create Twitch EventSub subscription after reconnect', {
					error: error instanceof Error ? error.message : String(error),
					event: subscriber.event,
				});
			}
		}
	}

	private dispatchNotification(data: IDataObject, subscriptionId: string): void {
		for (const subscriber of this.subscribers.values()) {
			if (subscriber.subscriptionId === subscriptionId) {
				subscriber.onNotification(data);
				return;
			}
		}
	}

	private handleRevocation(subscriptionId: string): void {
		for (const subscriber of this.subscribers.values()) {
			if (subscriber.subscriptionId === subscriptionId) {
				subscriber.subscriptionId = null;
				return;
			}
		}
	}

	private handleDisconnect(): void {
		if (this.closing) {
			return;
		}

		if (this.reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
			LoggerProxy.error('Twitch EventSub WebSocket: max reconnect attempts reached, giving up', {
				nodeType: 'n8n-nodes-twitch.twitchTrigger',
				attempts: this.reconnectAttempts,
			});
			return;
		}

		this.reconnectAttempts++;
		const delay = BASE_RECONNECT_DELAY_MS * Math.pow(2, this.reconnectAttempts - 1);

		LoggerProxy.warn(
			`Twitch EventSub WebSocket disconnected, reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`,
			{ nodeType: 'n8n-nodes-twitch.twitchTrigger' },
		);

		void sleep(delay).then(() => {
			if (this.closing) {
				return;
			}
			void this.reconnect();
		});
	}

	private async reconnect(): Promise<void> {
		try {
			this.sessionId = null;
			this.ws = this.createWebSocket();
			await this.ws.connect();
		} catch (error) {
			LoggerProxy.error('Twitch EventSub WebSocket reconnection failed', {
				error: error instanceof Error ? error.message : String(error),
				nodeType: 'n8n-nodes-twitch.twitchTrigger',
				attempt: this.reconnectAttempts,
			});
			this.handleDisconnect();
		}
	}
}

interface ConnectionEntry {
	connection: SharedEventSubConnection;
	connectPromise: Promise<void>;
}

/**
 * Process-wide registry of shared EventSub connections, one per Twitch
 * credential (Client ID + credential ID), so multiple Twitch Trigger nodes
 * never exceed Twitch's 3-WebSocket-per-user limit.
 */
export class TwitchEventSubManager {
	private static connections = new Map<string, ConnectionEntry>();

	static async subscribe(
		trigger: ITriggerFunctions,
		event: string,
		clientId: string,
		onNotification: (data: IDataObject) => void,
	): Promise<string> {
		const key = this.getConnectionKey(trigger, clientId);
		let entry = this.connections.get(key);

		if (!entry) {
			const connection = new SharedEventSubConnection(clientId, () => {
				if (this.connections.get(key)?.connection === connection) {
					this.connections.delete(key);
				}
			});
			entry = { connection, connectPromise: connection.connect() };
			this.connections.set(key, entry);
		}

		await entry.connectPromise;
		return entry.connection.addSubscriber(trigger, event, onNotification);
	}

	static async unsubscribe(
		trigger: ITriggerFunctions,
		clientId: string,
		subscriberId: string,
	): Promise<void> {
		const key = this.getConnectionKey(trigger, clientId);
		const entry = this.connections.get(key);
		if (!entry) {
			return;
		}
		await entry.connection.removeSubscriber(subscriberId);
	}

	private static getConnectionKey(trigger: ITriggerFunctions, clientId: string): string {
		const credentialId = trigger.getNode().credentials?.twitchUserOAuth2Api?.id;
		return credentialId ? `cred_${credentialId}` : `client_${clientId}`;
	}
}
