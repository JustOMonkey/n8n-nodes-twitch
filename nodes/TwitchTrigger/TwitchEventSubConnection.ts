import type { ITriggerFunctions, IDataObject } from 'n8n-workflow';
import { TwitchEventSubManager } from './TwitchEventSubManager';

/**
 * Per-trigger handle onto a shared Twitch EventSub WebSocket connection.
 * See TwitchEventSubManager for the connection-sharing logic.
 */
export class TwitchEventSubConnection {
	private subscriberId: string | null = null;

	constructor(
		private readonly trigger: ITriggerFunctions,
		private readonly event: string,
		private readonly clientId: string,
		private readonly onNotification: (data: IDataObject) => void,
	) {}

	async connect(): Promise<void> {
		this.subscriberId = await TwitchEventSubManager.subscribe(
			this.trigger,
			this.event,
			this.clientId,
			this.onNotification,
		);
	}

	async close(): Promise<void> {
		if (this.subscriberId) {
			await TwitchEventSubManager.unsubscribe(this.trigger, this.clientId, this.subscriberId);
			this.subscriberId = null;
		}
	}
}
