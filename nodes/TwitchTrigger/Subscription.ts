import {
	LoggerProxy,
	NodeOperationError,
	type ITriggerFunctions,
	type IDataObject,
	ApplicationError,
} from 'n8n-workflow';
import { eventConditionBuilders } from './events/conditionBuilders';

const EVENT_VERSIONS: Record<string, string> = {
	'automod.message.hold': '2',
	'automod.message.update': '2',
	'channel.update': '2',
	'channel.follow': '2',
	'channel.moderate': '2',
	'channel.guest_star_session.begin': 'beta',
	'channel.guest_star_session.end': 'beta',
	'channel.guest_star_guest.update': 'beta',
	'channel.guest_star_settings.update': 'beta',
};

function isEqual(a: IDataObject, b: IDataObject): boolean {
	const aKeys = Object.keys(a ?? {}).sort();
	const bKeys = Object.keys(b ?? {}).sort();
	if (aKeys.length !== bKeys.length || aKeys.some((key, i) => key !== bKeys[i])) {
		return false;
	}
	return aKeys.every((key) => a[key] === b[key]);
}

export class Subscription {
	constructor(
		private readonly trigger: ITriggerFunctions,
		private readonly event: string,
		private readonly clientId: string,
	) {}

	async create(sessionId: string): Promise<string> {
		const buildCondition = eventConditionBuilders.get(this.event);
		if (!buildCondition) {
			throw new ApplicationError(`No condition builder found for event: ${this.event}`);
		}

		const condition = await buildCondition(this.trigger, this.event);

		await this.deleteStaleDuplicates(condition);

		const requestBody = {
			type: this.event,
			version: EVENT_VERSIONS[this.event] ?? '1',
			condition,
			transport: {
				method: 'websocket',
				session_id: sessionId,
			},
		};

		try {
			const response = await this.trigger.helpers.httpRequestWithAuthentication.call(
				this.trigger,
				'twitchUserOAuth2Api',
				{
					method: 'POST',
					url: 'https://api.twitch.tv/helix/eventsub/subscriptions',
					headers: {
						'Client-ID': this.clientId,
						'Content-Type': 'application/json',
					},
					body: requestBody,
					json: true,
				},
			);

			const data = response as IDataObject;
			const subscription = (data.data as IDataObject[])[0];
			return subscription.id as string;
		} catch (error) {
			const errorData = error as {
				description?: string;
				message?: string;
			};

			const errorMessage = errorData.description || errorData.message || String(error);

			LoggerProxy.error('Failed to create Twitch EventSub subscription', {
				error: errorMessage,
				event: this.event,
				workflowId: this.trigger.getWorkflow().id,
				nodeType: 'n8n-nodes-twitch.twitchTrigger',
			});

			if (/subscription already exists/i.test(errorMessage)) {
				throw new NodeOperationError(
					this.trigger.getNode(),
					`Another "${this.event}" trigger with the exact same settings (same channel/user/reward, etc.) is already active for this credential — Twitch does not allow two identical EventSub subscriptions.`,
					{
						description:
							'Either remove the duplicate Twitch Trigger node, or change one of its settings (e.g. a different broadcaster, reward, or other condition field) so the two subscriptions are no longer identical.',
					},
				);
			}

			throw new NodeOperationError(
				this.trigger.getNode(),
				`Failed to create Twitch EventSub subscription for event "${this.event}": ${errorMessage}`,
				{
					description:
						'Check that the credential is valid, has the required scopes for this event, and that you have not exceeded Twitch API rate limits.',
				},
			);
		}
	}

	/**
	 * Twitch rejects a create() call if a subscription with the exact same
	 * type + condition already exists, even if it is orphaned (e.g. left
	 * over from a previous activation whose delete() failed, or from n8n
	 * restarting without deactivating the workflow first). Since we are
	 * about to create a fresh subscription with this exact condition, any
	 * existing one is redundant — delete it first so create() can succeed.
	 */
	private async deleteStaleDuplicates(condition: IDataObject): Promise<void> {
		let cursor: string | undefined;

		do {
			let response: IDataObject;
			try {
				response = (await this.trigger.helpers.httpRequestWithAuthentication.call(
					this.trigger,
					'twitchUserOAuth2Api',
					{
						method: 'GET',
						url: 'https://api.twitch.tv/helix/eventsub/subscriptions',
						headers: {
							'Client-ID': this.clientId,
						},
						qs: {
							type: this.event,
							...(cursor ? { after: cursor } : {}),
						},
						json: true,
					},
				)) as IDataObject;
			} catch (error) {
				LoggerProxy.warn('Failed to list existing Twitch EventSub subscriptions', {
					error: error instanceof Error ? error.message : String(error),
					event: this.event,
				});
				return;
			}

			const subscriptions = (response.data as IDataObject[]) ?? [];
			const duplicates = subscriptions.filter((sub) =>
				isEqual(sub.condition as IDataObject, condition),
			);

			for (const duplicate of duplicates) {
				await this.delete(duplicate.id as string);
			}

			cursor = (response.pagination as IDataObject | undefined)?.cursor as string | undefined;
		} while (cursor);
	}

	async delete(subscriptionId: string): Promise<void> {
		try {
			await this.trigger.helpers.httpRequestWithAuthentication.call(
				this.trigger,
				'twitchUserOAuth2Api',
				{
					method: 'DELETE',
					url: `https://api.twitch.tv/helix/eventsub/subscriptions?id=${subscriptionId}`,
					headers: {
						'Client-ID': this.clientId,
					},
				},
			);
		} catch (error) {
			LoggerProxy.warn('Failed to delete EventSub subscription during workflow deactivation', {
				error: error instanceof Error ? error.message : String(error),
				subscriptionId,
				workflowId: this.trigger.getWorkflow().id,
				nodeType: 'n8n-nodes-twitch.twitchTrigger',
			});
		}
	}
}
