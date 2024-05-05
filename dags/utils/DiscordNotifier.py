from airflow.notifications.basenotifier import BaseNotifier
import discord_notify as dn


class DiscordNotifier(BaseNotifier):

    def __init__(self, msg):
        self.msg = msg
        ## you may change the webhook url here to your discord chanles webhook:
        self.webhook_url = 'https://discord.com/api/webhooks/1236675995331985418/6EwNWgvUCjc3cHyclZTZaugJGHwaczgwc-j8D24zbqR_SeboHlUZAy0uAG3nexnYVjLN'
        self.notifier = dn.Notifier(self.webhook_url)

    def notify(self, context):
        # Send notification here, below is an example
        title = f">>Check DAG {context['dag'].dag_id} Task {context['task_instance'].task_id}"
        self.notifier.send(self.msg + title, True)



