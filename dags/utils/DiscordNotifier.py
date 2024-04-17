from airflow.notifications.basenotifier import BaseNotifier
import discord_notify as dn


class DiscordNotifier(BaseNotifier):

    def __init__(self, msg):
        self.msg = msg
        ## you may change the webhook url here to your discord chanles webhook:
        self.webhook_url = 'https://discord.com/api/webhooks/1225103273862762687/yJnViUXl1eJ58B1jwMq5VrSgVwYw3_8drtmc7S8rrKkjg3jukdIUhvk1Oki711oigww4'
        self.notifier = dn.Notifier(self.webhook_url)

    def notify(self, context):
        # Send notification here, below is an example
        title = f">>Check Task {context['task_instance'].task_id}"
        self.notifier.send(self.msg + title, True)



