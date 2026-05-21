# import requests

# # Replace with your bot token from BotFather
# BOT_TOKEN = "8222724046:AAHWJwd4hAs_hnDOQXPbLkdegXyOifsIiIE"

# # Get updates from your bot
# url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
# resp = requests.get(url)
# data = resp.json()

# # Print chat IDs from updates
# for update in data["result"]:
#     chat_id = update["message"]["chat"]["id"]
#     print("Chat ID:", chat_id)
