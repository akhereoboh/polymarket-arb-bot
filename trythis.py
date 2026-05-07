# import requests

# # Replace with your bot token from BotFather
# BOT_TOKEN = "8788064599:AAGukyCte5A_4knNYGckjiD1_HYfNqQcTfc"

# # Telegram API URL
# url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"

# def get_chat_id():
#     response = requests.get(url)
#     data = response.json()

#     # Print full response for debugging
#     print("Full response:", data)

#     if "result" in data and len(data["result"]) > 0:
#         chat_id = data["result"][0]["message"]["chat"]["id"]
#         print("✅ Your chat ID is:", chat_id)
#         return chat_id
#     else:
#         print("⚠️ No chat ID found. Make sure you send a message to your bot first!")
#         return None

# if __name__ == "__main__":
#     get_chat_id()
