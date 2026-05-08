import requests

url = "https://gamma-api.polymarket.com/events/keyset?limit=20&closed=false&order=volume24hr&ascending=false"
resp = requests.get(url)
data = resp.json()

events = data if isinstance(data, list) else data.get("events", [])

print("\n--- Active Events ---")
for event in events:
    title = event.get("title", "")
    end_time = event.get("end_date")
    print(f"Event: {title} | Ends: {end_time}")

    for m in event.get("markets", []):
        q = m.get("question")
        volume = m.get("volume")
        print(f"   Market: {q} | Volume: {volume}")
    print()
