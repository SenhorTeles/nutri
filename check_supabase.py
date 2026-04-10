import requests

SUPABASE_URL = "https://pdtxpsdbjrcrmockdfgw.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBkdHhwc2RianJjcm1vY2tkZmd3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3Mzg1MzY4MywiZXhwIjoyMDg5NDI5NjgzfQ.49jlPkuR0kd36tb8CtlEYAoHAGfBBiRwx-QvWbw54i8"

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}

# 1. Total row count
url_count = f"{SUPABASE_URL}/rest/v1/confrontofiscal?select=id"
headers["Prefer"] = "count=exact"
resp = requests.head(url_count, headers=headers)
print(f"Total Rows: {resp.headers.get('content-range')}")

# 2. Get some duplicate counts
url_data = f"{SUPABASE_URL}/rest/v1/confrontofiscal?select=chavecte&limit=1000"
resp2 = requests.get(url_data, headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"})
if resp2.status_code == 200:
    data = resp2.json()
    chaves = [d.get("chavecte") for d in data]
    print(f"Sampled {len(chaves)} rows.")
    import collections
    counts = collections.Counter(chaves)
    duplicates = {k: v for k, v in counts.items() if v > 1}
    print(f"Duplicates in sample: {len(duplicates)}")
    if duplicates:
        print("Some duplicates:", list(duplicates.items())[:5])
else:
    print(f"Error fetching data: {resp2.status_code} {resp2.text}")
