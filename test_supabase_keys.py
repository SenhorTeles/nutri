import urllib.request
import urllib.error

URL = 'https://hzljpmjhmrgwjjskubii.supabase.co/rest/v1/confrontofiscal?select=*&limit=1'
ANON = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh6bGpwbWpobXJnd2pqc2t1YmlpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQwMzUxMTAsImV4cCI6MjA4OTYxMTExMH0.8AVmg5kBhWI_KWjqqfDKzH9SSn0Vwfi1yAfywvdjt4Q'
SECRET = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh6bGpwbWpobXJnd2pqc2t1YmlpIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDAzNTExMCwiZXhwIjoyMDg5NjExMTEwfQ.fbJw8FyRxSFD45esE-y98tMQ93-6uc4yVID20EYiBz8'

def test(key, name):
    req = urllib.request.Request(URL, headers={'apikey': key, 'Authorization': 'Bearer ' + key})
    try:
        res = urllib.request.urlopen(req)
        print(f'{name} -> Status: {res.status}, Data: {res.read().decode("utf-8")}')
    except urllib.error.HTTPError as e:
        print(f'{name} -> HTTP {e.code}: {e.read().decode("utf-8")}')
    except Exception as e:
        print(f'{name} -> Exception: {e}')

test(ANON, 'ANON')
test(SECRET, 'SECRET')
