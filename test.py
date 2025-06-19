import os
import requests

token_response = requests.post(
    os.environ['AICORE_AUTH_URL'] + "/oauth/token",
    data={"grant_type": "client_credentials"},
    auth=(os.environ['AICORE_CLIENT_ID'], os.environ['AICORE_CLIENT_SECRET']),
)
token = token_response.json()["access_token"]

headers = {
    "Authorization": f"Bearer {token}",
}
url = f"{os.environ['AICORE_BASE_URL']}/deployments"
resp = requests.get(url, headers=headers)
print("Deployments:", resp.status_code, resp.text)