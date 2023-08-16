import requests

url = 'https://api.matt3r.ai/mdp/'

username = ''
password = ''

params = {
        'resource': 'video',
	'org_id': 'hamid',
        'k3y_id':'k3y-17700cf8',
        'start': '1691700000.34',
        'end':'1691770100.234',
        'fields':'front'
        }

response = requests.get(url, auth=(username, password), params=params)
print('status_code:', response.status_code)
print('body:', response.json())
