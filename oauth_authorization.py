from flask import Flask, request, redirect
import requests, base64, json, datetime

app = Flask(__name__)

config = json.load(open('config.json'))

@app.route(config.config['ebay']['auth_slug'])
def ebay_authorization():
    # eBay docs: https://developer.ebay.com/api-docs/static/oauth-authorization-code-grant.html
    # check if user returned from the authorization page with the code
    authCode = request.args.get('code')
    if authCode:
        # send the code to get the token
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic ' + str(base64.b64encode((config['ebay']['id'] + ':' + config['ebay']['secret']).encode("utf-8")), "utf-8")
        }
        payload = {
            'code': authCode,
            'redirect_uri': config['redirect_uri'] + config['ebay']['auth_slug'],
            'grant_type': 'authorization_code'
        }
        response = requests.post(f'https://api.sandbox.ebay.com/identity/v1/oauth2/token', data=payload, headers=headers)

        config['ebay']['access_token'] = response.json()['access_token']
        config['ebay']['refresh_token'] = response.json()['refresh_token']
        config['ebay']['best_before'] = datetime.datetime.utcnow() \
                                        + datetime.timedelta(seconds=response.json()['expires_in']) \
                                        - datetime.timedelta(seconds=300)
        json.dump(config, open('config.json', 'w'))

        return f'\n\nThe app is authorized, thank you.\n\nYou can close this tab now.'

    else:
        # send the authorization request to get the code
        redirectUrl = f'https://auth.sandbox.ebay.com/oauth2/authorize?' \
                      f'client_id={config["ebay"]["id"]}&' \
                      f'redirect_uri={config["redirect_uri"]}{config["ebay"]["auth_slug"]}&' \
                      f'response_type=code&' \
                      f'state=ProductionAuth&' \
                      f'scope={config["ebay"]["scope"]}&' \
                      f'prompt=login'
        return redirect(redirectUrl)

@app.route('/')
def hello_world():
    return 'Hello World!'
