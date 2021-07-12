from flask import Flask, request, redirect
import requests, base64, json, datetime, os, traceback, logging

app = Flask(__name__)

# define the application path and create the logging object
application_path = os.path.abspath(os.path.dirname(__file__))
logs_folder = 'logs for the last 20 days'
if logs_folder not in os.listdir(application_path):
    os.mkdir(os.path.join(application_path, logs_folder))

config = json.load(open(os.path.join(application_path, 'config.json')))

@app.route(config['ebay']['auth_slug'])
def ebay_authorization():
    logName = os.path.join(application_path, logs_folder, 'log ' + datetime.datetime.now().strftime('%Y-%m-%d') + '.txt')
    logging.basicConfig(filename=logName, level=logging.INFO, format=' %(asctime)s -  %(levelname)s -  %(message)s')

    # eBay docs: https://developer.ebay.com/api-docs/static/oauth-authorization-code-grant.html
    # check if user returned from the authorization page with the code
    authCode = request.args.get('code')
    if authCode:
        try:
            responseStr = ''
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
            response = requests.post(f'https://api.ebay.com/identity/v1/oauth2/token', data=payload, headers=headers)

            responseStr = str(response.json())
            config['ebay']['access_token'] = response.json()['access_token']
            config['ebay']['refresh_token'] = response.json()['refresh_token']
            config['ebay']['best_before'] = (datetime.datetime.utcnow()
                                             + datetime.timedelta(seconds=int(response.json()['expires_in']))
                                             - datetime.timedelta(seconds=300)).isoformat()
            json.dump(config, open('config.json', 'w'))

            return f'\n\nThe app is authorized, thank you.\n\nYou can close this tab now.'
        except:
            logging.error(traceback.format_exc())
            logging.error(responseStr)
            raise Exception(traceback.format_exc())
    else:
        # send the authorization request to get the code
        redirectUrl = f'https://auth.ebay.com/oauth2/authorize?' \
                      f'client_id={config["ebay"]["id"]}&' \
                      f'redirect_uri={config["redirect_uri"]}{config["ebay"]["auth_slug"]}&' \
                      f'response_type=code&' \
                      f'state=ProductionAuth&' \
                      f'scope={config["ebay"]["scope"]}&' \
                      f'prompt=login'
        return redirect(redirectUrl)


@app.route('/')
def hello_world():
    return 'Hello World, everyone!'

if __name__ == '__main__':
    app.run()
