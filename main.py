import requests, datetime, json, base64, os, traceback, logging
from sqlalchemy import create_engine, text, MetaData, Table
from woocommerce import API


def get_token(platform):
    # eBay docs: https://developer.ebay.com/api-docs/static/oauth-refresh-token-request.html
    # Amazon docs: https://github.com/amzn/selling-partner-api-docs/blob/main/guides/en-US/developer-guide/SellingPartnerApiDeveloperGuide.md#step-1-request-a-login-with-amazon-access-token

    # Update the access_token if it's expired
    if datetime.datetime.utcnow() > datetime.datetime.fromisoformat(config[platform]['best_before']):
        url = config[platform]['refresh_url']
        if platform == 'amazon':
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
            }
            data = {
                'grant_type': 'refresh_token',
                'refresh_token': config[platform]['refresh_token'],
                'client_id': config[platform]['id'],
                'client_secret': config[platform]['secret']
            }
        else:
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic ' + str(
                    base64.b64encode((config[platform]['id'] + ':' + config[platform]['secret']).encode('utf-8')), 'utf-8')
            }
            data = {
                'grant_type': 'refresh_token',
                'refresh_token': config[platform]['refresh_token'],
                'scope': config[platform]['scope']
            }
        response = requests.post(url, headers=headers, data=data)
        config[platform]['access_token'] = response.json()['access_token']
        config[platform]['refresh_token'] = response.json().get('refresh_token', config[platform]['refresh_token'])
        config[platform]['best_before'] = (datetime.datetime.utcnow()
                                           + datetime.timedelta(seconds=response.json()['expires_in'])
                                           - datetime.timedelta(seconds=300)).isoformat()

        # Save the new credentials
        json.dump(config, open('config.json', 'w'))

    return config[platform]['access_token']

config = json.load(open('config.json'))

# define the application path and create the logging object
application_path = os.path.abspath(os.path.dirname(__file__))
logs_folder = 'logs for the last 20 days'
if logs_folder not in os.listdir(application_path):
    os.mkdir(logs_folder)

logName = os.path.join(application_path, logs_folder, 'log ' + datetime.datetime.now().strftime('%Y-%m-%d') + '.txt')
logging.basicConfig(filename=logName, level=logging.INFO, format=' %(asctime)s -  %(levelname)s -  %(message)s')

# Remove logs older than 20 days
for fileName in os.listdir(os.path.join(application_path, logs_folder)):
    try:
        logDate = datetime.datetime.strptime(fileName[4:-4], '%Y-%m-%d')
        if logDate < (datetime.datetime.now() - datetime.timedelta(days=10)):
            os.remove(os.path.join(os.path.join(application_path, logs_folder), fileName))
    except:
        continue

try:
    # Create connection to the MySQL database
    engine = create_engine(f"mysql+pymysql://{config['mysql']['user']}:"
                           f"{config['mysql']['password']}@"
                           f"{config['mysql']['host']}:"
                           f"{config['mysql']['port']}/"
                           f"{config['mysql']['database']}")

    # Create the new tables if they don't already exist
    # Orders
    with engine.connect() as connection:
        connection.execute(text(
            f"""CREATE TABLE IF NOT EXISTS orders
            (
            id INT NOT NULL AUTO_INCREMENT,
            order_id VARCHAR(32),
            platform VARCHAR(8),
            creation_date VARCHAR(32),
            customer_name VARCHAR(128),
            
            subtotal_amount DECIMAL(9,2),
            discount_amount DECIMAL(9,2),
            delivery_amount DECIMAL(9,2),
            tax_amount DECIMAL(9,2),
            total_amount DECIMAL(9,2),
        
            PRIMARY KEY (id)
            );"""
        ))
        # Line items
        connection.execute(text(
            f"""CREATE TABLE IF NOT EXISTS line_items
            (
            id INT NOT NULL AUTO_INCREMENT,
            line_id VARCHAR(32),
            order_id VARCHAR(32),
            sku VARCHAR(64),
            title VARCHAR(256),
            quantity SMALLINT,
            total_amount DECIMAL(9,2),
            PRIMARY KEY (id)
            );"""
        ))
except:
    logging.error(traceback.format_exc())
    raise Exception(traceback.format_exc())

ordersToInsert = []
lineItemsToInsert = []

# eBay orders
try:
    # Get the list of eBay order IDs from the database to know when to stop pagination
    with engine.connect() as connection:
        result = connection.execute(text("SELECT order_id FROM orders WHERE platform='ebay';"))
    orderIds = list(row['order_id'] for row in result.fetchall())

    # Collect eBay orders from Fulfillment API
    orders = []
    # API docs: https://developer.ebay.com/api-docs/sell/fulfillment/resources/order/methods/getOrders
    url = 'https://api.ebay.com/sell/fulfillment/v1/order'
    headers = {
        'Authorization': 'Bearer ' + get_token('ebay')
    }
    response = requests.get(url, headers=headers)
    mayBeMoreOrders = True
    for order in response.json()['orders']:
        # Include only those orders not found in the database
        if order['orderId'] in orderIds:
            mayBeMoreOrders = False
            break
        else:
            mayBeMoreOrders = True
            orders.append(order)

    # Continue getting the rest of the orders if there is a next page
    while response.json().get('next') and mayBeMoreOrders:
        url = response.json().get('next')
        headers = {
            'Authorization': 'Bearer ' + get_token('ebay')
        }
        response = requests.get(url, headers=headers)
        for order in response.json()['orders']:
            # Include only those orders not found in the database
            if order['orderId'] in orderIds:
                mayBeMoreOrders = False
                break
            else:
                mayBeMoreOrders = True
                orders.append(order)

    # For each order, create the dictionary in the destination table format
    for order in orders:
        ordersToInsert.append({
            'order_id': str(order['orderId']),
            'platform': 'ebay',
            'creation_date': order['creationDate'].strip('Z'),  # saves UTC ISO timestamp, example: 2015-08-04T19:09:02.768
            'customer_name': order['buyer']['username'][:128],
            'subtotal_amount': order['pricingSummary'].get('priceSubtotal', 0),
            'discount_amount': order['pricingSummary'].get('priceDiscountSubtotal', 0),
            'delivery_amount': order['pricingSummary'].get('deliveryCost', 0),
            'tax_amount': order['pricingSummary'].get('tax', 0),
            'total_amount': order['pricingSummary'].get('total', 0),
        })
        for item in order['lineItems']:
            lineItemsToInsert.append({
                'line_id': str(item['lineItemId']),
                'order_id': order['orderId'],
                'sku': item.get('sku', ''),
                'title': item['title'][:256],
                'quantity': item['quantity'],
                'total_amount': float(item['total']['value']),
            })
except:
    logging.error(traceback.format_exc())
    print(traceback.format_exc())

# Amazon orders
try:
    # How to register a private app: https://github.com/amzn/selling-partner-api-docs/blob/main/guides/en-US/developer-guide/SellingPartnerApiDeveloperGuide.md
    # Get the list of Amazon order IDs from the database to know when to stop pagination
    with engine.connect() as connection:
        result = connection.execute(text("SELECT order_id FROM orders WHERE platform='amazon';"))
    orderIds = list(row['order_id'] for row in result.fetchall())

    # Collect Amazon orders from Orders API
    orders = []
    # API docs: https://github.com/amzn/selling-partner-api-docs/blob/main/references/orders-api/ordersV0.md#getorders
    # How to get refresh token: https://github.com/amzn/selling-partner-api-docs/blob/main/guides/en-US/developer-guide/SellingPartnerApiDeveloperGuide.md#Self-authorization
    url = 'https://sellingpartnerapi-na.amazon.com/orders/v0/orders'
    headers = {
        'Authorization': 'Bearer ' + get_token('amazon')
    }
    # response = requests.get(url, headers=headers)
    # mayBeMoreOrders = True
    # for order in response.json()['orders']:
    #     # Include only those orders not found in the database
    #     if order['orderId'] in orderIds:
    #         mayBeMoreOrders = False
    #         break
    #     else:
    #         mayBeMoreOrders = True
    #         orders.append(order)
    #
    # # Continue getting the rest of the orders if there is a next page
    # while response.json().get('next') and mayBeMoreOrders:
    #     url = response.json().get('next')
    #     headers = {
    #         'Authorization': 'Bearer ' + get_token('ebay')
    #     }
    #     response = requests.get(url, headers=headers)
    #     for order in response.json()['orders']:
    #         # Include only those orders not found in the database
    #         if order['orderId'] in orderIds:
    #             mayBeMoreOrders = False
    #             break
    #         else:
    #             mayBeMoreOrders = True
    #             orders.append(order)
except:
    logging.error(traceback.format_exc())
    print(traceback.format_exc())

# WooCommerce orders
try:
    # Get the list of eBay order IDs from the database to know when to stop pagination
    with engine.connect() as connection:
        result = connection.execute(text("SELECT order_id FROM orders WHERE platform='wc';"))
    orderIds = list(row['order_id'] for row in result.fetchall())

    # How to get the keys: https://docs.woocommerce.com/document/woocommerce-rest-api/
    wcapi = API(
        url=config['wc']['store url'],
        consumer_key=config['wc']['consumer_key'],
        consumer_secret=config['wc']['consumer_secret'],
        wp_api=True,
        version="wc/v3",
        query_string_auth=True
        )

    # API docs: https://woocommerce.github.io/woocommerce-rest-api-docs/?python#list-all-orders
    i = 0
    orders = []
    mayBeMoreOrders = True
    while mayBeMoreOrders:
        i += 1
        response = wcapi.get("orders", params={'per_page': 100, 'page': i}).json()
        for order in response:
            # Include only those orders not found in the database
            if order['number'] in orderIds:
                mayBeMoreOrders = False
                break
            else:
                mayBeMoreOrders = True
                orders.append(order)

        # If less than 100 orders is returned, this is the last page
        if len(response) < 100:
            mayBeMoreOrders = False

    # For each order, create the dictionary in the destination table format
    for order in orders:
        discount = float(order['discount_total'])
        delivery = float(order['shipping_total'])
        tax = float(order['total_tax'])
        total = float(order['total'])
        subtotal = (total * 100 - tax * 100 - delivery * 100 + discount * 100) / 100  # calculating in integer numbers to avoid rounding errors
        ordersToInsert.append({
            'order_id': order['number'],
            'platform': 'wc',
            'creation_date': order['date_created_gmt'].strip('Z'),  # saves UTC ISO timestamp, example: 2015-08-04T19:09:02
            'customer_name': str(order['customer_id'])[:128],
            'subtotal_amount': subtotal,
            'discount_amount': discount,
            'delivery_amount': delivery,
            'tax_amount': tax,
            'total_amount': total,
        })
        for item in order['line_items']:
            lineItemsToInsert.append({
                'line_id': str(item['id']),
                'order_id': order['number'],
                'sku': item.get('sku', ''),
                'title': item['name'][:256],
                'quantity': item['quantity'],
                'total_amount': float(item['total']),
            })
except:
    logging.error(traceback.format_exc())
    print(traceback.format_exc())

try:
    # Insert the orders and line items from all three platforms
    meta = MetaData()
    ordersTable = Table('orders', meta, autoload=True, autoload_with=engine)
    lineItemsTable = Table('line_items', meta, autoload=True, autoload_with=engine)
    with engine.connect() as connection:
        connection.execute(ordersTable.insert(), ordersToInsert)
        connection.execute(lineItemsTable.insert(), lineItemsToInsert)
except:
    logging.error(traceback.format_exc())
    print(traceback.format_exc())
