# REST API to expose data from www.marikhpay.com
# in order for SWAGGER UI to work, need to modify the openapi.yaml file in flask/static directory accordingly

import json
import math
from flask import Flask, request
import pymysql
from config import DB_CONFIG
from flask_basicauth import BasicAuth 
from flask_swagger_ui import get_swaggerui_blueprint


app = Flask(__name__)
app.config.from_file("flask_config.json", load=json.load)
auth = BasicAuth(app)

swaggerui_blueprint = get_swaggerui_blueprint(
    base_url='/docs1',
    api_url='/static/openapi.yaml',
)
app.register_blueprint(swaggerui_blueprint)

@app.route('/mpay/<int:k_id>')
@auth.required

def profile(k_id):

    # conn = pymysql.connect(**DB_CONFIG)
    
    
    conn = pymysql.connect(
        host="IP",
        user="DB_USER", 
        password="DB_PASS",
        database="DB_NAME",
        cursorclass=pymysql.cursors.DictCursor
    )

    with conn.cursor() as cursor:
        cursor.execute("""SELECT user_id as Machine, SUM(rech_amount) as Total_Sales FROM kiosk_transaction 
                       WHERE status_code = '1000' and user_id=%s
                       ORDER BY id DESC""", (k_id, ))

    kiosk = cursor.fetchall()

    with conn.cursor() as cursor:
        cursor.execute("""SELECT telco_code as Operator, SUM(rech_amount) as Total_Sales FROM kiosk_transaction 
                        WHERE status_code = '1000' and user_id=%s
                        GROUP BY telco_code""", (k_id, ))
        operators = cursor.fetchall()

    for operator in operators:
        operator_name = operator['Operator']
        total_sales = operator['Total_Sales']

        # Add operator details to kiosk dictionary
        kiosk_details = {
            operator_name: total_sales
        }

        # Since output is list we use kiosk[0] to traget the dictionary
        kiosk[0][operator_name] = total_sales

    with conn.cursor() as cursor:
        cursor.execute("""SELECT min(datetime) first_TXN_date, max(datetime) last_TXN_date, k_code_number Owner, k_cash_code Status 
                       FROM kiosk_transaction kt 
                       JOIN kiosk_profile kp ON kt.user_id = kp.k_id 
                       WHERE kt.status_code = '1000' and kt.user_id=%s""", (k_id, ))

        min_max_date = cursor.fetchall()

        kiosk[0]['Profile'] = min_max_date
        
    return kiosk


MAX_PAGE_SIZE = 20

@app.route('/mpay')
@auth.required

def txn():
    userId = int(request.args.get('machine', 0))
    network = str(request.args.get('network', 'etisalat'))
    
    page = int(request.args.get('page', 0))
    page_size = int(request.args.get('page_size', MAX_PAGE_SIZE))
    
    page_size = min(page_size, MAX_PAGE_SIZE)

    conn = pymysql.connect(
        host="IP",
        user="DB_USER", 
        password="DB_PASS",
        database="DB_NAME",
        cursorclass=pymysql.cursors.DictCursor
    )

    with conn.cursor() as cursor:
        cursor.execute("""
                        SELECT * FROM kiosk_transaction
                        WHERE user_id = %s LIMIT %s OFFSET %s
                        """, (userId, page_size, page * page_size))
        transactions = cursor.fetchall()

        if network:
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT * FROM kiosk_transaction
                WHERE telco_code = %s and user_id = %s ORDER BY id DESC LIMIT %s OFFSET %s
            """, (network, userId, page_size, page * page_size))
            transactions = cursor.fetchall()

    with conn.cursor() as total_count:
        total_count.execute("SELECT COUNT(*) AS total_count FROM kiosk_transaction")
        total = total_count.fetchone()
        last_page = math.ceil(total['total_count'] / page_size)

    return {
        'transactions' : transactions,
        'next_page': f'/mpay?machine={userId}&page={page+1}&page_size={page_size}',
        'last_page': f'/mpay?machine={userId}&page={last_page}&page_size={page_size}',
    }