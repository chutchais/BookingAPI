#!/usr/bin/python
# from flask import Flask
from flask.json import JSONEncoder
from datetime import date
from decimal import Decimal
# from flask import jsonify
# import sys
# import json
import pyodbc
# import urllib3

# FastAPI
# from typing import Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse



class CustomJSONEncoder(JSONEncoder):

    def default(self, obj):
        try:
            if isinstance(obj, date):
                return obj.isoformat()
            if isinstance(obj, Decimal):
                return str(obj)
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)



app = FastAPI()
origins = [
    "http://localhost",
    "http://localhost:5002",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# app = Flask(__name__)
# app.json_encoder = CustomJSONEncoder
# app.debug = True

#setup db connection to sql server
def init_db():
    # CTCS
    cnxn = pyodbc.connect('DRIVER={iSeries Access ODBC Driver};QRYSTGLMT=-1;PKG=QGPL/DEFAULT(IBM),2,0,1,0,512;'\
                            'LANGUAGEID=ENU;DFTPKGLIB=QGPL;DBQ=QGPL;'\
                            'SYSTEM=192.168.0.6;UID=OPSCC;PWD=OPSCC21')
    cursor = cnxn.cursor()
    return cursor


# Routing
@app.get("/")
def read_root():
    return {"name": "Welcome to Invoice information Service"}

# To get Invoice by Booking/BL
@app.get("/order/{order}")
def get_booking_invoice_info(order:str):
    print(f'Start get Invoice of {order}')
    Objects = db_ctcs_get_booking_invoice(order)
    json_compatible_item_data  = jsonable_encoder(Objects)
    return JSONResponse(content=json_compatible_item_data )

# To get Invoice by Booking/BL and Container
@app.get("/invoice/{invoice}")
def get_receive_by_invoice_info(invoice:str):
    print(f'Start get Receive of {invoice}')
    Objects = db_ctcs_get_receive_by_invoice(invoice)
    json_compatible_item_data  = jsonable_encoder(Objects)
    print(number2text('702.50','en'))
    print(number2text('702.50','th'))
    return JSONResponse(content=json_compatible_item_data )

@app.get("/invoice/{invoice}/detail")
def get_receive_by_invoice_info(invoice:str):
    print(f'Start get Receive Detail of {invoice}')
    Objects = db_ctcs_get_receivedetail_by_invoice(invoice)
    json_compatible_item_data  = jsonable_encoder(Objects)
    return JSONResponse(content=json_compatible_item_data )


# @app.get("/invoice/{invoice}")
# def get_receive_invoice_by_order(invoice:str):
#     print(f'Start get Receive of {invoice}')
#     Objects = db_ctcs_get_receive_by_invoice(invoice)
#     json_compatible_item_data  = jsonable_encoder(Objects)
#     return JSONResponse(content=json_compatible_item_data )

# # db_ctcs_get_receive_detail
# @app.get("/receive/{receive}")
# def get_receive_invoice_by_order(receive:str):
#     print(f'Start get Receive Detail of {receive}')
#     Objects = db_ctcs_get_receive_detail(receive)
#     json_compatible_item_data  = jsonable_encoder(Objects)
#     return JSONResponse(content=json_compatible_item_data )
# # End Routing




def db_ctcs_get_booking_invoice(order):
    from datetime import datetime
    from datetime import timedelta
    import decimal
    #import datetime
    
    cursor_ctcs = init_db()
    cursor_ctcs.execute("select ORRF93 as booking,"\
                        "CNID94 as container,"\
                        "NFK093 as invoice,"\
                        "DFK093 as issue_date, "\
                        "NRC093 as receive "
                    "from LCB1DAT.GAGTID02  "\
                    "where ORRF93='" + order + "' ")

    rows = cursor_ctcs.fetchall()
    columns = [column[0].lower() for column in cursor_ctcs.description]
    # print(columns, file=sys.stdout)
    if rows:
        # print('Found Data ' + hid , file=sys.stdout)
        results = []
        for row in rows:
            clean_d = { k:v.strip() for k, v in zip(columns,row) if isinstance(v, str)}
            
            clean_date = { k:v for k, v in zip(columns,row) if isinstance(v, decimal.Decimal)}
            clean_d.update(clean_date)
            results.append(dict(clean_d))

        # print(results, file=sys.stdout)
        return results



# Added on June 23,2021 -- To get Invoice and Receive info for Booking
def db_ctcs_get_receive_by_invoice(invoice):
    from datetime import datetime
    from datetime import timedelta
    import decimal
    #import datetime
    
    cursor_ctcs = init_db()
    cursor_ctcs.execute("select * "\
                    "from LCB1DAT.ETAX_INV  "\
                    "where NFK093='" + invoice + "'")

    rows = cursor_ctcs.fetchall()
    columns = [column[0].lower() for column in cursor_ctcs.description]
    # print(columns, file=sys.stdout)
    if rows:
        # print('Found Data ' + hid , file=sys.stdout)
        results = []
        for row in rows:
            clean_d = { k:v.strip() for k, v in zip(columns,row) if isinstance(v, str)}
            
            clean_date = { k:v for k, v in zip(columns,row) if isinstance(v, decimal.Decimal)}
            clean_d.update(clean_date)
            results.append(dict(clean_d))

        # print(results, file=sys.stdout)
        return results

def db_ctcs_get_receivedetail_by_invoice(invoice):
    from datetime import datetime
    from datetime import timedelta
    import decimal
    #import datetime
    
    cursor_ctcs = init_db()
    cursor_ctcs.execute("select * "\
                    "from LCB1SRC.ETAXDETAIL  "\
                    "where NFK00C='" + invoice + "'")

    rows = cursor_ctcs.fetchall()
    columns = [column[0].lower() for column in cursor_ctcs.description]
    # print(columns, file=sys.stdout)
    if rows:
        # print('Found Data ' + hid , file=sys.stdout)
        results = []
        for row in rows:
            clean_d = { k:v.strip() for k, v in zip(columns,row) if isinstance(v, str)}
            
            clean_date = { k:v for k, v in zip(columns,row) if isinstance(v, decimal.Decimal)}
            clean_d.update(clean_date)
            results.append(dict(clean_d))

        # print(results, file=sys.stdout)
        return results

def number2text(number_str:str,lang:str):
    from num2words import num2words
    if lang=='en':
        number_arry = number_str.split('.')
        baht_str = f"{num2words(number_arry[0],lang='en')} BAHT"
        if len(number_arry)>1:
            satang_str = f"AND {num2words(number_arry[1],lang='en')} STANG"
        str = f'{baht_str} {satang_str}'
    else :
        str = num2words(number_str,to='currency',lang='th')
    return str.upper()

#    cursor_ctcs.execute("select ORRF93 as booking,"\
#                         "CNID94 as container,"\
#                         "CNLL94 as container_size,"\
#                         "CNBT94 as container_full,"
#                         "NFK093 as invoice,"\
#                         "NRC093 as receive,"\
#                         "AMNT94 as amount,"\
#                         "CUIDD9 as customer_code,"\
#                         "RKNMD9 as customer_name,"\
#                         "DRSTD9 as customer_addr1,"\
#                         "DRWPD9 as customer_addr2,"\
#                         "DRLDD9 as customer_branch,"\
#                         "DRPSD9 as customer_tax,"\
#                         "DFK093 as issue_date "\

# def db_ctcs_get_receive_detail(receive):
#     from datetime import datetime
#     from datetime import timedelta
#     import decimal
#     #import datetime
    
#     cursor_ctcs = init_db()
#     cursor_ctcs.execute("SELECT * "\
#                     "FROM LCB1SRC.ETAXDETAIL WHERE NFK00C = '" + receive +"'")

#     rows = cursor_ctcs.fetchall()
#     columns = [column[0].lower() for column in cursor_ctcs.description]
#     # print(columns, file=sys.stdout)
#     if rows:
#         # print('Found Data ' + hid , file=sys.stdout)
#         results = []
#         for row in rows:
#             clean_d = { k:v.strip() for k, v in zip(columns,row) if isinstance(v, str)}
            
#             clean_date = { k:v for k, v in zip(columns,row) if isinstance(v, decimal.Decimal)}
#             clean_d.update(clean_date)
#             results.append(dict(clean_d))

#         # print(results, file=sys.stdout)
#         return results

