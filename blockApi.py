#!/usr/bin/python
from flask import Flask
from flask.json import JSONEncoder
from datetime import date
from decimal import Decimal
from flask import jsonify
import sys
import json
import pyodbc
import urllib3

# FastAPI
from typing import Optional
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
    "http://localhost:5003",
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


@app.get("/")
def read_root():
    return {"name": "Welcome to Block information Service"}

############## Export ######################### 
# @app.route('/order/<order>')
@app.get("/block/{container}")
def get_booking_invoice_info(container:str):
    print('Start get Blocking infomation')
    Objects = db_ctcs_get_blocking(container)
    json_compatible_item_data  = jsonable_encoder(Objects)
    # response=jsonify(Objects)
    # response.headers.add('Access-Control-Allow-Origin', '*')
    return JSONResponse(content=json_compatible_item_data )

def db_ctcs_get_blocking(container):
    from datetime import datetime
    from datetime import timedelta
    import decimal
    #import datetime
    
    cursor_ctcs = init_db()
    cursor_ctcs.execute("select TRMC45 as terminal,"\
                        "BHRS45 as reason,"\
                        "ICID45 as vessel_code,"\
                        "IVOY45 as voy,"\
                        "ILIN45 as service,"\
                        "CHGD45 as issue_date,"\
                        "CHGT45 as issue_time "\
                    "from LCB1DAT.CONTBLOCK "\
                    "where CNID45='" + container + "' ")

    rows = cursor_ctcs.fetchall()
    columns = [column[0].lower() for column in cursor_ctcs.description]
    # print(columns, file=sys.stdout)
    if rows:
        # print('Found Data ' + hid , file=sys.stdout)
        results = []
        for row in rows:
            clean_d = { k:v.strip() for k, v in zip(columns,row) if isinstance(v, str)}
            
            # clean_date = { k:v for k, v in zip(columns,row) if isinstance(v, decimal.Decimal)}
            # clean_d.update(clean_date)
            results.append(dict(clean_d))

        # print(results, file=sys.stdout)
        return results



# if __name__ == "__main__":
# 	app.run(host='127.0.0.1', port=5001)
    # app.run()