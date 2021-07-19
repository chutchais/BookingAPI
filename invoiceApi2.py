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
    
    return JSONResponse(content=json_compatible_item_data )

@app.get("/invoice/{invoice}/detail")
def get_receive_by_invoice_info(invoice:str):
    print(f'Start get Receive Detail of {invoice}')
    Objects = db_ctcs_get_receivedetail_by_invoice(invoice)
    json_compatible_item_data  = jsonable_encoder(Objects)
    return JSONResponse(content=json_compatible_item_data )

def remove_dupe_dicts(l):
  return [
    dict(t) 
    for t in {
      tuple(d.items())
      for d in l
    }
  ]


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
            
            # Modify on June 24,2021 -- To fix receive number consist .0
            clean_date = { k:v for k, v in zip(columns,row) if k=='issue_date'}
            clean_d.update(clean_date)
            clean_receive = { k:str(v) for k, v in zip(columns,row) if k=='receive'}
            clean_d.update(clean_receive)
            results.append(dict(clean_d))

        # print(results, file=sys.stdout)
        return results



# Added on June 23,2021 -- To get Invoice and Receive info for Booking
def db_ctcs_get_receive_by_invoice(invoice):
    from datetime import datetime
    from datetime import timedelta
    import decimal

    cursor_ctcs = init_db()
    cursor_ctcs.execute("select ORRF93 booking,NFK093 invoice,NRC093 receive,"\
                    "DFK093 issue_date,"\
                    "CUIDD9 customer_code,RKNMD9 addr1,RKN2D9 addr2,DRSTD9 addr3,DRWPD9 addr4,"\
                    "DRPSD9 tax,DRLDD9 branch,"\
                    "CNID94 container,CNLL94 size,"\
                    "HDDT94 paid_until,FRMK93 terminal "
                    "from LCB1DAT.ETAX_INV  "\
                    "where NFK093='" + invoice + "'")
    
    # cursor_ctcs.execute("select * "
    #                 "from LCB1DAT.ETAX_INV  "\
    #                 "where NFK093='" + invoice + "'")

    rows = cursor_ctcs.fetchall()
    columns = [column[0].lower() for column in cursor_ctcs.description]
    # print(columns, file=sys.stdout)
    if rows:
        # print('Found Data ' + hid , file=sys.stdout)
        results = []
        containers = []
        for row in rows:
            # print(row)
            clean_d = { k:v.strip() for k, v in zip(columns,row) if isinstance(v, str)}
            # print(clean_d)
            clean_date = { k:str(v) for k, v in zip(columns,row) if isinstance(v, decimal.Decimal)}
            clean_size= { k:int(v) for k, v in zip(columns,row) if k=='size'}

            clean_d.update(clean_date)
            clean_d.update(clean_size)
            results.append(dict(clean_d))
            containers.append({'container':clean_d['container'],
                                'booking':clean_d['booking'],
                                'size':clean_d['size']})
            # break

        containers = remove_dupe_dicts(containers)
        from operator import itemgetter
        containers_sorted = sorted(containers, key=itemgetter('size'))
        # containers_sorted = sorted(containers, key=itemgetter('container'))
        # print(newlist)
        results[0]['containers']=containers_sorted
        results[0]['charges']=db_ctcs_get_receivedetail_by_invoice(invoice)
        return results[0]

def db_ctcs_get_receivedetail_by_invoice(invoice):
    from datetime import datetime
    from datetime import timedelta
    import decimal
    #import datetime
    
    cursor_ctcs = init_db()
    # cursor_ctcs.execute("select IDTXUD tariff_name1 ,IDT3UD tarliff_name2,"\
    #                 "sum(AEHD0C) qty,max(TARF0C) unit_price,sum(TAR10C) amount,max(MTKD0C) currency,"\
    #                 "max(BOF10C) total_charge,max(BBT10C) vat,max(BTF00C) grand_total,max(USPCUF) user "
    #                 "from LCB1SRC.ETAXDETAIL  "\
    #                 "where NFK00C='" + invoice + "' and EENH0C='CNT' "\
    #                 "group by IDTXUD,IDT3UD,TARF0C")

# Comment on July 19,2021 -- Found Tariff details show all 
    # cursor_ctcs.execute("select t.tariff_name1 ,t.tariff_name2,"\
    #                     "sum(t.qty) qty ,max(t.unit_price) unit_price,sum(t.qty)*max(t.unit_price) amount,"\
    #                     "max(t.currency) currency,max(t.total_charge) total_charge,max(t.vat) vat,max(t.grand_total) grand_total,"\
    #                     "max(t.user) user "\
    #                     "from ("\
    #                     "select IDTXUD tariff_name1 ,IDT3UD tariff_name2, "\
    #                         "max(AEHD0C) qty,max(TARF0C) unit_price,'0' amount,max(MTKD0C) currency,"\
    #                         "max(BOF10C) total_charge,max(BBT10C) vat,max(BTF00C) grand_total,max(USPCUF) user,VFID0C "\
    #                         "from LCB1SRC.ETAXDETAIL  "\
    #                         "where NFK00C='" + invoice + "' "\
    #                         "group by VFID0C,IDTXUD,IDT3UD,TARF0C "\
    #                     ") t "\
    #                     "group by t.tariff_name1,t.tariff_name2,t.unit_price,VFID0C "\
    #                     "order by t.tariff_name1,t.tariff_name2,t.unit_price")

    # PASSED verify---
    # cursor_ctcs.execute("select t.tariff_name1 ,t.tariff_name2,"\
    #                     "sum(t.qty) qty ,max(t.unit_price) unit_price,sum(t.qty)*max(t.unit_price) amount,"\
    #                     "max(t.currency) currency,max(t.total_charge) total_charge,max(t.vat) vat,max(t.grand_total) grand_total,"\
    #                     "max(t.user) user "\
    #                     "from ("\
    #                     "select IDTXUD tariff_name1 ,IDT3UD tariff_name2, "\
    #                         "sum(AEHD0C) qty,max(TARF0C) unit_price,'0' amount,max(MTKD0C) currency,"\
    #                         "max(BOF10C) total_charge,max(BBT10C) vat,max(BTF00C) grand_total,max(USPCUF) user "\
    #                         "from LCB1SRC.ETAXDETAIL  "\
    #                         "where NFK00C='" + invoice + "' "\
    #                         "group by IDTXUD,IDT3UD,TARF0C "\
    #                     ") t "\
    #                     "group by t.tariff_name1,t.tariff_name2,t.unit_price "\
    #                     "order by t.tariff_name1,t.tariff_name2,t.unit_price")

    # Modify on July 19,2021 -- To fix chareges ahow all record.
    cursor_ctcs.execute("select IDTXUD tariff_name1 ,IDT3UD tariff_name2, "\
                            "sum(AEHD0C) qty,max(TARF0C) unit_price,sum(AEHD0C)*max(TARF0C) amount,max(MTKD0C) currency,"\
                            "max(BOF10C) total_charge,max(BBT10C) vat,max(BTF00C) grand_total,max(USPCUF) user "\
                            "from LCB1SRC.ETAXDETAIL  "\
                            "where NFK00C='" + invoice + "' "\
                            "group by IDTXUD,IDT3UD,TARF0C "\
                            "order by IDTXUD,IDT3UD,TARF0C desc")
    
    rows = cursor_ctcs.fetchall()
    
    columns = [column[0].lower() for column in cursor_ctcs.description]
    # print(columns, file=sys.stdout)
    if rows:
        results = []
        for row in rows:
            clean_d = { k:v.strip() for k, v in zip(columns,row) if isinstance(v, str)}
            clean_date = { k:v for k, v in zip(columns,row) if isinstance(v, decimal.Decimal)}
            clean_d.update(clean_date)
            results.append(dict(clean_d))

        # Check Is it IMPORT?? , if yes ,we must have STORAGE charge

        total_text_en = number2text(str(results[0]['grand_total']),'en')
        total_text_th = number2text(str(results[0]['grand_total']),'th')
        final_result={'currency':results[0]['currency'],
                    'total_charge':results[0]['total_charge'],
                    'vat':results[0]['vat'],
                    'grand_total':results[0]['grand_total'],
                    'grand_text_en':total_text_en,
                    'grand_text_thai':total_text_th,
                    'user':results[0]['user'] }

        final_result['details'] = reorder_tariff(results)
        return final_result

def reorder_tariff(tariffs):
    # Edit on July 7,2021 -- In case there is no 'LIFT' in tarliff
    found = False
    for tariff in tariffs:
        if 'LIFT' in tariff['tariff_name1']:
            found = True

    if not found :
        return tariffs

    # for tariff in tariffs:
    new_tariff =[]
    tariff_ix = 0
    # Find tariff that consist of 'LIFT'
    for i, tariff in enumerate(tariffs):
        if 'LIFT' in tariff['tariff_name1']:
            new_tariff.append(tariff)
            tariff_ix=i
            break

    
    for i, tariff in enumerate(tariffs):
        if i != tariff_ix:
            new_tariff.append(tariff)
    return new_tariff

def number2text(number_str:str,lang:str):
    from num2words import num2words
    if lang=='en':
        number_arry = number_str.split('.')
        baht_str = f"{num2words(number_arry[0],lang='en')} BAHT"
        if len(number_arry)>1:
            # print (number_arry[1])
            satang_str = f" AND {num2words(number_arry[1],lang='en')} STANG"
            satang_str = '' if (number_arry[1] == '00' or number_arry[1] == '0' or number_arry[1] == '') else satang_str
        str = f'{baht_str}{satang_str} ONLY'
    else :
        str = num2words(number_str,to='currency',lang='th')
    return str.upper()
