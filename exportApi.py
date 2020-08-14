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




app = Flask(__name__)
app.json_encoder = CustomJSONEncoder
app.debug = True

#setup db connection to sql server
def init_db():
	# CTCS
	cnxn = pyodbc.connect('DRIVER={iSeries Access ODBC Driver};QRYSTGLMT=-1;PKG=QGPL/DEFAULT(IBM),2,0,1,0,512;'\
							'LANGUAGEID=ENU;DFTPKGLIB=QGPL;DBQ=QGPL;'\
							'SYSTEM=192.168.0.6;UID=OPSCC;PWD=OPSCC21')
	cursor = cnxn.cursor()
	return cursor

@app.route('/')
def hello():
   return jsonify(name='Welcome to Export Container information Service')

############## Export ######################### 
# @app.route('/container/<container>')
# def get_nsw_exp_container_info(container):
# 	Objects = db_ctcs_exp_get_container(container)
# 	response=jsonify(Objects)
# 	response.headers.add('Access-Control-Allow-Origin', '*')
# 	return response

@app.route('/booking/<booking>')
def get_nsw_emp_booking_info(booking):
	Objects = db_ctcs_exp_get_booking(booking)
	response=jsonify(Objects)
	response.headers.add('Access-Control-Allow-Origin', '*')
	return response

# def db_ctcs_exp_get_container(container):
# 	from datetime import datetime
# 	import decimal
# 	cursor_ctcs.execute("select BTWI03,"\
# 					    "CNBT03 as full,"\
# 					    "CNHH03 as high,"\
# 					    "CNID10 as container,"\
# 					    "CNIS03 as iso,"\
# 					    "CNLL03 as size,"\
# 					    "CNTP03 as container_type,"\
# 					    "HDDT03,"\
# 					    "HDID10,"\
# 					    "LTFS02 as status,"\
# 					    "LTID02,"\
# 					    "LTSQ02,"\
# 					    "LTSR02 as direction,"\
# 					    "LYND05 as line,"\
# 					    "ORCD05 as date_in,"\
# 					    "ORCT05 as time_in,"\
# 					    "ORFS05 as status2,"\
# 					    "ORID05,"\
# 					    "OROP05 as comment,"\
# 					    "ORRF05 as booking,"\
# 					    "ORTP05 as in_by "\
# 					"from lcb1net.ctordr10 "\
# 					"where CNID10 ='" + container + "' "\
# 					"and ortp05 in ('BKG','FOT','MTI','CNA') and ORFS05 <>'CAN' "\
# 					"order by ORCD05 DESC, ORCT05 DESC")

# 	rows = cursor_ctcs.fetchall()
# 	columns = [column[0].lower() for column in cursor_ctcs.description]
# 	# print(columns, file=sys.stdout)
# 	if rows:
# 		print('Found Data', file=sys.stdout)
# 		results = []
# 		for row in rows:
# 			clean_d = { k:v.strip() for k, v in zip(columns,row) if isinstance(v, str)}
			
# 			clean_date = { k:v for k, v in zip(columns,row) if isinstance(v, decimal.Decimal)}
# 			clean_d.update(clean_date)

# 			date_in_str = str(clean_d.get("date_in", None))
# 			time_in_str = str(clean_d.get("time_in", None))
# 			date_in_date = datetime(int(date_in_str[:4]),int(date_in_str[4:6]),int(date_in_str[-2:]))
# 			if len(time_in_str) == 6 :
# 				hour_in = int(time_in_str[:2])
# 				minute_in = int(time_in_str[2:4])
# 				second_in = int(time_in_str[-2:])
# 			if len(time_in_str) == 5 :
# 				hour_in = int(time_in_str[:1])
# 				minute_in = int(time_in_str[1:3])
# 				second_in = int(time_in_str[-2:])
# 			if len(time_in_str) == 4 :
# 				hour_in = 0
# 				minute_in = int(time_in_str[:2])
# 				second_in = int(time_in_str[-2:])
# 			date_in_date=date_in_date.replace(hour=hour_in,minute=minute_in,second=second_in)
# 			clean_d.update({'datetime_in' :date_in_date })

# 			results.append(dict(clean_d))
# 		# print(results, file=sys.stdout)
# 		return results

def db_ctcs_get_pod(hdid10,cursor_ctcs):
	import decimal
	cursor_ctcs.execute("select BZID10,"\
						"VUPL02 as country,"\
						"VUPP02 as port,"\
						"VUPO02 as port_name "\
						"FROM LCB1NET.CTLTHD02 WHERE HDID10=" + hdid10 )
	row = cursor_ctcs.fetchone()
	columns = [column[0].lower() for column in cursor_ctcs.description]
	clean_d = { k:v.strip() for k, v in zip(columns,row) if isinstance(v, str)}
	clean_date = { k:v for k, v in zip(columns,row) if isinstance(v, decimal.Decimal)}
	clean_d.update(clean_date)
	# print(dict(clean_d), file=sys.stdout)
	if row != None:
		return dict(clean_d)
	return ''

def db_ctcs_get_gatein(bzid01,cursor_ctcs):
	from datetime import datetime
	import decimal
	cursor_ctcs.execute("select VMSR01 as by,VMYK01,VMID01 as license_plate,"\
						"ATAD01 as date_in,ATAT01 as time_in,VMSN01 as company "\
						"from LCB1NET.CTVSIT02 where BZID01=" + bzid01 )
	row = cursor_ctcs.fetchone()

	if row == None:
		return {}
	

	columns = [column[0].lower() for column in cursor_ctcs.description]
	clean_d = { k:v.strip() for k, v in zip(columns,row) if isinstance(v, str)}
	clean_date = { k:v for k, v in zip(columns,row) if isinstance(v, decimal.Decimal)}
	clean_d.update(clean_date)
	
	date_in_str = str(clean_d.get("date_in", None))
	time_in_str = str(clean_d.get("time_in", None))
	date_in_date = datetime(int(date_in_str[:4]),int(date_in_str[4:6]),int(date_in_str[-2:]))
	if len(time_in_str) == 6 :
		hour_in = int(time_in_str[:2])
		minute_in = int(time_in_str[2:4])
		second_in = int(time_in_str[-2:])
	if len(time_in_str) == 5 :
		hour_in = int(time_in_str[:1])
		minute_in = int(time_in_str[1:3])
		second_in = int(time_in_str[-2:])
	if len(time_in_str) == 4 :
		hour_in = 0
		minute_in = int(time_in_str[:2])
		second_in = int(time_in_str[-2:])
	date_in_date=date_in_date.replace(hour=hour_in,minute=minute_in,second=second_in)
	clean_d.update({'datetime_in' :date_in_date })
	del clean_d['time_in']
	del clean_d['date_in']
	return dict(clean_d)

	

def db_ctcs_get_load(hdid10,cursor_ctcs):
	from datetime import datetime
	import decimal
	# cursor_ctcs.execute("select HDDT03 as date_out,HDTD03 as time_out "\
	# 					"FROM LCB1NET.CTHNDL01 WHERE HDID03 =" + hdid10 )
	# row = cursor_ctcs.fetchone()
	# print(hdid10, file=sys.stdout)

	cursor_ctcs.execute("select HDDT03 as date_out,HDTD03 as time_out, "\
						"VMID01 as vessel_code,MVVA47 as vessel_name,RSUT01 as voy,TOPR01 as terminal "\
						"from LCB1DAT.loaded "\
						"where HDRA03 =" + hdid10)
	row = cursor_ctcs.fetchone()
	# print('PASS', file=sys.stdout)

	if row == None:
		return {}

	columns = [column[0].lower() for column in cursor_ctcs.description]
	clean_d = { k:v.strip() for k, v in zip(columns,row) if isinstance(v, str)}
	clean_date = { k:v for k, v in zip(columns,row) if isinstance(v, decimal.Decimal)}
	clean_d.update(clean_date)
	
	date_in_str = str(clean_d.get("date_out", None))
	time_in_str = str(clean_d.get("time_out", None))
	date_in_date = datetime(int(date_in_str[:4]),int(date_in_str[4:6]),int(date_in_str[-2:]))

	if len(time_in_str) == 6 :
		hour_in = int(time_in_str[:2])
		minute_in = int(time_in_str[2:4])
		second_in = int(time_in_str[-2:])
	if len(time_in_str) == 5 :
		hour_in = int(time_in_str[:1])
		minute_in = int(time_in_str[1:3])
		second_in = int(time_in_str[-2:])
	if len(time_in_str) == 4 :
		hour_in = 0
		minute_in = int(time_in_str[:2])
		second_in = int(time_in_str[-2:])
	if len(time_in_str) == 3 :
		hour_in = 0
		minute_in = int(time_in_str[:1])
		second_in = int(time_in_str[-2:])
	if len(time_in_str) == 2 :
		hour_in = 0
		minute_in = 0
		second_in = int(time_in_str[-2:])
	date_in_date = date_in_date.replace(hour=hour_in,minute=minute_in,second=second_in)
	clean_d.update({'datetime_out' :date_in_date })
	del clean_d['time_out']
	del clean_d['date_out']
	return dict(clean_d)
	

def get_tarriff(category='E',full='F',size='20',booking='',container=''):
	url_tariff = "http://192.168.99.100:8000/tariff/" #my Docker
	url_tariff = "http://127.0.0.1:8000/tariff/" #my local
	url_tariff = "http://10.24.50.81/tariff/" #prduction
	http = urllib3.PoolManager()
	url = f"{url_tariff}?category={category}&full={full}&size={size}&booking={booking}&container={container}"
	# print(url)
	r = http.request('GET', url)
	# print(r.data)
	return json.loads(r.data.decode('utf-8'))


def db_ctcs_get_payment(hdid10,cursor_ctcs):
	cursor_ctcs.execute("select 1 "\
						"from LCB1NET.GAGTID01  "\
						"where HDID94 =" + hdid10)
	row = cursor_ctcs.fetchone()
	# print(row)
	if row == None:
		return False
	return True

# def db_ctcs_exp_get_booking(booking):
# 	from datetime import datetime
# 	import decimal
# 	cursor_ctcs = init_db()
# 	cursor_ctcs.execute("select LTID02,HDID10,BTWI03 as cash,"\
# 						"CNBT03 as full,"\
# 						"CNHH03 as high,"\
# 						"CNID10 as container,"\
# 						"CNIS03 as iso,"\
# 						"CNLL03 as size,"\
# 						"CNTP03 as container_type,"\
# 						"HDDT03,"\
# 						"LTFS02 as status,"\
# 						"LTSQ02,"\
# 						"LTSR02 as direction,"\
# 						"LYND05 as line,"\
# 						"ORGV05 as agent,"\
# 						"ORCD05 as date_in,"\
# 						"ORCT05 as time_in,"\
# 						"ORFS05 as status2,"\
# 						"ORID05,"\
# 						"OROP05 as comment,"\
# 						"ORRF05 as booking,"\
# 						"ORTP05 as in_by, "\
# 						"VUVI02 as vessel_code,"\
# 						"VURS02 as voy "\
# 					"from lcb1net.ctordr11 "\
# 					"where orrf05='" + booking + "' "\
# 					"and ortp05 in ('BKG','FOT','MTI','CNA') and ORFS05 <>'CAN' "\
# 					"order by CNID10 ")
def db_ctcs_exp_get_booking(booking):
	from datetime import datetime
	from datetime import timedelta
	import decimal
	#import datetime
	last30day = datetime.now() - timedelta(days=30)
	last30dayStr = last30day.strftime("%Y%m%d")
	# print(last30dayStr)
	cursor_ctcs = init_db()
	cursor_ctcs.execute("select LTID02,HDID10,BTWI03 as cash,"\
						"CNBT03 as full,"\
						"CNHH03 as high,"\
						"CNID10 as container,"\
						"CNIS03 as iso,"\
						"CNLL03 as size,"\
						"CNTP03 as container_type,"\
						"HDDT03,"\
						"LTFS02 as status,"\
						"LTSQ02,"\
						"LTSR02 as direction,"\
						"LYND05 as line,"\
						"ORGV05 as agent,"\
						"ORCD05 as date_in,"\
						"ORCT05 as time_in,"\
						"ORFS05 as status2,"\
						"ORID05,"\
						"OROP05 as comment,"\
						"ORRF05 as booking,"\
						"ORTP05 as in_by, "\
						"VUVI02 as vessel_code,"\
						"VURS02 as voy "\
					"from lcb1net.ctordr11 "\
					"where orrf05='" + booking + "' "\
					"and ORCD05 > '" + last30dayStr + "' "\
					"and ortp05 in ('BKG','FOT','MTI','CNA') and ORFS05 <>'CAN' "\
					"order by CNID10 ")

	rows = cursor_ctcs.fetchall()
	columns = [column[0].lower() for column in cursor_ctcs.description]
	# print(columns, file=sys.stdout)
	if rows:
		
		# print('Found Data ' + hid , file=sys.stdout)
		results = []
		for row in rows:
			hidid = str(row[1])
			pod =  db_ctcs_get_pod(hidid,cursor_ctcs)
			load ={}
			# print(hidid)
			load = db_ctcs_get_load(hidid,cursor_ctcs)
			# print('POD Data ' + str(pod.get('bzid10',None)) , file=sys.stdout)
			truck_in = {}
			if pod.get('bzid10',None) != 0 :
				truck_in = db_ctcs_get_gatein(str(pod.get('bzid10',None)),cursor_ctcs)
			clean_d = { k:v.strip() for k, v in zip(columns,row) if isinstance(v, str)}
			
			clean_date = { k:v for k, v in zip(columns,row) if isinstance(v, decimal.Decimal)}
			clean_d.update(clean_date)

			date_in_str = str(clean_d.get("date_in", None))
			time_in_str = str(clean_d.get("time_in", None))
			date_in_date = datetime(int(date_in_str[:4]),int(date_in_str[4:6]),int(date_in_str[-2:]))
			if len(time_in_str) == 6 :
				hour_in = int(time_in_str[:2])
				minute_in = int(time_in_str[2:4])
				second_in = int(time_in_str[-2:])
			if len(time_in_str) == 5 :
				hour_in = int(time_in_str[:1])
				minute_in = int(time_in_str[1:3])
				second_in = int(time_in_str[-2:])
			if len(time_in_str) == 4 :
				hour_in = 0
				minute_in = int(time_in_str[:2])
				second_in = int(time_in_str[-2:])
			if len(time_in_str) == 3 :
				hour_in = 0
				minute_in = int(time_in_str[:1])
				second_in = int(time_in_str[-2:])
			if len(time_in_str) == 2 :
				hour_in = 0
				minute_in = 0
				second_in = int(time_in_str[-2:])
			date_in_date=date_in_date.replace(hour=hour_in,minute=minute_in,second=second_in)
			clean_d.update({'datetime_in' :date_in_date })
			clean_d.update({'pod' :pod })
			clean_d.update({'in' :truck_in })
			clean_d.update({'load' :load })

			'For tariff'
			sum_price = 0
			size = str(clean_d.get('size', '40'))
			clean_d.update({'size' :size.replace('.00','') })

			is_oog = False #is_OOG(clean_d.get('container', ''),booking)
			
			is_paid = False

			if str(clean_d.get("status", None)) == 'RGS' and clean_d.get('container', '') != '':
				full = str(clean_d.get('full', 'F'))
				
				
				size = str(clean_d.get('size', '40'))
				size= size.replace('.00','')
				# print(full,size)
				tariff = get_tarriff(full=full,size=size,
					booking=clean_d.get('booking', ''),
					container=clean_d.get('container', ''))

				if len(tariff) > 0 :
					is_oog = True if 'OOG' in str(tariff) else False
					# print(tariff)
					sum_price = sum(v for k,v in tariff.items())

				clean_d.update({'tariff' :tariff })

				# Check Payment (paid) by Chutchai on July 17,2020
				is_paid = db_ctcs_get_payment(hidid,cursor_ctcs)
				# print ('Check Payment (PAID)',is_paid)
			else :
				clean_d.update({'tariff' :{} })

			# To check PAID
			if str(clean_d.get("status", None)) == 'EXE':
				is_paid = True
			clean_d.update({'paid' :is_paid })


			# TODO -- check Continer is OOG
			
			clean_d.update({'oog' :is_oog })
			# -----------------------------
			clean_d.update({'tariff_sum_total' : 0 if is_paid else sum_price })

			# Add Terminal , on Aug 7,2020
			# Agent ,Terminal
			# XXX1 = LCB1 (B1)
			# MSC  = LCB1 (B1)
			# MSC0 = LCMT (A0)
			terminal = 'LCMT'#default value
			agent = clean_d.get('agent', '')
			if '1' in agent :
				terminal = 'LCB1'

			if agent == 'MSC' :
				terminal = 'LCB1'

			clean_d.update({'terminal' :terminal })


			results.append(dict(clean_d))

		# print(results, file=sys.stdout)
		return results



# if __name__ == "__main__":
# 	app.run(host='127.0.0.1', port=5001)
	# app.run()