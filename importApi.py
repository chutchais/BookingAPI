#!/usr/bin/python
from flask import Flask
from flask.json import JSONEncoder
from datetime import date
from decimal import Decimal
from flask import jsonify
import sys
import json
import pyodbc
import redis



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

# Added on March 3,2021 -- To enable cach (redis)
db = redis.StrictRedis('localhost', 6379,db=1, charset="utf-8", decode_responses=True) #Production

#setup db connection to sql server
def init_db():
	# CTCS
	cnxn = pyodbc.connect('DRIVER={iSeries Access ODBC Driver};QRYSTGLMT=-1;PKG=QGPL/DEFAULT(IBM),2,0,1,0,512;'\
							'LANGUAGEID=ENU;DFTPKGLIB=QGPL;DBQ=QGPL;'\
							'SYSTEM=192.168.0.6;UID=OPSCC;PWD=OPSCC21')
	cursor = cnxn.cursor()
	# NSW - Export
	cn_nsw = pyodbc.connect('DRIVER=SQL Server;UID=goods;PWD=password;Address=192.168.10.53,1433;Network=DBMSSOCN;'\
				'DATABASE=GoodsTransit;WSID=NB_SECMGR;APP=Microsoft® Windows® Operating System;'\
				'SERVER=192.168.10.53;Description=goodtransit')
	cursor_nsw = cn_nsw.cursor()

	return cursor,cursor_nsw

# cursor_ctcs,cursor_nsw = init_db()

@app.route('/')
def hello():
   return jsonify(name='Welcome to Import Container information Service')



############## Import ######################### 
@app.route('/container/<container>')
def get_imp_container_info(container):
	# Added on March 3,2021 -- To read from cache
	from datetime import datetime
	now = datetime.now() # current date and time
	date_time = now.strftime("%Y-%m-%d %H:%M:%S")
	key =f'{container}'
	container_data = db.get(key)
	if container_data:
		# if found then return
		print(f'{date_time} Getting Container :{container} from cache')
		Objects = json.loads(container_data)
	else:
		print(f'{date_time} Getting Container :{container}')
		cursor_ctcs,cursor_nsw = init_db()
		Objects = db_nsw_imp_get_container(cursor_ctcs,cursor_nsw,container,'full')
		# save db
		ttl=60*3 #3 mins
		db.set(container,json.dumps(Objects,cls=CustomJSONEncoder))
		db.expire(container, ttl)

	
	response=jsonify(Objects)
	response.headers.add('Access-Control-Allow-Origin', '*')
	return response

# @app.route('/container/<container>/<voy>/discharge')
# def db_ctcs_imp_get_discharge_info(container,voy):
# 	Objects = db_ctcs_imp_get_discharge_info(container,voy)
# 	response=jsonify(Objects)
# 	response.headers.add('Access-Control-Allow-Origin', '*')
# 	return response

# @app.route('/container/<container>/<handleid>/out')
# def db_ctcs_imp_get_out_info(container,handleid):
# 	Objects = db_ctcs_imp_get_out_info(container,handleid)
# 	response=jsonify(Objects)
# 	response.headers.add('Access-Control-Allow-Origin', '*')
# 	return response

@app.route('/bl/<bl>')
def get_nsw_imp_bl_info(bl):
	# Added on March 3,2021 -- To read from cache
	from datetime import datetime
	now = datetime.now() # current date and time
	date_time = now.strftime("%Y-%m-%d %H:%M:%S")
	key =f'{bl}'
	bl_data = db.get(key)
	if bl_data:
		# if found then return
		print(f'{date_time} Getting BL :{bl} from cache')
		Objects = json.loads(bl_data)
	else:
		print(f'{date_time} Getting BL :{bl}')
		cursor_ctcs,cursor_nsw = init_db()
		Objects = db_nsw_imp_get_bl(bl)
		# save db
		ttl=60*3 #3 mins
		db.set(bl,json.dumps(Objects,cls=CustomJSONEncoder))
		db.expire(bl, ttl)
	
	response=jsonify(Objects)
	response.headers.add('Access-Control-Allow-Origin', '*')
	return response


def query_execute(cursor_ctcs,number,voy,mode='full'):
	for retry in range(3):
		try :
			# print ('Execute %s' % retry)
			if mode =='full':
				cursor_ctcs.execute("select "\
							"CNID03 as container,"\
							"CNBT03 as full,CNHH03 as high,CNLL03 as size,CNTP03 as container_type,CNIS03 as iso,"\
							"CNSK03 as container_status,"\
							"RSIN01 as voy_in,RSUT01 as voy_out,"
							"LYND05 as line,"\
							"ORGV05 as agent,"\
							"MVV447 as callsign,"\
							"MVVA47 as vessel_name,"\
							"VMID01 as vessel_code,"\
							"HDDT03 as date_in,HDTD03 as time_in,"\
							"TOPR01 as terminal,HDRA03 "\
							"FROM S2114C2V.LCB1DAT.DISCHARGE DISCHARGE "\
							"where DISCHARGE.CNID03 = '"+ number +"' and RSIN01='"+ voy + "' "\
							"order by DISCHARGE.HDDT03 desc")

			else:
				cursor_ctcs.execute("select "\
							"CNID03 as container,"\
							"MVVA47 as vessel_name,"\
							"VMID01 as vessel_code,"\
							"CNLL03 as size,"\
							"HDDT03 as date_in,HDTD03 as time_in,"\
							"TOPR01 as terminal,HDRA03 "\
							"FROM S2114C2V.LCB1DAT.DISCHARGE DISCHARGE "\
							"where DISCHARGE.CNID03 = '"+ number +"' and RSIN01='"+ voy + "' "\
							"order by DISCHARGE.HDDT03 desc")
			# print ('Fetch all ROW')
			rows = cursor_ctcs.fetchall()
			break
		except :
			rows = None
			print ('Execute %s : ERROR' % retry)
			continue
		# 	# print ('DB error --- Reconnect %s ' % retry)
		# 	# cursor_ctcs,cursor_nsw = init_db()
		# 	# continue
		# 	return None
	# 		print ('DB error --- Reconnect')
	# 		cnxn = pyodbc.connect('DRIVER={iSeries Access ODBC Driver};QRYSTGLMT=-1;PKG=QGPL/DEFAULT(IBM),2,0,1,0,512;'\
	# 						'LANGUAGEID=ENU;DFTPKGLIB=QGPL;DBQ=QGPL;'\
	# 						'SYSTEM=192.168.0.6;UID=OPSCC;PWD=OPSCC21')
	# 		cursor_ctcs = cnxn.cursor()
	# 		continue

	return rows


def db_ctcs_imp_get_discharge_info(cursor_ctcs,number,voy,mode='full'):
	from datetime import datetime
	import decimal
	# if mode =='full':
	# 	cursor_ctcs.execute("select "\
	# 				"CNID03 as container,"\
	# 				"CNBT03 as full,CNHH03 as high,CNLL03 as size,CNTP03 as container_type,CNIS03 as iso,"\
	# 				"CNSK03 as container_status,"\
	# 				"RSIN01 as voy_in,RSUT01 as voy_out,"
	# 				"LYND05 as line,"\
	# 				"ORGV05 as agent,"\
	# 				"MVV447 as callsign,"\
	# 				"MVVA47 as vessel_name,"\
	# 				"VMID01 as vessel_code,"\
	# 				"HDDT03 as date_in,HDTD03 as time_in,"\
	# 				"TOPR01 as terminal,HDRA03 "\
	# 				"FROM S2114C2V.LCB1DAT.DISCHARGE DISCHARGE "\
	# 				"where DISCHARGE.CNID03 = '"+ number +"' and RSIN01='"+ voy + "' "\
	# 				"order by DISCHARGE.HDDT03 desc")

	# else:
	# 	cursor_ctcs.execute("select "\
	# 				"CNID03 as container,"\
	# 				"MVVA47 as vessel_name,"\
	# 				"VMID01 as vessel_code,"\
	# 				"CNLL03 as size,"\
	# 				"HDDT03 as date_in,HDTD03 as time_in,"\
	# 				"TOPR01 as terminal,HDRA03 "\
	# 				"FROM S2114C2V.LCB1DAT.DISCHARGE DISCHARGE "\
	# 				"where DISCHARGE.CNID03 = '"+ number +"' and RSIN01='"+ voy + "' "\
	# 				"order by DISCHARGE.HDDT03 desc")

	# rows = cursor_ctcs.fetchall()
	rows = query_execute(cursor_ctcs,number,voy,mode='full')

	if rows == None:
		return None

	columns = [column[0].lower() for column in cursor_ctcs.description]
	# print(columns, file=sys.stdout)
	if rows:
		clean_d = { k:v.strip() for k, v in zip(columns,rows[0]) if isinstance(v, str)}
		clean_date = { k:v for k, v in zip(columns,rows[0]) if isinstance(v, decimal.Decimal)}
		clean_d.update(clean_date)
		date_in_str = str(clean_d.get("date_in", None))
		time_in_str = str(clean_d.get("time_in", None))
		date_in_date = datetime(int(date_in_str[:4]),int(date_in_str[4:6]),int(date_in_str[-2:]))
		# print(date_in_str,time_in_str)
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
			
		# print (date_in_date)
		return dict(clean_d)

def db_ctcs_imp_get_out_info(cursor_ctcs,number,handleid):
	from datetime import datetime
	import decimal
	cursor_ctcs.execute("select "\
				"CNID03 as container,"\
				"HDDT03 as date_out,"\
				"HDTD03 as time_out,"\
				"VMID01 as license_number,"\
				"VMSR01 as out_by "\
				"FROM S2114C2V.LCB1DAT.DWELL_OUT DWELL_OUT "\
				"where CNID03 = '"+ number +"' and HDRA03='"+ handleid + "' "\
				"order by HDDT03 desc")
	rows = cursor_ctcs.fetchall()
	columns = [column[0].lower() for column in cursor_ctcs.description]
	# print(columns, file=sys.stdout)
	if rows:
		clean_d = { k:v.strip() for k, v in zip(columns,rows[0]) if isinstance(v, str)}
		clean_date = { k:v for k, v in zip(columns,rows[0]) if isinstance(v, decimal.Decimal)}
		clean_d.update(clean_date)
		date_out_str = str(clean_d.get("date_out", None))
		time_out_str = str(clean_d.get("time_out", None))
		date_out_date = datetime(int(date_out_str[:4]),int(date_out_str[4:6]),int(date_out_str[-2:]))
		if len(time_out_str) == 6 :
			hour_out = int(time_out_str[:2])
			minute_out = int(time_out_str[2:4])
			second_out = int(time_out_str[-2:])
		if len(time_out_str) == 5 :
			hour_out = int(time_out_str[:1])
			minute_out = int(time_out_str[1:3])
			second_out = int(time_out_str[-2:])
		if len(time_out_str) == 4 :
			hour_out = 0
			minute_out = int(time_out_str[:2])
			second_out = int(time_out_str[-2:])
		if len(time_out_str) == 3 :
			hour_out = 0
			minute_out = int(time_out_str[:1])
			second_out = int(time_out_str[-2:])
		if len(time_out_str) == 2 :
			hour_out = 0
			minute_out = 0
			second_out = int(time_out_str[-2:])
		date_out_date = date_out_date.replace(hour=hour_out,minute=minute_out,second=second_out)
		clean_d.update({'datetime_out' :date_out_date })
		return dict(clean_d)



# def db_nsw_imp_get_container(cursor_ctcs,cursor_nsw,number,mode='full'):

# 	# Case "4" : vStatus = "MTY"
#  #            Case "7" : vStatus = "LCL"
#  #            Case "8" : vStatus = "FCL"
#  #            Case "9" : vStatus = "LCL/CFS"
#  # shedNumberReleasePort
#  # 2835--A0
#  # 2811 -- B1
# 	# cursor_ctcs,cursor_nsw = init_db()
# 	if mode == 'full':
# 		cursor_nsw.execute("select masterbl as bill_of_landing,"\
# 							"containerdetail_number as container,"\
# 							"callsign,"\
# 							"voyagenumber as voy,"\
# 							"berthdate,"\
# 							"consigneeinfo_name,"\
# 							"TotalgrossweightInfo_weight gross,"\
# 							"TotalgrossweightInfo_unitcode unit_gross,"\
# 							"TotalPackageInfo_amount amount,"\
# 							"TotalPackageInfo_unitcode unit_amount,recordtime,lastentry, "\
# 							"MeasurementInfo_Measurement meas,MeasurementInfo_unitcode unit_meas,"\
# 							"ShipperInfo_Name,ShipperInfo_NameAndAddress,"\
#                         	"ConsigneeInfo_Name,ConsigneeInfo_NameAndAddress,"\
# 							"portofdischarge,placeofdelivery,containerdetail_status,shedNumberReleasePort "\
# 							"FROM  mman "\
# 							"where containerdetail_number = '"+ number +"' "\
# 							"order by recordtime desc")
# 							# descriptionOfGoods,
# 	else:
# 		cursor_nsw.execute("select masterbl as bill_of_landing,"\
# 							"containerdetail_number as container,"\
# 							"voyagenumber as voy,"\
# 							"berthdate "\
# 							"FROM  mman "\
# 							"where containerdetail_number = '"+ number +"' "\
# 							"order by recordtime desc")

# 	rows = cursor_nsw.fetchall()
# 	columns = [column[0] for column in cursor_nsw.description]
# 	delivery_info={}
# 	dis_info = {}
# 	if rows:
# 		dict_data = dict(zip(columns,rows[0]))
# 		# Get Discharge info
# 		voy = dict_data['voy']
# 		dis_info = db_ctcs_imp_get_discharge_info(cursor_ctcs,number,voy,mode)
# 		# print(dis_info)
# 		if dis_info != None :
# 			handle_id = dis_info['hdra03']
# 			# Get delivery information
# 			delivery_info = db_ctcs_imp_get_out_info(cursor_ctcs,number,str(handle_id))
# 			dict_data.update({'on_yard': 1})
# 		else:
# 			dis_info={}
# 			dict_data.update({'on_yard': 0})

# 		# Update data with Discharge and Delivery information
# 		dict_data.update({'discharge': dis_info})
# 		dict_data.update({'delivery': delivery_info})


# 		dict_data.update({'total': 0})
# 		dict_data.update({'lolo': 0})
# 		dict_data.update({'relo': 0})
# 		dict_data.update({'rate1': 0})
# 		dict_data.update({'rate2': 0})
# 		dict_data.update({'rate3': 0})
# 		# Remain CFS data
# 		return dict_data

# Modify on Jan 5,2021 -- To support CFS (One contiainer on multiple BL)
def db_nsw_imp_get_container(cursor_ctcs,cursor_nsw,number,mode='full'):

	# Case "4" : vStatus = "MTY"
 #            Case "7" : vStatus = "LCL"
 #            Case "8" : vStatus = "FCL"
 #            Case "9" : vStatus = "LCL/CFS"
 # shedNumberReleasePort
 # 2835--A0
 # 2811 -- B1
	# Get Latest record to find latest VOY
	cursor_nsw.execute("select voyagenumber as voy "\
					"FROM  mman "\
					"where containerdetail_number = '"+ number +"' "\
					"order by recordtime desc")
	rows = cursor_nsw.fetchone()
	voy=''
	if rows :
		# print(f'VOY : {rows[0]}')
		voy = rows[0]
	else :
		# Change return null to return [] , on Feb 6,2021
		return []

	# cursor_ctcs,cursor_nsw = init_db()
	if mode == 'full':
		cursor_nsw.execute("select masterbl as bill_of_landing,"\
							"containerdetail_number as container,"\
							"callsign,"\
							"voyagenumber as voy,"\
							"berthdate,"\
							"consigneeinfo_name,"\
							"TotalgrossweightInfo_weight gross,"\
							"TotalgrossweightInfo_unitcode unit_gross,"\
							"TotalPackageInfo_amount amount,"\
							"TotalPackageInfo_unitcode unit_amount,recordtime,lastentry, "\
							"MeasurementInfo_Measurement meas,MeasurementInfo_unitcode unit_meas,"\
							"ShipperInfo_Name,ShipperInfo_NameAndAddress,"\
                        	"ConsigneeInfo_Name,ConsigneeInfo_NameAndAddress,"\
							"portofdischarge,placeofdelivery,containerdetail_status,shedNumberReleasePort "\
							"FROM  mman "\
							"where containerdetail_number = '"+ number +"' and voyagenumber='" + voy +"' "\
							"order by recordtime desc")
							# descriptionOfGoods,
	else:
		cursor_nsw.execute("select masterbl as bill_of_landing,"\
							"containerdetail_number as container,"\
							"voyagenumber as voy,"\
							"berthdate "\
							"FROM  mman "\
							"where containerdetail_number = '"+ number +"' and voyagenumber='" + voy +"' "\
							"order by recordtime desc")

	rows = cursor_nsw.fetchall()
	columns = [column[0] for column in cursor_nsw.description]
	delivery_info={}
	dis_info = {}
		
	if rows:
		results = []
		for row in rows:
			dict_data = dict(zip(columns,row))
			# Get Discharge info
			voy = dict_data['voy']
			dis_info = db_ctcs_imp_get_discharge_info(cursor_ctcs,number,voy,mode)
			# print(dis_info)
			if dis_info != None :
				handle_id = dis_info['hdra03']
				# Get delivery information
				delivery_info = db_ctcs_imp_get_out_info(cursor_ctcs,number,str(handle_id))
				dict_data.update({'on_yard': 1})
			else:
				dis_info={}
				dict_data.update({'on_yard': 0})

			# Update data with Discharge and Delivery information
			dict_data.update({'discharge': dis_info})
			dict_data.update({'delivery': delivery_info})


			dict_data.update({'total': 0})
			dict_data.update({'lolo': 0})
			dict_data.update({'relo': 0})
			dict_data.update({'rate1': 0})
			dict_data.update({'rate2': 0})
			dict_data.update({'rate3': 0})
			# Added on Jan 19,2021 -- TO support CFS
			dict_data.update({'unstuffing': 0})
			dict_data.update({'wharf': 0})
			# Remain CFS data
			results.append(dict_data)
		return results

# Added on Jan 5,2021 -- To support CFS , One container on multiple BL
def db_nsw_imp_get_container_bl(cursor_ctcs,cursor_nsw,number,bl):
	mode='full'
	cursor_nsw.execute("select masterbl as bill_of_landing,"\
						"containerdetail_number as container,"\
						"callsign,"\
						"voyagenumber as voy,"\
						"berthdate,"\
						"consigneeinfo_name,"\
						"TotalgrossweightInfo_weight gross,"\
						"TotalgrossweightInfo_unitcode unit_gross,"\
						"TotalPackageInfo_amount amount,"\
						"TotalPackageInfo_unitcode unit_amount,recordtime,lastentry, "\
						"MeasurementInfo_Measurement meas,MeasurementInfo_unitcode unit_meas,"\
						"ShipperInfo_Name,ShipperInfo_NameAndAddress,"\
						"ConsigneeInfo_Name,ConsigneeInfo_NameAndAddress,"\
						"portofdischarge,placeofdelivery,containerdetail_status,shedNumberReleasePort "\
						"FROM  mman "\
						"where containerdetail_number = '"+ number +"' and masterbl ='" + bl + "' "\
						"order by recordtime desc")
	rows = cursor_nsw.fetchall()
	columns = [column[0] for column in cursor_nsw.description]
	delivery_info={}
	dis_info = {}
	if rows:
		is_paid = False
		dict_data = dict(zip(columns,rows[0]))
		# Get Discharge info
		voy = dict_data['voy']
		dis_info = db_ctcs_imp_get_discharge_info(cursor_ctcs,number,voy,mode)
		# print(dis_info)
		if dis_info != None :
			handle_id = dis_info['hdra03']
			# Get delivery information
			delivery_info = db_ctcs_imp_get_out_info(cursor_ctcs,number,str(handle_id))
			dict_data.update({'on_yard': 1})

			# Check Payment (paid) by Chutchai on March 2,2021
			is_paid = db_ctcs_get_payment(cursor_ctcs,bl,number)
		else:
			dis_info={}
			dict_data.update({'on_yard': 0})

		# Update data with Discharge and Delivery information
		dict_data.update({'discharge': dis_info})
		dict_data.update({'delivery': delivery_info})


		dict_data.update({'total': 0})
		dict_data.update({'lolo': 0})
		dict_data.update({'relo': 0})
		dict_data.update({'rate1': 0})
		dict_data.update({'rate2': 0})
		dict_data.update({'rate3': 0})
		# Remain CFS data
		dict_data.update({'unstuffing': 0})
		dict_data.update({'extra': 0})

		# Check Payment (paid) by Chutchai on March 2,2021
		dict_data.update({'paid': is_paid})

		# Added on March 25,2021 -- To remove white space from container and bill_of_landing
		dict_data['bill_of_landing']=dict_data['bill_of_landing'].strip()
		dict_data['container']=dict_data['container'].strip()

		return dict_data

def db_nsw_imp_get_bl(number):
	# db_get_container(number)
	# cursor_nsw.execute("select masterbl as booking,"\
	# 					"containerdetail_number as container "\
	# 					"FROM  mman "\
	# 					"where masterbl = '"+ number +"' "\
	# 					"order by containerdetail_number,recordtime")
	cursor_ctcs,cursor_nsw = init_db()
	cursor_nsw.execute("select	masterbl as booking,"\
						"containerdetail_number as container "\
						"FROM  mman "\
						"where masterbl = '" + number + "' "\
						"group by masterbl,containerdetail_number")
	
	# cursor_nsw.execute("select	masterbl as booking,"\
	# 					"containerdetail_number as container "\
	# 					"FROM  mman "\
	# 					"where masterbl = '" + number + "' ")

	rows = cursor_nsw.fetchall()
	columns = [column[0].lower() for column in cursor_nsw.description]
	# print(columns, file=sys.stdout)
	if rows:
		results = []
		for row in rows:
			clean_d = { k:v.strip() for k, v in zip(columns,row) if isinstance(v, str)}
			container_info = db_nsw_imp_get_container_bl(cursor_ctcs,cursor_nsw,clean_d['container'],number)
			results.append(container_info)
		# print(results, file=sys.stdout)
		return results
##############################################################

# Added on March 2,2021 -- To get payment status (Paid or Not paid)
def db_ctcs_get_payment(cursor_ctcs,order,container):
	cursor_ctcs.execute("select 1 "\
						"from LCB1DAT.GAGTID02  "\
						"where ORRF93='" + order + "' and CNID94='" + container + "' ")
	row = cursor_ctcs.fetchone()
	# print(row)
	if row == None:
		return False
	return True


# if __name__ == "__main__":
# 	app.run(host='127.0.0.1', port=5000)
	# app.run()