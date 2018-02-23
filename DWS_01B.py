# -*- coding: utf-8 -*-
"""
Created on Mon Jun 26 22:33:39 2017

@author: ghb
"""

import sys
import os
import pandas as pd
from Query_obj import Query_obj
from checkFlag import DataValidation
from timeit import default_timer as timer
import dws_config
import cif_config
import cx_Oracle
import datetime
DBHOST = dws_config.DBHOST
DBPORT = dws_config.DBPORT
DBSID = dws_config.DBSID
DBUSER = dws_config.DBUSER
DBPASS = dws_config.DBPASS
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"
g_date_format = "YYYY-MM-DDHH24MISS"
insert_date_format = "DD/MM/YYYY"
table = 'res_dws_01b_result'

if __name__ == '__main__':
    #print len(sys.argv)
    if (len(sys.argv) <= 1):
        sys.exit("ERROR:Not found JOB_ID \nEx " + sys.argv[0] + " <JOB_ID>")

    job_id = sys.argv[1]
    process_id = os.getpid()
    
    print "Start Script ...DWS_01B "
    print "Process ID = " + str(process_id)
    
    print 'Create query obj'
    db1 = Query_obj(cif_config.username,cif_config.password,cif_config.txtcnt)
    
    
    # Get SYSDATE for START_DATE
    print 'start get sysdate'
    sql = "SELECT TO_CHAR(SYSDATE,'" + g_date_format + "') FROM DUAL"
    
    try:
        job_start_date_time = db1.get_scalar(sql)
    except:
        print "Unexpected error:", sys.exc_info()[0]
    
    
    # STAMP START JOB
    sql = "UPDATE JOB_TRN_BACKEND \
           SET STATUS='P' \
             , PROGRESS='0' \
             , JOB_START_DATE_TIME=TO_DATE('" + job_start_date_time + "','" + g_date_format + "') \
             , PROCESS_ID='" + str(process_id) + "'  \
           WHERE JOB_KEY = '" + str(job_id) + "' "
    #print sql
    db1.update_rows(sql)
    
    start = timer()
    print("Start Checking Interest Code")
    intCode =  DataValidation(pd.DataFrame(), "GAM.SOL_ID,FORACID,CIF_ID,ACCT_NAME,SCHM_CODE, CUST_TYPE_CODE,INT_TBL_CODE,ENTITY_ID, INT_TBL_CODE_SRL_NUM","tbaadm","ITC")
    intCode.checkInterestCode()
    #50%
    GSP = DataValidation(pd.DataFrame(), "CUST_TYPE_CODE,GSP.INTEREST_CODE,SCHM_CODE","CUSTOM","C_GSP")
    GSP.df = GSP.query2("SELECT " + GSP.colStatement + " FROM C_GSP GSP WHERE (SCHM_TYPE=\'TDA\' OR SCHM_TYPE=\'SBA\' OR SCHM_TYPE=\'CAA\')","2")
    joined = pd.merge(intCode.df,GSP.df,on = ['CUST_TYPE_CODE','SCHM_CODE'],how = 'left')      
    joined['GSP.INTEREST_CODE'] = joined['GSP.INTEREST_CODE'].str.upper()
    joined['INT_TBL_CODE'] = joined['INT_TBL_CODE'].str.upper()
    result = joined[joined['GSP.INTEREST_CODE'] != joined.INT_TBL_CODE]
    result.rename(columns={'GAM.SOL_ID': 'SOL_ID'}, inplace=True) 
    #75%
    SOL = DataValidation(pd.DataFrame(), "SOL_DESC, SOL_ID","tbaadm","CBT")
    SOL.df = SOL.query2("SELECT " + SOL.colStatement + " FROM tbaadm.SOL", "2")
    result = pd.merge(result,SOL.df,on = ['SOL_ID'],how = 'left')    
    
    result.rename(columns={'INT_TBL_CODE': 'INTEREST_CODE', 'GSP.INTEREST_CODE': 'GSP_INTEREST_CODE'}, inplace=True)  
    result = result.sort_values(['SOL_ID','FORACID'], ascending=[True, True])
    result['JOB_KEY'] = job_id
    #result = result.drop('INT_TBL_CODE_SRL_NUM', 1)
    #75%
    #tns = '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=' + DBHOST + ')(PORT=' + DBPORT + '))(CONNECT_DATA= (SID='+ DBSID +')))'
    #con = cx_Oracle.connect(DBUSER,DBPASS,tns)
    #cursor = con.cursor()
    #sql = "INSERT INTO "+ table +" \
    #values(:SOL_DESC, :SOL_ID, :FORACID, :CIF_ID, :ACCT_NAME, :SCHM_CODE, :CUST_TYPE_CODE, :INTEREST_CODE, :GSP_INTEREST_CODE, :JOB_KEY, TO_DATE('{0}','{1}')".format(job_start_date_time, g_date_format)
    #cursor.executemany(sql, result.values.tolist())
    #con.commit()
    #cursor.close()
    result = result.reset_index(drop=True)
    for i in range(result.shape[0]):
    #for i in range(0, 100):    
        sql_2 = '''
        INSERT INTO res_dws_01b_result 
		values('{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', TO_DATE('{0}','{1}'))
        
        '''.format(job_start_date_time, g_date_format,result.SOL_DESC[i],result.SOL_ID[i],result.FORACID[i],result.CIF_ID[i],result.ACCT_NAME[i],result.SCHM_CODE[i],result.CUST_TYPE_CODE[i],result.INTEREST_CODE[i],result.GSP_INTEREST_CODE[i],result.JOB_KEY[i])
        db1.update_rows(sql_2)
        if i%20 == 0:
            print "Done: "+ str(i)  
	end = timer()
    intCode.elaspedTime = end - start
    print( "checkInterestCode Completed, Time Elapsed : " + str(intCode.elaspedTime) + " seconds")
    # STAMP FINISH JOB
    sql = "UPDATE JOB_TRN_BACKEND \
           SET STATUS='S' \
             , PROGRESS='100' \
             , JOB_END_DATE_TIME=SYSDATE \
           WHERE JOB_KEY = '" + str(job_id) + "' "
    #print sql
    cur = db1.update_rows(sql)
    