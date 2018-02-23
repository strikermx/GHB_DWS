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
import cx_Oracle
import datetime
import cif_config
from dateutil.relativedelta import relativedelta
from timeit import default_timer as timer
DBHOST = dws_config.DBHOST
DBPORT = dws_config.DBPORT
DBSID = dws_config.DBSID
DBUSER = dws_config.DBUSER
DBPASS = dws_config.DBPASS
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"
g_date_format = "YYYY-MM-DDHH24MISS"
insert_date_format = "DD/MM/YYYY"
table = 'res_dws_05a_result'
def to_tyr(row):
    row["MATURITY_DATE"] = row["MATURITY_DATE"] + relativedelta(years=+543)   
    #row["MATURITY_DATE"] = row["MATURITY_DATE"].strftime('%Y-%m-%d%H%M%S')
    return row

if __name__ == '__main__':
    #print len(sys.argv)
    if (len(sys.argv) <= 1):
        sys.exit("ERROR:Not found JOB_ID \nEx " + sys.argv[0] + " <JOB_ID>")

    job_id = sys.argv[1]
    process_id = os.getpid()
    
    print "Start Script ...DWS_05A "
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
    print("Start Checking Renew")
    renewed = DataValidation(pd.DataFrame(), "GAM.SOL_ID,FORACID,CIF_ID,ACCT_NAME,SCHM_CODE,MATURITY_DATE,GAM.CLR_BAL_AMT,GAM.FREZ_CODE","tbaadm","TAM")
    renewed.checkRenewed()
    renewed.df.rename(columns={'GAM.CLR_BAL_AMT': 'CLR_BAL_AMT', 'GAM.SOL_ID': 'SOL_ID', 'GAM.FREZ_CODE':'FREZ_CODE'}, inplace=True)
    centralTime = DataValidation(pd.DataFrame(), "DB_STAT_DATE","tbaadm","GCT")
    centralTime.df = centralTime.query2("SELECT " + centralTime.colStatement + " FROM tbaadm.gct", "2")
    CheckingDate = centralTime.df.DB_STAT_DATE[0] + relativedelta(days=-2)
    renewed.df = renewed.df.loc[(renewed.df.MATURITY_DATE < CheckingDate)]
    #renewed.df = renewed.df.apply(to_tyr, axis=1)
    #50%
    #SOL = DataValidation(pd.DataFrame(), "SOL_DESC, SOL_ID","tbaadm","CBT")
    #SOL.df = SOL.query2("SELECT " + SOL.colStatement + " FROM tbaadm.SOL", "2")
    #renewed.df = pd.merge(renewed.df,SOL.df,on = ['SOL_ID'],how = 'left')
    #75%
    #tns = '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=' + DBHOST + ')(PORT=' + DBPORT + '))(CONNECT_DATA= (SID='+ DBSID +')))'
    #con = cx_Oracle.connect(DBUSER,DBPASS,tns)
    #cursor = con.cursor()
    #sql = '''INSERT INTO res_dws_05a_result VALUES(:SOL_DESC, :SOL_ID, :FORACID, :CIF_ID, :ACCT_NAME, :SCHM_CODE, MATURITY_DATE, :CLR_BAL_AMT, '{0}', TO_DATE('{1}', '{2}'), :FREZ_CODE)'''.format(job_id, job_start_date_time, g_date_format)
    #cursor.executemany(sql, renewed.df.values.tolist())
    #con.commit()
    #cursor.close()
    renewed.df = renewed.df.reset_index(drop=True)
    for i in range(renewed.df.shape[0]):
    #for i in range(0, 100):    
        sql_2 = '''
        INSERT INTO RES_DWS_05A_RESULT
        select sol_desc, '{3}', '{4}', '{5}', '{6}', '{7}', TO_DATE('{8}','{2}'), '{9}', '{0}', TO_DATE('{1}','{2}'), '{10}'
        from
        (select sol_desc, sol_id from tbaadm.sol 
        where sol_id = '{3}')
        '''.format(job_id,job_start_date_time, g_date_format,renewed.df.SOL_ID[i],renewed.df.FORACID[i],renewed.df.CIF_ID[i],renewed.df.ACCT_NAME[i],renewed.df.SCHM_CODE[i],renewed.df.MATURITY_DATE[i].strftime('%Y-%m-%d%H%M%S'),renewed.df.CLR_BAL_AMT[i],renewed.df.FREZ_CODE[i])
        db1.update_rows(sql_2)
        if i%20 == 0:
            print "Done: "+ str(i)
    end = timer()
    renewed.elaspedTime = end - start
    print( "checkRenew Completed, Time Elapsed : " + str(renewed.elaspedTime) + " seconds")
    # STAMP FINISH JOB
    sql = "UPDATE JOB_TRN_BACKEND \
           SET STATUS='S' \
             , PROGRESS='100' \
             , JOB_END_DATE_TIME=SYSDATE \
           WHERE JOB_KEY = '" + str(job_id) + "' "
    #print sql
    cur = db1.update_rows(sql)
    