# -*- coding: utf-8 -*-
"""
Created on Wed Jun 07 22:11:16 2017

@author: warat.pue
"""
import cx_Oracle
import pandas

class Query_obj:
    def __init__(self, username, password, txtcnt):
        self.username = username
        self.password = password
        self.txtcnt = txtcnt
        self.g_con = None
    
    def get_rows(self,sql):
        
        if self.g_con is None:
            self.g_con = cx_Oracle.connect(self.username,self.password,self.txtcnt)
        con = self.g_con
        curs = con.cursor()
        result = curs.execute(sql)
        return result
    
    def update_rows(self,sql):
        
        if self.g_con is None:
            self.g_con = cx_Oracle.connect(self.username,self.password,self.txtcnt)
        con = self.g_con
        curs = con.cursor()    
        curs.execute(sql) 
        con.commit()
        return True
    
    def get_scalar(self,sql):
    
        if self.g_con is None:
            self.g_con = cx_Oracle.connect(self.username,self.password,self.txtcnt)
        con = self.g_con
        curs = con.cursor()
        curs.execute(sql)
        data = curs.fetchone()
        return data[0]

    def update_dataframes(self,sql,data):
    
        if self.g_con is None:
            self.g_con = cx_Oracle.connect(self.username,self.password,self.txtcnt)
        con = self.g_con
        curs = con.cursor()
        curs.executemany(sql, data)
        con.commit()
        return True

    def call_function(self, name):
    
        if self.g_con is None:
            self.g_con = cx_Oracle.connect(self.username,self.password,self.txtcnt)
        con = self.g_con
        curs = con.cursor()
        curs.callfunc(name, float)
        return True

    def call_function_2(self, name, parameter):
    
        if self.g_con is None:
            self.g_con = cx_Oracle.connect(self.username,self.password,self.txtcnt)
        con = self.g_con
        curs = con.cursor()
        result = curs.callfunc(name, float, parameter)
        return result



        



