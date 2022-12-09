#! /usr/bin/python3

import datetime
import json     # json
import pymysql  # mariadb
import Logger


_logger = Logger.Logger("BemsService")

#############################################################################################################################################
# 총사용량 데이터


def GetConnection():
    connection = pymysql.connect(host='43.200.196.117', port=3306, user='root', password='1234',
                                    db='project_real_final', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    return connection
    

async def GetAhuTotalPowerDaily(startDate: str, endDate: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            query = "select left(a.runDateTime,8) as runDate, cast(sum(a.ahu_total_pow) as char) as sumData " + \
                    "from " + \
                    "( " + \
                    "   select runDateTime, ahu_total_pow " + \
                    "   from ahu_total_use " + \
                    "   where left(runDateTime,8) between " + "'" + startDate + "'" + " and " + "'" + endDate + "'" + \
                    ") a " +\
                    "group by left(a.runDateTime,8) " + \
                    "order by left(runDateTime,8) ; "

            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetAhuTotalPowerDaily('{startDate}',{endDate})'")
            _logger.Info(
                f"succeed to do 'GetAhuTotalPowerDaily('{startDate}',{endDate})'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetAhuTotalPowerDaily('{startDate}',{endDate})'")
        _logger.Info(f"error to do 'GetAhuTotalPowerDaily('{startDate}',{endDate})'")


async def GetAhuTotalPowerHourly(runDate: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            # query = "SELECT LpID, LpDate, cast(LpData as char) as LpData FROM wm_fems.Raw_KepcoDayLpData where LpDate > %s limit 2;"
            query = "select left(a.runDateTime,10) as runDate, cast(sum(a.ahu_total_pow) as char) as sumData " + \
                    "from " + \
                    "( " + \
                    "   select runDateTime, ahu_total_pow " + \
                    "   from ahu_total_use " + \
                    "   where left(runDateTime,8) = " + "'" + runDate + "'" + \
                    ") a " +\
                    "group by left(runDateTime,10) order by left(runDateTime,10); "
            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetAhuTotalPowerHourly('{runDate}')'")
            _logger.Info(f"succeed to do 'GetAhuTotalPowerHourly('{runDate}')'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetAhuTotalPowerHourly('{runDate}'")
        _logger.Info(f"error to do 'GetAhuTotalPowerHourly('{runDate}'")


async def GetChillerTotalPowerDaily(startDate: str, endDate: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            query = "select left(a.runDateTime,8) as runDate, cast(sum(a.ch_total_pow) as char) as sumData " + \
                    "from " + \
                    "( " + \
                    "   select runDateTime, ch_total_pow " + \
                    "   from chiller_total_use " + \
                    "   where left(runDateTime,8) between " + "'" + startDate + "'" + " and " + "'" + endDate + "'" + \
                    ") a " +\
                    "group by left(a.runDateTime,8) " + \
                    "order by left(runDateTime,8) ; "

            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetChillerTotalPowerDaily('{startDate}',{endDate})'")
            _logger.Info(
                f"succeed to do 'GetChillerTotalPowerDaily('{startDate}',{endDate})'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetChillerTotalPowerDaily('{startDate}',{endDate})'")
        _logger.Info(f"error to do 'GetChillerTotalPowerDaily('{startDate}',{endDate})'")


async def GetChillerTotalPowerHourly(runDate: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            # query = "SELECT LpID, LpDate, cast(LpData as char) as LpData FROM wm_fems.Raw_KepcoDayLpData where LpDate > %s limit 2;"
            query = "select left(a.runDateTime,10) as runDate, cast(sum(a.ch_total_pow) as char) as sumData " + \
                    "from " + \
                    "( " + \
                    "   select runDateTime, ch_total_pow " + \
                    "   from chiller_total_use " + \
                    "   where left(runDateTime,8) = " + "'" + runDate + "'" + \
                    ") a " +\
                    "group by left(runDateTime,10) order by left(runDateTime,10); "
            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetChillerTotalPowerHourly('{runDate}')'")
            _logger.Info(f"succeed to do 'GetChillerTotalPowerHourly('{runDate}')'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetChillerTotalPowerHourly('{runDate}'")
        _logger.Info(f"error to do 'GetChillerTotalPowerHourly('{runDate}'")


async def GetBoilerTotalPowerDaily(startDate: str, endDate: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            query = "select left(a.runDateTime,8) as runDate, cast(sum(a.bo_total_pow) as char) as sumData " + \
                    "from " + \
                    "( " + \
                    "   select runDateTime, bo_total_pow " + \
                    "   from boiler_total_use " + \
                    "   where left(runDateTime,8) between " + "'" + startDate + "'" + " and " + "'" + endDate + "'" + \
                    ") a " +\
                    "group by left(a.runDateTime,8) " + \
                    "order by left(runDateTime,8) ; "

            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetBoilerTotalPowerDaily('{startDate}',{endDate})'")
            _logger.Info(
                f"succeed to do 'GetBoilerTotalPowerDaily('{startDate}',{endDate})'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetBoilerTotalPowerDaily('{startDate}',{endDate})'")
        _logger.Info(f"error to do 'GetBoilerTotalPowerDaily('{startDate}',{endDate})'")


async def GetBoilerTotalPowerHourly(runDate: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            # query = "SELECT LpID, LpDate, cast(LpData as char) as LpData FROM wm_fems.Raw_KepcoDayLpData where LpDate > %s limit 2;"
            query = "select left(a.runDateTime,10) as runDate, cast(sum(a.bo_total_pow) as char) as sumData " + \
                    "from " + \
                    "( " + \
                    "   select runDateTime, bo_total_pow " + \
                    "   from boiler_total_use " + \
                    "   where left(runDateTime,8) = " + "'" + runDate + "'" + \
                    ") a " +\
                    "group by left(runDateTime,10) order by left(runDateTime,10); "
            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetBoilerTotalPowerHourly('{runDate}')'")
            _logger.Info(f"succeed to do 'GetBoilerTotalPowerHourly('{runDate}')'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetBoilerTotalPowerHourly('{runDate}'")
        _logger.Info(f"error to do 'GetBoilerTotalPowerHourly('{runDate}'")


async def GetBoilerTotalGasDaily(startDate: str, endDate: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            query = "select left(a.runDateTime,8) as runDate, cast(sum(a.total_gas) as char) as sumData " + \
                    "from " + \
                    "( " + \
                    "   select runDateTime, total_gas " + \
                    "   from boiler_total_use " + \
                    "   where left(runDateTime,8) between " + "'" + startDate + "'" + " and " + "'" + endDate + "'" + \
                    ") a " +\
                    "group by left(a.runDateTime,8) " + \
                    "order by left(runDateTime,8) ; "

            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetBoilerTotalGasDaily('{startDate}',{endDate})'")
            _logger.Info(
                f"succeed to do 'GetBoilerTotalGasDaily('{startDate}',{endDate})'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetBoilerTotalGasDaily('{startDate}',{endDate})'")
        _logger.Info(f"error to do 'GetBoilerTotalGasDaily('{startDate}',{endDate})'")


async def GetBoilerTotalGasHourly(runDate: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            # query = "SELECT LpID, LpDate, cast(LpData as char) as LpData FROM wm_fems.Raw_KepcoDayLpData where LpDate > %s limit 2;"
            query = "select left(a.runDateTime,10) as runDate, cast(sum(a.bo_total_pow) as char) as sumData " + \
                    "from " + \
                    "( " + \
                    "   select runDateTime, bo_total_pow " + \
                    "   from boiler_total_use " + \
                    "   where left(runDateTime,8) = " + "'" + runDate + "'" + \
                    ") a " +\
                    "group by left(runDateTime,10) order by left(runDateTime,10); "
            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetBoilerTotalGasHourly('{runDate}')'")
            _logger.Info(f"succeed to do 'GetBoilerTotalGasHourly('{runDate}')'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetBoilerTotalGasHourly('{runDate}'")
        _logger.Info(f"error to do 'GetBoilerTotalGasHourly('{runDate}'")


#############################################################################################################################################
# AHU 온도 데이터



async def GetAhuTempData(machine_num: int, startDate: str, endDate: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            # query = "SELECT LpID, LpDate, cast(LpData as char) as LpData FROM wm_bems.Raw_KepcoDayLpData where LpDate > %s limit 2;"
            query = "select " +\
                "   machine_num, runDateTime, " +\
                "   cast(ahu_sat as char) as ahu_sat , " +\
                "   cast(ahu_oat as char) as ahu_oat, " +\
                "   cast(ahu_mat as char) as ahu_mat, " +\
                "   cast(ahu_rat as char) as ahu_rat " +\
                "from ahu_info \n" +\
                f"where machine_num = '{machine_num}' and left(runDateTime,8) between '{startDate}' and '{endDate}'" +\
                "order by runDateTime ;"
            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetAhuTempData('{machine_num}','{startDate}','{endDate}')'")
            _logger.Info(
                f"succeed to do 'GetAhuTempData('{machine_num}','{startDate}','{endDate}')'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetAhuTempData('{machine_num}','{startDate}','{endDate}')'")
        _logger.Info(
            f"error to do 'GetAhuTempData('{machine_num}','{startDate}','{endDate}')'")





#############################################################################################################################################
# AHU 개별 전력 데이터



async def GetAhuPowerData(machine_num: int, startDate: str, endDate: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            # query = "SELECT LpID, LpDate, cast(LpData as char) as LpData FROM wm_bems.Raw_KepcoDayLpData where LpDate > %s limit 2;"
            query = "select " +\
                "   machine_num, runDateTime, " +\
                "   cast(ahu_pow as char) as ahu_pow " +\
                "from ahu_info \n" +\
                f"where machine_num = '{machine_num}' and left(runDateTime,8) between '{startDate}' and '{endDate}'" +\
                "order by runDateTime ;"
            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetAhuPowerData('{machine_num}','{startDate}','{endDate}')'")
            _logger.Info(
                f"succeed to do 'GetAhuPowerData('{machine_num}','{startDate}','{endDate}')'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetAhuPowerData('{machine_num}','{startDate}','{endDate}')'")
        _logger.Info(
            f"error to do 'GetAhuPowerData('{machine_num}','{startDate}','{endDate}')'")




#############################################################################################################################################
# Chiller 개별 급수온도 데이터



async def GetChillerCwstData(machine_num: int, startDate: str, endDate: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            # query = "SELECT LpID, LpDate, cast(LpData as char) as LpData FROM wm_bems.Raw_KepcoDayLpData where LpDate > %s limit 2;"
            query = "select " +\
                "   machine_num, runDateTime, " +\
                "   cast(ch_cwst as char) as ch_cwst " +\
                "from chiller_info \n" +\
                f"where machine_num = '{machine_num}' and left(runDateTime,8) between '{startDate}' and '{endDate}'" +\
                "order by runDateTime ;"
            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetChillerCwstData('{machine_num}','{startDate}','{endDate}')'")
            _logger.Info(
                f"succeed to do 'GetChillerCwstData('{machine_num}','{startDate}','{endDate}')'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetChillerCwstData('{machine_num}','{startDate}','{endDate}')'")
        _logger.Info(
            f"error to do 'GetChillerCwstData('{machine_num}','{startDate}','{endDate}')'")



#############################################################################################################################################
# Chiller 개별 전력 데이터



async def GetChillerPowerData(machine_num: int, startDate: str, endDate: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            # query = "SELECT LpID, LpDate, cast(LpData as char) as LpData FROM wm_bems.Raw_KepcoDayLpData where LpDate > %s limit 2;"
            query = "select " +\
                "   machine_num, runDateTime, " +\
                "   cast(ch_pow as char) as ch_pow " +\
                "from chiller_info \n" +\
                f"where machine_num = '{machine_num}' and left(runDateTime,8) between '{startDate}' and '{endDate}'" +\
                "order by runDateTime ;"
            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetChillerPowerData('{machine_num}','{startDate}','{endDate}')'")
            _logger.Info(
                f"succeed to do 'GetChillerPowerData('{machine_num}','{startDate}','{endDate}')'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetChillerPowerData('{machine_num}','{startDate}','{endDate}')'")
        _logger.Info(
            f"error to do 'GetChillerPowerData('{machine_num}','{startDate}','{endDate}')'")



#############################################################################################################################################
# Boiler 개별 가스량 데이터



async def GetBoilerGasData(machine_num: int, startDate: str, endDate: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            # query = "SELECT LpID, LpDate, cast(LpData as char) as LpData FROM wm_bems.Raw_KepcoDayLpData where LpDate > %s limit 2;"
            query = "select " +\
                "   machine_num, runDateTime, " +\
                "   cast(bo_gas as char) as bo_gas " +\
                "from boiler_info \n" +\
                f"where machine_num = '{machine_num}' and left(runDateTime,8) between '{startDate}' and '{endDate}'" +\
                "order by runDateTime ;"
            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetBoilerGasData('{machine_num}','{startDate}','{endDate}')'")
            _logger.Info(
                f"succeed to do 'GetBoilerGasData('{machine_num}','{startDate}','{endDate}')'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetBoilerGasData('{machine_num}','{startDate}','{endDate}')'")
        _logger.Info(
            f"error to do 'GetBoilerGasData('{machine_num}','{startDate}','{endDate}')'")




#############################################################################################################################################
# Boiler 개별 전력 데이터



async def GetBoilerPowerData(machine_num: int, startDate: str, endDate: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            # query = "SELECT LpID, LpDate, cast(LpData as char) as LpData FROM wm_bems.Raw_KepcoDayLpData where LpDate > %s limit 2;"
            query = "select " +\
                "   machine_num, runDateTime, " +\
                "   cast(bo_pow as char) as bo_pow " +\
                "from boiler_info \n" +\
                f"where machine_num = '{machine_num}' and left(runDateTime,8) between '{startDate}' and '{endDate}'" +\
                "order by runDateTime ;"
            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetBoilerPowerData('{machine_num}','{startDate}','{endDate}')'")
            _logger.Info(
                f"succeed to do 'GetBoilerPowerData('{machine_num}','{startDate}','{endDate}')'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetBoilerPowerData('{machine_num}','{startDate}','{endDate}')'")
        _logger.Info(
            f"error to do 'GetBoilerPowerData('{machine_num}','{startDate}','{endDate}')'")


#############################################################################################################################################
# 원본 AHU 데이터
'''
async def GetAHUConfiguration():
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            # query = "SELECT LpID, LpDate, cast(LpData as char) as LpData FROM wm_fems.Raw_KepcoDayLpData where LpDate > %s limit 2;"
            query = "select * from Config_WMMachines order by data_id ;"
            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetAHUConfiguration()'")
            _logger.Info(f"succeed to do 'GetAHUConfiguration()'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetAHUConfiguration()")
        _logger.Info(f"error to do 'GetAHUConfiguration()")


async def GetAHUInfos():
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            # query = "SELECT LpID, LpDate, cast(LpData as char) as LpData FROM wm_fems.Raw_KepcoDayLpData where LpDate > %s limit 2;"
            query = "select FAC_ID, FAC_NAME, FAC_TYPE, FAC_LOC, FAC_USE, " + \
                "cast(FAC_VOLTAGE as char) as FAC_VOLTAGE, cast(FAC_KW as char) as FAC_KW, cast(FAC_INV_CNT as char) as FAC_INV_CNT, FAC_DESC " +\
                "from INFO_FACILITY order by FAC_ID ; "
                
            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetAHUInfo()'")
            _logger.Info(f"succeed to do 'GetAHUInfo()'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetAHUInfo()")
        _logger.Info(f"error to do 'GetAHUInfo() : {ex}")


async def GetAHUInfo(FAC_NAME: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            # query = "SELECT LpID, LpDate, cast(LpData as char) as LpData FROM wm_bems.Raw_KepcoDayLpData where LpDate > %s limit 2;"
            query = "select FAC_ID, FAC_NAME, FAC_TYPE, FAC_LOC, FAC_USE, " + \
                "cast(FAC_VOLTAGE as char) as FAC_VOLTAGE, cast(FAC_KW as char) as FAC_KW, cast(FAC_INV_CNT as char) as FAC_INV_CNT, FAC_DESC " +\
                "from INFO_FACILITY	" + \
                "where FAC_NAME = " + "'" + FAC_NAME + "' ;"
            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetAHUInfo('{FAC_NAME}')'")
            _logger.Info(f"succeed to do 'GetAHUInfo('{FAC_NAME}')'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetAHUInfo('{FAC_NAME}')")
        _logger.Info(f"error to do 'GetAHUInfo('{FAC_NAME}')")


async def GetAHUData(ahu_id: str, startDate: str, endDate: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            # query = "SELECT LpID, LpDate, cast(LpData as char) as LpData FROM wm_bems.Raw_KepcoDayLpData where LpDate > %s limit 2;"
            query = "select " +\
                "   ahu_id, run_datetime, " +\
                "   cast(ahu_set_temp as char) as ahu_set_temp , " +\
                "   cast(ahu_set_hum as char) as ahu_set_hum, " +\
                "   cast(ahu_ret_temp as char) as ahu_ret_temp, " +\
                "   cast(ahu_ret_hum as char) as ahu_ret_hum, " +\
                "   cast(ahu_sup_temp as char) as ahu_sup_temp, " +\
                "   cast(ahu_sup_hum as char) as ahu_sup_hum, " +\
                "   cast(ahu_out_temp as char) as ahu_out_temp, " +\
                "   cast(ahu_out_hum as char) as ahu_out_hum, " +\
                "   cast(ahu_comp1_run as char) as ahu_comp1_run, " +\
                "   cast(ahu_comp2_run as char) as ahu_comp2_run, " +\
                "   cast(ahu_warm_run as char) as ahu_warm_run, " +\
                "   cast(ahu_addhum_run as char) as ahu_addhum_run, " +\
                "   cast(ahu_cool_diff as char) as ahu_cool_diff, " +\
                "   cast(ahu_warm_diff as char) as ahu_warm_diff, " +\
                "   cast(ahu_addhum_diff as char) as ahu_addhum_diff, " +\
                "   cast(ahu_rmvhum_diff as char) as ahu_rmvhum_diff " +\
                "from Raw_WMAHUData \n" +\
                f"where ahu_id = '{ahu_id}' and left(run_datetime,8) between '{startDate}' and '{endDate}'" +\
                "order by run_datetime ;"
            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # print(f"{datetime.datetime.now()} : succeed to do 'GetAHUData('{ahu_id}','{startDate}','{endDate}')'")
            _logger.Info(
                f"succeed to do 'GetAHUData('{ahu_id}','{startDate}','{endDate}')'")
            return json_data

    except Exception as ex:
        # print(f"{datetime.datetime.now()} : error to do 'GetAHUData('{ahu_id}','{startDate}','{endDate}')'")
        _logger.Info(
            f"error to do 'GetAHUData('{ahu_id}','{startDate}','{endDate}')'")


async def GetAHUSetSupData(runDate: str):
    try:
        connection = GetConnection()

        with connection.cursor() as cursor:
            # query = "SELECT LpID, LpDate, cast(LpData as char) as LpData FROM wm_bems.Raw_KepcoDayLpData where LpDate > %s limit 2;"
            query = "select ahu_id, run_datetime, " +\
                "   cast(ahu_set_temp as char) as ahu_set_temp, " +\
                "   cast(ahu_sup_temp as char) as ahu_sup_temp " +\
                "from Raw_WMAHUData " +\
                f"where run_datetime = '{runDate}';"
            cursor.execute(query)
            # row_headers=[x[0] for x in cursor.description] #this will extract row headers
            rv = cursor.fetchall()
            # ress = str(rv)
            json_data = json.dumps(rv, indent=4)
            # message = f"{datetime.datetime.now()} : succeed to do 'GetAHUSetSupData('{runDate}')'"
            # print(message)
            message = f"succeed to do 'GetAHUSetSupData('{runDate}')'"
            _logger.Info(message)
            return json_data

    except Exception as ex:
        # print(
        #     f"{datetime.datetime.now()} : error to do 'GetAHUSetSupData('{runDate}')'")
        message = f"error to do 'GetAHUSetSupData('{runDate}')'"
        _logger.Info(message)
'''