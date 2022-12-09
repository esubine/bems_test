#! /usr/bin/python3

# 참고 -> https://soyoung-new-challenge.tistory.com/81
# http://bigdata.dongguk.ac.kr/lectures/DB/_book/python%EC%97%90%EC%84%9C-mysql%EB%8D%B0%EC%9D%B4%ED%84%B0%EC%9D%98-%EC%A0%91%EA%B7%BC.html
# CORS : https://lucky516.tistory.com/108

# uvicorn main:app --reload --host=0.0.0.0 --port=8888
# uvicorn BemsMain:app --reload --host=0.0.0.0 --port=8888


import uvicorn                                      # Web Server
from fastapi import FastAPI, Response               # Json Response
from fastapi.responses import HTMLResponse          # HTML Response
from fastapi.middleware.cors import CORSMiddleware  # CORS
import Logger

# import json     # json
# import pymysql  # mariadb

# _logger = Logger.Logger("BemsService")
import BemsService  # implementation


app = FastAPI()

# CORS
origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# WAS


@app.get("/")
async def welcome():
    html_content = """
    <html>
    <br/>
    <br/>
    <head>
    <title>Welcome to BEMS Service!</title>
    <style>
    html { color-scheme: light dark; }
    body { width: 35em; margin: 0 auto;
    font-family: Tahoma, Verdana, Arial, sans-serif; }
    </style>
    </head>
    <body>
    <h1>Welcome to BEMS Service!</h1>
    <br/>
    <p>If you see this page, the BemsREST web server is successfully working.</p>

    <p>For documentation and support please refer to swagger documents '.../docs'
    </a>.<br/>

    <p><em>Thank you for using BEMS Service.</em></p>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content, status_code=200)

# async def root():
#     return """
#     <html>
#     <head>
#     <title>Welcome to BEMS Service!</title>
#     <style>
#     html { color-scheme: light dark; }
#     body { width: 35em; margin: 0 auto;
#     font-family: Tahoma, Verdana, Arial, sans-serif; }
#     </style>
#     </head>
#     <body>
#     <h1>Welcome to BEMS Service!</h1>
#     <br/>
#     <p>If you see this page, the BemsREST web server is successfully working.</p>

#     <p>For documentation and support please refer to
#     <a href="http://localhost:8888/docs">documents
#     </a>.<br/>

#     <p><em>Thank you for using BEMS Service.</em></p>
#     </body>
#     </html>
#     """
# async def root():
#     return {"message": "Welcome to Bems Service"}

# 총사용량 - 일별 데이터
@app.get("/GetAhuTotalPowerDaily")
async def GetAhuTotalPowerDaily(startDate: str, endDate: str):
    result = await BemsService.GetAhuTotalPowerDaily(startDate, endDate)
    return Response(content=result, media_type="application/json")


@app.get("/GetChillerTotalPowerDaily")
async def GetChillerTotalPowerDaily(startDate: str, endDate: str):
    result = await BemsService.GetChillerTotalPowerDaily(startDate, endDate)
    return Response(content=result, media_type="application/json")


@app.get("/GetBoilerTotalPowerDaily")
async def GetBoilerTotalPowerDaily(startDate: str, endDate: str):
    result = await BemsService.GetBoilerTotalPowerDaily(startDate, endDate)
    return Response(content=result, media_type="application/json")


@app.get("/GetBoilerTotalGasDaily")
async def GetBoilerTotalGasDaily(startDate: str, endDate: str):
    result = await BemsService.GetBoilerTotalGasDaily(startDate, endDate)
    return Response(content=result, media_type="application/json")



# 총사용량 - 시간별 데이터
@app.get("/GetAhuTotalPowerHourly")
async def GetAhuTotalPowerHourly(runDate: str):
    result = await BemsService.GetAhuTotalPowerHourly(runDate)
    return Response(content=result, media_type="application/json")


@app.get("/GetChillerTotalPowerHourly")
async def GetChillerTotalPowerHourly(runDate: str):
    result = await BemsService.GetChillerTotalPowerHourly(runDate)
    return Response(content=result, media_type="application/json")


@app.get("/GetBoilerTotalPowerHourly")
async def GetBoilerTotalPowerHourly(runDate: str):
    result = await BemsService.GetBoilerTotalPowerHourly(runDate)
    return Response(content=result, media_type="application/json")


@app.get("/GetBoilerTotalGasHourly")
async def GetBoilerTotalGasHourly(runDate: str):
    result = await BemsService.GetBoilerTotalGasHourly(runDate)
    return Response(content=result, media_type="application/json")



# AHU Temp Data 조회
@app.get("/GetAhuTempData")
async def GetAhuTempData(machine_num: int, startDate: str, endDate: str):
    result = await BemsService.GetAhuTempData(machine_num, startDate, endDate)
    return Response(content=result, media_type="application/json")



# AHU Power Data 조회
@app.get("/GetAhuPowerData")
async def GetAhuPowerData(machine_num: int, startDate: str, endDate: str):
    result = await BemsService.GetAhuPowerData(machine_num, startDate, endDate)
    return Response(content=result, media_type="application/json")



# Chiller CWST Data 조회
@app.get("/GetChillerCwstData")
async def GetChillerCwstData(machine_num: int, startDate: str, endDate: str):
    result = await BemsService.GetChillerCwstData(machine_num, startDate, endDate)
    return Response(content=result, media_type="application/json")



# Chiller Power Data 조회
@app.get("/GetChillerPowerData")
async def GetChillerPowerData(machine_num: int, startDate: str, endDate: str):
    result = await BemsService.GetChillerPowerData(machine_num, startDate, endDate)
    return Response(content=result, media_type="application/json")



# Boiler Gas Data 조회
@app.get("/GetBoilerGasData")
async def GetBoilerGasData(machine_num: int, startDate: str, endDate: str):
    result = await BemsService.GetBoilerGasData(machine_num, startDate, endDate)
    return Response(content=result, media_type="application/json")



# Boiler Power Data 조회
@app.get("/GetBoilerPowerData")
async def GetBoilerPowerData(machine_num: int, startDate: str, endDate: str):
    result = await BemsService.GetBoilerPowerData(machine_num, startDate, endDate)
    return Response(content=result, media_type="application/json")


'''
# AHU 구성
@app.get("/GetAHUConfiguration")
async def GetAHUConfiguration():
    result = await BemsService.GetAHUConfiguration()
    return Response(content=result, media_type="application/json")




# AHU 모든 정보
@app.get("/GetAHUInfos")
async def GetAHUInfos():
    result = await BemsService.GetAHUInfos()
    return Response(content=result, media_type="application/json")

# 개별 AHU 정보
@app.get("/GetAHUInfo")
async def GetAHUInfo(FAC_NAME: str):
    result = await BemsService.GetAHUInfo(FAC_NAME)
    return Response(content=result, media_type="application/json")

# AHU Data 조회
@app.get("/GetAHUData")
async def GetAHUData(ahu_id: str, startDate: str, endDate: str):
    result = await BemsService.GetAHUData(ahu_id, startDate, endDate)
    return Response(content=result, media_type="application/json")

# Dashboard 
@app.get("/GetAHUSetSupData")
async def GetAHUSetSupData(runDate: str):
    result = await BemsService.GetAHUSetSupData(runDate)
    return Response(content=result, media_type="application/json")
'''

def test_server():
    return


if __name__ == "__main__":    
    #test_server()
    uvicorn.run(app, host="0.0.0.0", port=8888)
    pass
