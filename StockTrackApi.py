from flask import Flask, request
from flask_restful import Resource, Api
from flask_cors import CORS
from APIFunctions import updateDailyStockDbByTicker, isDailyStockInDb, meanRevisionCalculator, movingDayAverage, isDailyStockUpToDate, getHistoricalData, getLatestStocksFromDb, getPreviousDayStockFromDb, getSAndP500

app = Flask(__name__)
CORS(app, origins="http://localhost:4200", allow_headers=[
    "Content-Type"])
api = Api(app)

#API Endpoint to update daily stock DB using POST
class UpdateDailyStockDb(Resource):
    #def get(self, ticker):
    #    return {'Rows Inserted': updateDailyStockDbByTicker(ticker)}

    def post(self):
        request_json = request.get_json()
        return {'Rows Inserted': updateDailyStockDbByTicker(request_json['ticker'])}

#API Endpoint to pull mean revision from DB for ticker using GET
class MeanRevision(Resource):
    def get(self, ticker):
        result = []
        if (isDailyStockInDb(ticker)):
            result = meanRevisionCalculator(ticker)
        else:
            updateDailyStockDbByTicker(ticker)
            result = meanRevisionCalculator(ticker)
        return {'Average 50': result[0], 'Average 200': result[1]}

    #def post(self):
    #    some_json = request.get_json()
    #    return {'input was': some_json}

#API Endpoint to pull moving day average from DB for ticker using GET
class MovingDayAverage(Resource):
    def get(self, ticker, days):
        result = movingDayAverage(ticker, days)
        return {"MovingDayAverage": result[0], "m": result[1], "b": result[2]}

    #def post(self):
    #    some_json = request.get_json()
    #    return {'input was': some_json}

class StockUpToDate(Resource):
    def get(self, ticker):
        return {"UpToDate": isDailyStockUpToDate(ticker)}

class GetHistoricalData(Resource):
    def get(self, ticker):
        return {"HistoricalData": getHistoricalData(ticker)}

class GetLatestStocksFromDb(Resource):
    def get(self):
        return {"LatestStocks": getLatestStocksFromDb()}


class GetPreviousDayStockFromDb(Resource):
    def get(self):
        return {"PreviousStocks": getPreviousDayStockFromDb()}

class GetSAndP500(Resource):
    def get(self):
        result = getSAndP500()
        return {"SAndP500": result[0][6]}


api.add_resource(UpdateDailyStockDb, '/UpdateDailyStockDb/')
api.add_resource(MeanRevision, '/MeanRevision/<string:ticker>')
api.add_resource(MovingDayAverage, '/MovingDayAverage/<string:ticker>&<int:days>')
api.add_resource(StockUpToDate, '/StockUpToDate/<string:ticker>')
api.add_resource(GetHistoricalData, '/GetHistoricalData/<string:ticker>')
api.add_resource(GetLatestStocksFromDb, '/GetLatestStocksFromDb/')
api.add_resource(GetPreviousDayStockFromDb, '/GetPreviousDayStockFromDb/')
api.add_resource(GetSAndP500, '/GetSAndP500/')

if __name__ == '__main__':
    app.run(debug=True)