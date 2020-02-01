from flask import Flask, request
from flask_restful import Resource, Api
from APIFunctions import updateDailyStockDbByTicker, isDailyStockInDb, meanRevisionCalculator, movingDayAverage

app = Flask(__name__)
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
        print(result)
        return result

    #def post(self):
    #    some_json = request.get_json()
    #    return {'input was': some_json}


api.add_resource(UpdateDailyStockDb, '/UpdateDailyStockDb/')
api.add_resource(MeanRevision, '/MeanRevision/<string:ticker>')
api.add_resource(MovingDayAverage, '/MovingDayAverage/<string:ticker>&<int:days>')

if __name__ == '__main__':
    app.run(debug=True)