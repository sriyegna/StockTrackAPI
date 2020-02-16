from flask import Flask, request
from flask_restful import Resource, Api
from flask_cors import CORS
from APIFunctions import updateDailyStockDbByTicker, isDailyStockInDb, meanRevisionCalculator, movingDayAverage, \
    isDailyStockUpToDate, getHistoricalData, getLatestStocksFromDb, getPreviousDayStockFromDb, getSAndP500

app = Flask(__name__)
# Define cross origin policy exceptions
CORS(app, origins="http://localhost:4200", allow_headers=[
    "Content-Type"])
api = Api(app)


# API Endpoint to update daily stock DB using POST
class UpdateDailyStockDb(Resource):
    # Will receive a POST with a single JSON containing value of ticker
    # Will update the DB with latest stock data for that ticker
    # Returns number of rows inserted
    def post(self):
        request_json = request.get_json()  # Parse JSON data
        return {'Rows Inserted': updateDailyStockDbByTicker(request_json['ticker'])}


# API Endpoint to pull mean revision from DB for ticker using GET
class MeanRevision(Resource):
    # Will receive a GET with a value for ticker
    # Will check if the stock is in DB, and update the DB if it is not
    # Will calculate the meanRevision and return the average for 50 days and 200 days
    def get(self, ticker):
        result = []  # Define array ahead of time due to scope
        if (isDailyStockInDb(ticker)):  # If stock is in DB
            # Calls meanRevisionCalculator in APIFunctions and passes the ticker
            result = meanRevisionCalculator(ticker)
        else:  # If stock is not in DB
            # Update the stock, then call meanRevisionCalculator in APIFunctions
            updateDailyStockDbByTicker(ticker)
            result = meanRevisionCalculator(ticker)
        return {'Average 50': result[0], 'Average 200': result[1]}

### Rename to populate graph???
# API Endpoint to pull moving day average from DB for ticker using GET
class MovingDayAverage(Resource):
    # Will receive a GET with ticker, n-day value to calculate, from and to date range
    def get(self, ticker, days, fromDate, toDate):
        # Call movingDayAverage from APIFunctions to get MDA, linear slope data, and actual historical data
        result = movingDayAverage(ticker, days, fromDate, toDate)
        # print(result)
        # Returns MDA array, slope of linear line, y-intercept of linear line, actual historical stock data
        return {"MovingDayAverage": result[0], "bestFitData": result[1], "histStockData": result[2]}

# API Endpoint to check if a stock is up to date
class StockUpToDate(Resource):
    # Will receive a GET with ticker value
    # Will return true if stock is up to date, or false if stock can be updated
    def get(self, ticker):
        return {"UpToDate": isDailyStockUpToDate(ticker)}

# API Endpoint to pull historical data from DB
class GetHistoricalData(Resource):
    # Will receive a GET with ticker value
    # Will return an array containing historical stock data
    def get(self, ticker):
        return {"HistoricalData": getHistoricalData(ticker)}

# API Endpoint to get the last set of latest stock data for all tickers from the DB
class GetLatestStocksFromDb(Resource):
    # Will receive a get with no parameters
    # Will return an array containing the latest stock data, one for each ticker in the DB
    def get(self):
        return {"LatestStocks": getLatestStocksFromDb()}

# API Endpoint to get the last set of previous day stock data for all tickers form the DB
class GetPreviousDayStockFromDb(Resource):
    # Will receive a get with no parameters
    # Will return an array containing the previous days stock data, one for each ticker in the DB
    def get(self):
        return {"PreviousStocks": getPreviousDayStockFromDb()}

# API Endpoint to get the latest close price of S&P 500
class GetSAndP500(Resource):
    # Will receive a get with no parameters
    # Will return the latest close value of S&P 500
    def get(self):
        result = getSAndP500()
        return {"SAndP500": result[0][6]}


api.add_resource(UpdateDailyStockDb, '/UpdateDailyStockDb/')
api.add_resource(MeanRevision, '/MeanRevision/<string:ticker>')
api.add_resource(MovingDayAverage, '/MovingDayAverage/<string:ticker>&<int:days>&<string:fromDate>&<string:toDate>')
api.add_resource(StockUpToDate, '/StockUpToDate/<string:ticker>')
api.add_resource(GetHistoricalData, '/GetHistoricalData/<string:ticker>')
api.add_resource(GetLatestStocksFromDb, '/GetLatestStocksFromDb/')
api.add_resource(GetPreviousDayStockFromDb, '/GetPreviousDayStockFromDb/')
api.add_resource(GetSAndP500, '/GetSAndP500/')

if __name__ == '__main__':
    app.run(debug=True)
