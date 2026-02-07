"""
Flask API Server for Stock Chart UI
Integrates with the existing stock analysis data
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import pickle
import os
from datetime import datetime
import glob

app = Flask(__name__)
CORS(app)  # Enable CORS for React app

# Base directory for data files
DATA_DIR = os.path.dirname(os.path.abspath(__file__))


def get_available_dates():
    """Get available trade dates from pickle files"""
    files = glob.glob(os.path.join(DATA_DIR, 'analyzed_stocks_data_optimized_*.pickle'))
    dates = []
    for file in files:
        # Extract date from filename
        parts = file.split('_')
        if len(parts) >= 5:
            date = parts[-3]  # Gets the date part
            if date not in dates:
                dates.append(date)
    return sorted(dates, reverse=True)


def load_stock_data(trade_date):
    """Load stock data from pickle file"""
    try:
        filename = f'analyzed_stocks_data_optimized_{trade_date}_to_{trade_date}.pickle'
        filepath = os.path.join(DATA_DIR, filename)
        
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
        return data
    except Exception as e:
        print(f"Error loading data: {e}")
        return None


@app.route('/api/stock-data', methods=['GET'])
def get_stock_data():
    """Get stock chart data"""
    trade_date = request.args.get('tradeDate', '').replace('-', '')
    stock = request.args.get('stock', '')
    expiry_date = request.args.get('expiryDate', '')
    
    if not all([trade_date, stock, expiry_date]):
        return jsonify({'error': 'Missing required parameters'}), 400
    
    try:
        # Load analyzed data
        data = load_stock_data(trade_date)
        
        if data is None:
            # Return mock data if file doesn't exist
            return generate_mock_data(stock)
        
        # Extract stock-specific data
        # Modify this based on your actual data structure
        stock_info = data.get(stock, {})
        
        # Format data for frontend
        response = {
            'labels': stock_info.get('timestamps', generate_default_timestamps()),
            'volumes': stock_info.get('volumes', []),
            'prices': stock_info.get('prices', []),
            'openClose': stock_info.get('open_close', [])
        }
        
        return jsonify(response)
    
    except Exception as e:
        print(f"Error processing request: {e}")
        return generate_mock_data(stock)


@app.route('/api/trade-dates', methods=['GET'])
def get_trade_dates():
    """Get available trade dates"""
    dates = get_available_dates()
    
    if not dates:
        # Return default dates if no files found
        dates = ['20251016', '20250508']
    
    # Format dates as YYYY-MM-DD
    formatted_dates = [f"{d[:4]}-{d[4:6]}-{d[6:8]}" for d in dates]
    return jsonify(formatted_dates)


@app.route('/api/stocks', methods=['GET'])
def get_stocks():
    """Get available stock symbols"""
    # You can modify this to read from your database or config
    stocks = [
        'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 
        'META', 'NVDA', 'NIFTY', 'BANKNIFTY'
    ]
    return jsonify(stocks)


@app.route('/api/expiry-dates', methods=['GET'])
def get_expiry_dates():
    """Get available expiry dates"""
    # You can modify this to read from your database or config
    from datetime import datetime, timedelta
    
    today = datetime.now()
    dates = []
    
    # Generate next 4 weekly expiry dates (Thursdays)
    for i in range(4):
        days_ahead = (3 - today.weekday() + 7 * i) % 7 + 7 * i
        if days_ahead == 0:
            days_ahead = 7
        expiry = today + timedelta(days=days_ahead)
        dates.append(expiry.strftime('%Y-%m-%d'))
    
    return jsonify(dates)


def generate_default_timestamps():
    """Generate default intraday timestamps"""
    times = []
    for hour in range(9, 16):
        for minute in [0, 15, 30, 45]:
            if hour == 9 and minute < 15:
                continue
            if hour == 15 and minute > 30:
                continue
            times.append(f"{hour:02d}:{minute:02d}")
    return times


def generate_mock_data(stock):
    """Generate mock data for demonstration"""
    base_prices = {
        'AAPL': 150,
        'GOOGL': 2800,
        'MSFT': 300,
        'AMZN': 3200,
        'TSLA': 700,
        'META': 320,
        'NVDA': 450,
        'NIFTY': 19500,
        'BANKNIFTY': 44000
    }
    
    base_price = base_prices.get(stock, 100)
    times = generate_default_timestamps()
    
    import random
    
    response = {
        'labels': times,
        'volumes': [random.randint(10000, 40000) for _ in times],
        'prices': [base_price + random.uniform(-10, 10) for _ in times],
        'openClose': [base_price + random.uniform(-8, 8) for _ in times]
    }
    
    return jsonify(response)


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'message': 'Stock Chart API is running',
        'data_dir': DATA_DIR,
        'available_dates': get_available_dates()
    })


if __name__ == '__main__':
    print("=" * 60)
    print("Stock Chart API Server")
    print("=" * 60)
    print(f"Data directory: {DATA_DIR}")
    print(f"Available dates: {get_available_dates()}")
    print("\nStarting server on http://localhost:5000")
    print("=" * 60)
    
    app.run(debug=True, port=5000, host='0.0.0.0')
















