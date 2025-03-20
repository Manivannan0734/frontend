from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import os
from dotenv import load_dotenv
from urllib.parse import urlparse

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)
CORS(app) 


# Parse the DATABASE_URL from .env file
DATABASE_URL = os.getenv("DATABASE_URL")

# Parse the URL to extract the credentials
url = urlparse(DATABASE_URL)
DATABASE_SETTINGS = {
    "dbname": url.path[1:],
    "user": url.username,
    "password": url.password,
    "host": url.hostname,
    "port": url.port
}

@app.route("/api/etf_data", methods=["GET"])
def get_etf_data():
    try:
        date_param = request.args.get('date', None)
        
        if date_param:
            # Convert date from YYYY-MM-DD to YYYYMMDD format
            formatted_date = date_param.replace("-", "")
        else:
            return jsonify({"error": "Date parameter is required."}), 400
        
        conn = psycopg2.connect(**DATABASE_SETTINGS)
        cursor = conn.cursor()

        # Query to get ETF details
        etf_query = """
            SELECT etf_code, etf_name, fund_cash_component, shares_outstanding, fund_date
            FROM ETFSummary
            WHERE fund_date = %s
            ORDER BY etf_code
        """
        cursor.execute(etf_query, (formatted_date,))
        etf_rows = cursor.fetchall()

        etf_data = []
        for etf_row in etf_rows:
            etf_code = etf_row[0]

            # Query to get stock details for each ETF
            stock_query = """
                SELECT stock_code, stock_name, isin, exchange, currency, shares_amount, stock_price
                FROM ETFDetail
                WHERE etf_code = %s
                ORDER BY stock_code
            """
            
            cursor.execute(stock_query, (etf_code,))
            stock_rows = cursor.fetchall()

            # Format data
            etf_data.append({
                "etf_code": etf_row[0],
                "etf_name": etf_row[1],
                "fund_cash_component": etf_row[2],
                "shares_outstanding": etf_row[3],
                "fund_date": etf_row[4],
                "stocks": [
                    {
                        "stock_code": stock_row[0],
                        "stock_name": stock_row[1],
                        "isin": stock_row[2],
                        "exchange": stock_row[3],
                        "currency": stock_row[4],
                        "shares_amount": stock_row[5],
                        "stock_price": stock_row[6]
                    }
                    for stock_row in stock_rows
                ]
            })

        cursor.close()
        conn.close()
        return jsonify(etf_data)

    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/solactive_etf_details", methods=["GET"])
def get_solactive_etf_details():
    try:
        date_param = request.args.get('date', None)

        if not date_param:
            return jsonify({"error": "Date parameter is required."}), 400

        # Convert date from YYYY-MM-DD to integer format for matching in the database
        formatted_date = date_param.replace("-", "")

        conn = psycopg2.connect(**DATABASE_SETTINGS)
        cursor = conn.cursor()

        # Query to fetch ETF details
        solactive_query = """
            SELECT id, etf_code, etf_name, fund_cash_component, shares_outstanding, fund_date
            FROM ETFSummary
            WHERE fund_date = %s
            ORDER BY id
        """
        cursor.execute(solactive_query, (formatted_date,))
        etf_rows = cursor.fetchall()

        etf_details = []
        etf_codes = []
        
        for row in etf_rows:
            etf_details.append({
                "id": row[0],
                "etf_code": row[1],
                "etf_name": row[2],
                "fund_cash_component": row[3],
                "shares_outstanding": row[4],
                "fund_date": row[5]
            })
            etf_codes.append(row[1])

        stock_details = []

        if etf_codes:
            # Query to fetch stock details for the fetched ETF codes
            solactive_stock_query = """
                SELECT  etf_code, stock_code, stock_name, isin, exchange, currency, shares_amount, stock_price
                FROM ETFDetail
                WHERE etf_code = ANY(%s)
                ORDER BY stock_code
            """
            cursor.execute(solactive_stock_query, (etf_codes,))
            stock_rows = cursor.fetchall()

            for row in stock_rows:
                stock_details.append({
                    "etf_code": row[0],
                    "stock_code": row[1],
                    "stock_name": row[2],
                    "isin": row[3],
                    "exchange": row[4],
                    "currency": row[5],
                    "shares_amount": row[6],
                    "stock_price": row[7]
                })

        cursor.close()
        conn.close()

        # Return both ETF and stock details
        return jsonify({
            "etf_details": etf_details,
            "stock_details": stock_details
        })

    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/sp_etf_details", methods=["GET"])
def get_sp_etf_details():
    try:
        date_param = request.args.get('date', None)
        if not date_param:
            return jsonify({"error": "Date parameter is required."}), 400

        # Convert date to the format in the database: YYYYMMDD
        formatted_date = date_param.replace("-", "")

        conn = psycopg2.connect(**DATABASE_SETTINGS)
        cursor = conn.cursor()

        # Query to fetch ETF details
        query = """
            SELECT etf_code, etf_name, fund_cash_component, shares_outstanding, fund_date
            FROM ETFSummary
            WHERE fund_date = %s
            ORDER BY etf_code
        """
        cursor.execute(query, (formatted_date,))
        etf_rows = cursor.fetchall()

        if not etf_rows:
            print(f"No data found for date: {formatted_date}")  # Debugging log
            return jsonify({"etf_details": [], "stock_details": []})

        etf_details = []
        etf_codes = []

        for row in etf_rows:
            etf_details.append({
                "etf_code": row[0],
                "etf_name": row[1],
                "fund_cash_component": row[2],
                "shares_outstanding": row[3],
                "fund_date": row[4]
            })
            etf_codes.append(row[0])

        # Fetch stock details for the matched ETF codes
        stock_details = []
        if etf_codes:
            stock_query = """
                SELECT etf_code, stock_code, stock_name, isin, exchange, currency, shares_amount, stock_price
                FROM ETFDetail
                WHERE etf_code = ANY(%s)
                ORDER BY stock_code
            """
            
            cursor.execute(stock_query, (etf_codes,))
            stock_rows = cursor.fetchall()

            for row in stock_rows:
                stock_details.append({
                    "etf_code": row[0],
                    "stock_code": row[1],
                    "stock_name": row[2],
                    "isin": row[3],
                    "exchange": row[4],
                    "currency": row[5],
                    "shares_amount": row[6],
                    "stock_price": row[7]
                })

        cursor.close()
        conn.close()

        return jsonify({"etf_details": etf_details, "stock_details": stock_details})

    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)