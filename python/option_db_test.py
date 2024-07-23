import psycopg2

#Establishing the connection
conn = psycopg2.connect(
   database="stock-dumps", user='sd_admin', password='sdadmin@postgres', host='192.168.1.72', port= '5430'
)
#Setting auto commit false
conn.autocommit = True

#Creating a cursor object using the cursor() method
cursor = conn.cursor()

# Preparing SQL queries to INSERT a record into the database.
cursor.execute('''INSERT INTO options.ticker(
	ticker_id, trade_date, stock_name, expiry, strike_price, option_type)
	VALUES ( '141953', '2024-07-01', 'RELIANCE', '25JUL24', '2400', 'PE')''')

# Commit your changes in the database
conn.commit()
print("Records inserted........")

# Closing the connection
conn.close()