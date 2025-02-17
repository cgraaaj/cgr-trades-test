from sqlalchemy import create_engine, text
from urllib.parse import quote
import pandas as pd

def main ():
    engine = create_engine(
        "postgresql+psycopg2://sd_admin:%s@192.168.1.72:5430/stock-dumps"
        % quote("sdadmin@postgres")
    )
    instrument_df = pd.read_csv('instrument_test.csv')
    existing_ids = pd.read_sql("SELECT id FROM options.instrument", engine)['id'].apply(lambda x: str(x)).tolist()
    df_filtered = instrument_df[~instrument_df['id'].isin(existing_ids)]  # Keep only new rows

    # Insert only new data
    df_filtered.to_sql(
        "instrument", schema="options", if_exists="append", con=engine, index=False
    )
    print("done")
    
if __name__ == "__main__":
	main()