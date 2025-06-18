import requests
import pandas as pd
from datetime import datetime, timedelta
import os

API_KEY = "a6d3fbdcfe732b2bcc756128c9821893"

class ExchangeRateExtractor:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "http://api.exchangeratesapi.io/v1/"
    
    def get_historical_rates(self, date, base_currency='EUR', symbols=None):
        url = f"{self.base_url}{date}"
        params = {
            'access_key': self.api_key,
            'base': base_currency
        }
        if symbols:
            params['symbols'] = ','.join(symbols)
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API Error: {response.status_code} - {response.text}")
    
    def extract_to_csv(self, output_file='/opt/airflow/scripts/exchange_rates.csv', days_back=30):
        currencies = ['USD', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF', 'CNY', 'PLN', 'VND']
        all_data = []
        for i in range(days_back):
            date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            try:
                print(f"Fetching data for {date}...")
                data = self.get_historical_rates(date, symbols=currencies)
                row = {
                    'date': data['date'],
                    'base_currency': data['base'],
                    'timestamp': data['timestamp']
                }
                target = data['rates'].get('VND')
                for currency, rate in data['rates'].items():
                    row[f'rate_{currency}_per_VND'] = target / rate if rate else None
                all_data.append(row)
            except Exception as e:
                print(f"Error fetching data for {date}: {e}")
                continue
        df = pd.DataFrame(all_data)
        df = df.sort_values('date')
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")
        print(f"Shape: {df.shape}")
        return df

if __name__ == "__main__":
    extractor = ExchangeRateExtractor(API_KEY)
    df = extractor.extract_to_csv('/opt/airflow/scripts/exchange_rates.csv', days_back=30)
    print("\nFirst few rows:")
    print(df.head()) 