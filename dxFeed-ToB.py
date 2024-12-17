from px_snapshot_tt import px_flow
from datetime import datetime
import sqlite3
import pandas as pd
import platform
from discordwebhook import Discord
import yaml


class DiscordMessenger:
    """Handles sending messages to Discord webhooks."""
    def __init__(self, alerts_url, logs_url):
        self.alerts_url = alerts_url
        self.logs_url = logs_url

    def post_message(self, url, message):
        """Send a message to a specified Discord webhook."""
        discord = Discord(url=url)
        discord.post(content=message)

    def post_alert(self, message):
        """Send alert messages."""
        self.post_message(self.alerts_url, message)

    def post_log(self, message):
        """Send log messages."""
        self.post_message(self.logs_url, message)


class ToBSnapshot:
    """Handles top-of-book (ToB) snapshot processing and data storage."""
    def __init__(self):
        self.tickers_file, self.ds_file = self._set_file_paths()
        self.discord_urls = self._load_discord_credentials()
        self.symbols, self.thresholds = self._load_tickers()
        self.discord = DiscordMessenger(self.discord_urls["alerts"], self.discord_urls["logs"])

    def _set_file_paths(self):
        """Set file paths based on the operating system."""
        if platform.system() == "Darwin":
            return "tickers.yaml", "creds.yaml"
        return "/home/ec2-user/tt/tickers.yaml", "/home/ec2-user/tt/creds.yaml"

    def _load_discord_credentials(self):
        """Load Discord webhook credentials from a YAML file."""
        with open(self.ds_file, "r") as file:
            ddata = yaml.safe_load(file)
        return {
            "alerts": ddata["discord_alerts"][0],
            "logs": ddata.get("discord_url_logs")[0]
        }

    def _load_tickers(self):
        """Load ticker symbols and thresholds from a YAML file."""
        with open(self.tickers_file, "r") as f:
            tdata = yaml.safe_load(f)
        return tdata['tickers'], dict(zip(tdata['tickers'], tdata['ticker_threshold']))

    def process_snapshot(self):
        """Process market data and check for alerts."""
        pxi = px_flow(self.symbols)
        ts = datetime.now()
        prices = pxi.process_market_data()
        prices.insert(0, 'timestamp', ts)
        
        # Process and add new columns
        prices = prices[['timestamp', 'streamer-symbol', 'eventType', 'eventType2', 
                         'bidPrice', 'askPrice', 'midPrice', 'bidSize', 'askSize']]
        prices['bidoffer'] = prices['askPrice'] - prices['bidPrice']
        prices['bidoffer_pct'] = ((prices['bidoffer'] / prices['midPrice']) * 100).round(2)
        prices['bidoffer_bp'] = ((prices['bidoffer'] / prices['midPrice']) * 10000).round(1)
        prices['threshold'] = prices['streamer-symbol'].map(self.thresholds)
        
        # Send alerts if thresholds are breached
        for _, row in prices.iterrows():
            if row['bidoffer_pct'] > row['threshold'] * 100:
                message = (
                    f"Ticker: {row['streamer-symbol']}\n"
                    f"Bid: {row['bidPrice']}\n"
                    f"Ask: {row['askPrice']}\n"
                    f"Mid: {row['midPrice']}\n"
                    f"Bid Size: {row['bidSize']}\n"
                    f"Ask Size: {row['askSize']}\n"
                    f"Bid-Offer %: {row['bidoffer_pct']}\n"
                    f"Bid-Offer BP: {row['bidoffer_bp']}\n"
                    f"Threshold: {row['threshold']}\n"
                    "------------------------------------"
                )
                self.discord.post_alert(message)

        # Log completion
        self.discord.post_log("ToB snapshot completed.")
        return prices

    def save_to_sqlite(self, data, table_name="ToB_data"):
        """Set file paths based on the operating system."""
        if platform.system() == "Darwin":
            db_name = "ToB_data.sqlite"
        else:
            db_name = "/home/ec2-user/tt/ToB_data.sqlite"
        

        """Save data to a SQLite database."""
        conn = sqlite3.connect(db_name)
        try:
            table_exists = pd.read_sql_query(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';", conn
            )
            if table_exists.empty:
                data.to_sql(table_name, conn, index=False)
            else:
                data.to_sql(table_name, conn, if_exists='append', index=False)
        finally:
            conn.close()


if __name__ == "__main__":
    # Instantiate the class and run the process
    tob_snapshot = ToBSnapshot()
    snapshot_data = tob_snapshot.process_snapshot()
    print(snapshot_data)
    
    # Save snapshot data to SQLite
    tob_snapshot.save_to_sqlite(snapshot_data)
