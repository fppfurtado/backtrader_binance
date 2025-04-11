# How to get Binance API Token:
# 1. Register your account at Binance https://www.binance.com/?ref=CPA_004RZBKQWK
# 2. Go to "API Management" https://www.binance.com/en/my/settings/api-management?ref=CPA_004RZBKQWK
# 3. Then push the button "Create API" and select "System generated"
# 4. In "API restrictions" enable "Enable Spot & Margin Trading"
# 5. Copy & Paste here "API Key" and "Secret Key"
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# P.S. If you use my referral link - Thanks a lot))
# If you liked this software => Put a star on github - https://github.com/WISEPLAT/backtrader_binance
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
import dotenv, os

class Config:
    def __init__(self):
        dotenv.load_dotenv(dotenv_path='.env', override=True)        
        self.BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
        self.BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')