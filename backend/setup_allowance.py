import os
from dotenv import load_dotenv
from py_clob_client.client import ClobClient

# Load variables from your .env file
load_dotenv()

def main():
    print("🚀 Initializing CLOB Client for Proxy Wallet...")
    
    key = os.getenv("POLY_PRIVATE_KEY")
    funder = os.getenv("POLY_FUNDER")
    sig_type = int(os.getenv("POLY_SIG_TYPE", "2"))
    
    if sig_type != 2:
        print("⚠️ Warning: POLY_SIG_TYPE is not 2. For gasless proxy trading, it should be 2.")

    client = ClobClient(
        host="https://clob.polymarket.com",
        key=key,
        chain_id=137, 
        signature_type=sig_type,
        funder=funder
    )

    print("🔑 Deriving API credentials...")
    creds = client.create_or_derive_api_creds()
    client.set_api_creds(creds)
    print("✅ API Credentials active.")

    print("🔓 Setting USDC allowance for the Exchange contract...")
    try:
        # This tells your Proxy Wallet to allow the Polymarket exchange to move your USDC
        client.update_funder()
        print("✅ Allowance successfully set! Alpha-Z is fully unlocked.")
    except Exception as e:
        print(f"❌ Error setting allowance: {e}")

if __name__ == "__main__":
    main()