"""Script witht the functions to connect the data base"""
import os
import pyodbc

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

def env_get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default

def connection_string_builder():
    """Builds the connection string for the database."""
    server = os.getenv("DB_SERVER")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    odbc_driver = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
    db_encrypt = os.getenv("DB_ENCRYPT", "yes")
    db_trust = os.getenv("DB_TRUST_SERVER_CERT", "no")
    db_login_timeout = env_get_int("DB_LOGIN_TIMEOUT", 8)
    db_timeout = env_get_int("DB_TIMEOUT", 15)

    missing = []
    if not db_user:
        missing.append("DB_USER")
    if not db_password:
        missing.append("DB_PASSWORD")
    if missing:
        raise SystemExit(f"Missing required env vars: {', '.join(missing)}")

    # Build connection string with explicit Login/Connection timeout
    connection_string = (
        f"DRIVER={{{odbc_driver}}};"
        f"SERVER={server};"
        f"DATABASE={db_name};"
        f"UID={db_user};"
        f"PWD={db_password};"
        f"Encrypt={db_encrypt};"
        f"TrustServerCertificate={db_trust};"
        f"LoginTimeout={db_login_timeout};"
        f"Connection Timeout={db_timeout};"
    )
    return connection_string

def connect_to_db(connection_string=None):
    """Establishes a connection to Azure SQL Managed Instance."""
    if connection_string is None:
        connection_string = connection_string_builder()
    try:
        conn = pyodbc.connect(connection_string)
        print("✅ Connection successful!")
        return conn

    except pyodbc.Error as e:
        print("❌ Database connection failed.")
        print("Error details:", e)
        return None

if __name__ == "__main__":
    connection_string = connection_string_builder()
    conn = connect_to_db(connection_string)
    print(conn)
