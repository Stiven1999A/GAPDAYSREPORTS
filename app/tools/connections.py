"""Script witht the functions to connect the data base"""
import os
import pyodbc
from sqlalchemy import create_engine

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

def pyodbc_connection(connection_string=None):
    """Establishes a connection to Azure SQL Managed Instance."""
    if connection_string is None:
        connection_string = connection_string_builder()
    try:
        conn = pyodbc.connect(connection_string)
        return conn

    except pyodbc.Error as e:
        print("❌ Database connection failed.")
        print("Error details:", e)
        return None

def engine_connection_string_builder():
    """Builds the SQLAlchemy connection string for the database."""
    server = os.getenv("DB_SERVER")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    odbc_driver = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

    missing = []
    if not db_user:
        missing.append("DB_USER")
    if not db_password:
        missing.append("DB_PASSWORD")
    if missing:
        raise SystemExit(f"Missing required env vars: {', '.join(missing)}")

    # Build SQLAlchemy connection string
    connection_string = (
        f"mssql+pyodbc://{db_user}:{db_password}@{server}/{db_name}"
        f"?driver={odbc_driver.replace(' ', '+')}"
    )
    return connection_string

def create_sqlalchemy_engine(connection_string=None):
    """Creates a SQLAlchemy engine for the database connection."""
    if connection_string is None:
        connection_string = engine_connection_string_builder()
    try:
        engine = create_engine(connection_string, fast_executemany=True)
        return engine
    except Exception as e:
        print("❌ SQLAlchemy engine connection failed.")
        print("Error details:", e)
        return None

def alchemy_connection(engine=None):
    """Establishes a connection using SQLAlchemy engine."""
    if engine is None:
        connection_string = engine_connection_string_builder()
        engine = create_sqlalchemy_engine(connection_string)
    try:
        conn = engine.connect()
        return conn
    except Exception as e:
        print("❌ SQLAlchemy connection failed.")
        print("Error details:", e)
        return None

if __name__ == "__main__":
    connection_string = engine_connection_string_builder()
    engine = create_sqlalchemy_engine(connection_string)
    conn = alchemy_connection(engine)
