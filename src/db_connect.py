import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()  

def get_engine():

    host     = os.getenv("DB_HOST",     "localhost")
    port     = os.getenv("DB_PORT",     "5432")
    name     = os.getenv("DB_NAME",     "marine_intelligence")
    user     = os.getenv("DB_USER",     "postgres")
    password = os.getenv("DB_PASSWORD", "")

    url = f"postgresql://{user}:{password}@{host}:{port}/{name}"
    engine = create_engine(url)
    return engine


def test_connection():
 # on successful connection print postgis version
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT PostGIS_Version();"))
            version = result.fetchone()[0]
            print(f" Connected to database successfully.")
            print(f"   PostGIS version: {version}")
    except Exception as e:
        print(f"Connection failed: {e}")


if __name__ == "__main__":
    test_connection()
