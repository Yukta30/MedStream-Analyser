import psycopg2
import yaml

# Load Redshift configuration
with open("config/db_config.yaml", "r") as f:
    db_config = yaml.safe_load(f)

def connect_redshift():
    """
    Connect to Redshift
    """
    conn = psycopg2.connect(
        dbname=db_config['redshift']['jdbc_url'].split("/")[-1],
        user=db_config['redshift']['user'],
        password=db_config['redshift']['password'],
        host=db_config['redshift']['jdbc_url'].split("//")[1].split(":")[0],
        port=5439
    )
    return conn

def search_hospital_by_service(service, location=None):
    """
    Search hospitals by service and location
    """
    conn = connect_redshift()
    cursor = conn.cursor()

    query = "SELECT hospital_name, location, vacancies FROM hospital_data WHERE service = %s"
    params = [service]

    if location:
        query += " AND location = %s"
        params.append(location)

    cursor.execute(query, params)
    result = cursor.fetchall()

    for row in result:
        print(f"Hospital: {row[0]}, Location: {row[1]}, Vacancies: {row[2]}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    service = input("Enter the service you're looking for: ")
    location = input("Enter the location (optional): ")
    search_hospital_by_service(service, location)
