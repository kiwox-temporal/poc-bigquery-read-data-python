from google.cloud import bigquery
import time


def executeQueryBigDataTest():
    client = bigquery.Client()

    query = """
        SELECT *
        FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips` t1
    """
    inicio = time.time()
    query_job = client.query(query)  # Make an API request.

    print("The query data:")
    # print(query_job)
    for row in query_job:
        # Row values can be accessed by field name or index.
        print("trip_id={}, subscriber_type={}, bikeid={}".format(
            row["trip_id"], row["subscriber_type"], row["bikeid"]))

    fin = time.time()
    print(f'TIME FOR EXECUTE QUERY: {fin-inicio}')


def executeSimpleQuery():
    # Construct a BigQuery client object.
    client = bigquery.Client()

    query = """
        SELECT *
        FROM `test-project-350020.Pruebas.AllTables` t1
        ORDER BY t1.ModifiedDate DESC LIMIT 20000;
    """
    inicio = time.time()
    query_job = client.query(query)  # Make an API request.

    print("The query data:")
    # print(query_job)
    for row in query_job:
        # Row values can be accessed by field name or index.
        print("BusinessEntityID={}, rowguid={}, ModifiedDate={}".format(
            row["BusinessEntityID"], row["rowguid"], row["ModifiedDate"]))

    fin = time.time()
    print(f'TIME FOR EXECUTE QUERY: {fin-inicio}')


def executeComplexQuery():
    client = bigquery.Client()
    query = """
        SELECT *
        FROM `test-project-350020.Pruebas.BusinessEntity` t1
            INNER JOIN `test-project-350020.Pruebas.Person` t2
                ON t1.BusinessEntityID = t2.BusinessEntityID
            INNER JOIN `test-project-350020.Pruebas.PersonPhone` t3
                ON t3.BusinessEntityID = t2.BusinessEntityID
            INNER JOIN `test-project-350020.Pruebas.PhoneType` t4
                ON t3.PhoneNumberTypeID = t4.PhoneNumberTypeID
        ORDER BY t1.ModifiedDate DESC LIMIT 20000;
    """
    inicio = time.time()
    query_job = client.query(query)  # Make an API request.

    print("The query data:")
    # print(query_job)
    for row in query_job:
        # Row values can be accessed by field name or index.
        print("BusinessEntityID={}, rowguid={}, ModifiedDate={}".format(
            row["BusinessEntityID"], row["rowguid"], row["ModifiedDate"]))
    fin = time.time()
    print(f'TIME FOR EXECUTE QUERY: {fin-inicio}')


# executeSimpleQuery()
#executeComplexQuery()
executeQueryBigDataTest()
