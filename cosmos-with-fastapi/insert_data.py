from azure.cosmos import CosmosClient
import os

URL = os.environ['URI']
KEY = os.environ['KEY']
client = CosmosClient(URL, credential=KEY)
DATABASE_NAME = 'NCDR'
database = client.get_database_client(DATABASE_NAME)
CONTAINER_NAME = 'forest_point_table'
container = database.get_container_client(CONTAINER_NAME)

for i in range(1, 10):
    container.upsert_item({
            'id': 'item{0}'.format(i),
            'productName': 'Widget',
            'productModel': 'Model {0}'.format(i)
        }
    )