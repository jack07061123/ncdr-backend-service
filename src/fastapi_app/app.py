
import os
import uvicorn
from dotenv import load_dotenv
from azure.cosmos.aio import CosmosClient
from fastapi import FastAPI, HTTPException, Query, Depends
from azure.cosmos import PartitionKey, exceptions

# 加载环境变量
load_dotenv()

app = FastAPI()

# 从环境变量中获取 Cosmos DB 的连接信息
COSMOS_DB_URI = os.getenv("COSMOS_DB_URI")
COSMOS_DB_KEY = os.getenv("COSMOS_DB_KEY")
DATABASE_NAME = "NCDR"
CONTAINER_NAME = "forest_polygon"

# 创建 Cosmos 客户端并在应用启动时连接数据库
@app.on_event("startup")
async def startup_db_client():
    try:
        print('Start to initialize Cosmos db clinet at starut up function')
        # Initialize Cosmos DB client
        app.cosmos_client = CosmosClient(COSMOS_DB_URI, COSMOS_DB_KEY)

        # Create or get database
        app.database = await app.cosmos_client.create_database_if_not_exists(DATABASE_NAME)

        # Create or get container
        app.container = await app.database.create_container_if_not_exists(
            id=CONTAINER_NAME,
            partition_key=PartitionKey(path="/id"),
            offer_throughput=400
        )
        print('end to initialize Cosmos db clinet at starut up function')
    except Exception as e:
        print(f"Failed to initialize Cosmos DB: {e}")
        raise HTTPException(status_code=500, detail="Failed to initialize Cosmos DB")

@app.on_event("shutdown")
async def shutdown_db_client():
    await app.cosmos_client.close()

@app.get("/")
async def read_root():
    return {"message": "Hello, Cosmos DB!"}

@app.get("/test/")
async def read_root():
    
    return {'data': 
                {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                    [
                        [
                        121.443068,
                        24.025319
                        ],
                        [
                        121.442094,
                        24.028031
                        ],
                        [
                        121.443074,
                        24.027125
                        ],
                        [
                        121.443071,
                        24.026222
                        ],
                        [
                        121.443068,
                        24.025319
                        ]
                    ]
                    ]
                },
                "properties": {
                    "forest_Type": "C3A09"
                }
                }
            }

@app.get("/items/")
async def read_items():
    if not hasattr(app, "container"):
        raise HTTPException(status_code=500, detail="Cosmos DB container not initialized")

    query = "SELECT TOP 1000 c.type, c.geometry, c.properties FROM c"
    try:
        items = [item async for item in app.container.query_items(query, enable_cross_partition_query=True)]
        return {'data': {"type": "FeatureCollection", 'features':items}}
    except exceptions.CosmosHttpResponseError as e:
        print(f"Query failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to query items from Cosmos DB")

# 示例 API: 根据ID查询单个数据项
@app.get("/item/{item_id}")
async def read_item(item_id: str):
    query = f"SELECT * FROM c WHERE c.id=@id"
    parameters = [{"name": "@id", "value": item_id}]
    
    try:
        items = [item async for item in app.container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        )]
        
        if not items:
            raise HTTPException(status_code=404, detail="Item not found")
        
        return {'data': {"type": "FeatureCollection", 'features':[items[0], ]}}
    
    except exceptions.CosmosHttpResponseError as e:
        print(f"Query failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to query item from Cosmos DB")

# API: 查询特定 forest_type 的数据项，支持分页和限制返回的数量
@app.get("/items/forest_type/{forest_type}")
async def read_items_by_forest_type(
    forest_type: str,
    limit: int = Query(default=1000, le=2000),  # Limit the number of results (default 10, max 100)
    continuation_token: str = None  # Optional continuation token for pagination
):

    query = "SELECT TOP @limit c.type, c.geometry, c.properties FROM c WHERE c.properties.forest_type = @forest_type"
    # query = "SELECT TOP @limit * FROM c WHERE c.properties.forest_type = @forest_type"
    parameters = [
        {"name": "@forest_type", "value": forest_type},
        {"name": "@limit", "value": limit}
    ]
    
    try:
        # Use by_page() for pagination and pass the continuation_token if available
        query_result = app.container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True,
            max_item_count=limit  # Limit the number of items per page
        ).by_page(continuation_token)

        items = []
        
        # Use async for loop to iterate over pages returned by by_page()
        async for page in query_result:
            async for item in page:
                items.append(item)  # Add all items from this page
        
        # Get the continuation token for the next page (if available)
        new_continuation_token = query_result.continuation_token
        
        return  {'data': {"type": "FeatureCollection", 'features':items},
                'continuation_token': new_continuation_token  # Include token for fetching next page 
                }
    
    except exceptions.CosmosHttpResponseError as e:
        print(f"Query failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to query items from Cosmos DB")