import pyspark
from pyspark.sql import SparkSession
import pymongo
from bson.json_util import dumps as mongo_dumps
import oci

def main():

    # Config
    signer = oci.auth.signers.get_resource_principals_signer()
    object_storage_client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    namespace = object_storage_client.get_namespace().data
    bucket_name = "<<NOME_BUCKET>>"
    object_name = "<<NOME_ARQUIVO>>"
    content = collection_mongo()

    object_storage_client.put_object(
    namespace_name=namespace,
    bucket_name=bucket_name,
    object_name=object_name,
    content_type="application/json",
    put_object_body=content
    )

def collection_mongo():
    # String conexão Mongodb
    conn_str = "mongodb://<USER>:<PASSWORD>@<HOSTNAME>:<PORT>/<USER>?authMechanism=PLAIN&authSource=$external&ssl=true&retryWrites=false&loadBalanced=true"

    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    db = client.<USER>
    content = mongo_dumps(db.<COLLECTION>.find())
     
    return content


if __name__ == "__main__":
    main()