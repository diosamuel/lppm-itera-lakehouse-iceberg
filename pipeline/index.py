from setup_minio import MinioS3

catalog = SetupCatalog(catalog_name="lppm", namespace="default").init()
catalog.create_namespace()
table = catalog.create_table("sipaper", schema=lppm_schema)
print("Table schema:", table.schema())

def uploadFile():
    s3 = MinioS3(endpoint_url="http://localhost:9000").initialize()
    result = s3.upload("insightera.pdf","./pdf/ardikasatria-proposal-penelitian-kepakaran-2025.pdf")
    print("Upload:", result)
    data = s3.load("insightera.pdf")
    print("Loaded bytes:", len(data))
    meta = s3.read_meta("insightera.pdf")
    print(meta)

uploadFile()

