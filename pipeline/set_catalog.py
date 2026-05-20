# Hive metastore catalog
from pyiceberg.catalog import load_catalog

data = Schema(
    NestedField(
        field_id=1,
        name="prodi",
        field_type=StringType(),
        required=False,
    )
)

class SetupCatalog:
    def __init__(self):
        self.catalog = None

    def setup():
        cat = load_catalog('default')
        cat.create_namespace('default')
        self.catalog = cat
        tbl = cat.create_table(
            'default.taxis',
            schema=data.schema
        )
    