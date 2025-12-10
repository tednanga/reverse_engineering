# catalog = "studies"
# schema = dbName = db = "cern_growth"

# volume_name = "raw"
# volume_path = f"/Volumes/{catalog}/{db}/{volume_name}"


def get_full_table_name(table_name, catalog="studies", schema="cern_growth"):
    return f"{catalog}.{schema}.{table_name}"

def get_volume_path(volume_name="raw", catalog="studies", schema="cern_growth", *folders):
    base = f"/Volumes/{catalog}/{schema}/{volume_name}"
    if folders:
        return "/".join([base] + list(folders))
    return base
