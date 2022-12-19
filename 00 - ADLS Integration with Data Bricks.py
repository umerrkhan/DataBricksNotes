###### ADLS Integration with Data Bricks can be done  #####
# 1. Using ADLS access key Directly
# 2. Creating Mount Point using Access Key
# 3. Using Service Principle
# 4. Using Active Directory Credential Known as Credential Pass through


# OPTION - 1 - Using ADLS access key Directly

spark.conf.set(
"fs.azure.account.key.storageaccountwtc.dfs.core.windows.net",
    "ENMVA6hLfXRhiYoKOnb4jyVjvpjpelASpG82zHxOmWTX6vdW0/hQluFELX8sahj+C+mOCCee94jF-232fsaqACS=="
)
fileLocation = "abfss://container-source@storageaccountwtc.dfs.core.windows.net/"

display(
    spark.read.format("csv").option("inferschema","True").option("header","true").load(fileLocation)
)


# Option - 2 Creating Mount Point using Access Key

dbutils.fs.mount(
source = "wasbs://container-source@storageaccountwtc.blob.core.windows.net/",
    mount_point = "/mnt/This_is_my_mount_point/",    
    extra_configs = {"fs.azure.account.key.storageaccountwtc.blob.core.windows.net":"ENMVA6hLfXRhiYoKOnb4jyVjvpjpelASpG82zHxOmWTX6vdW0/hQluFELX8sahj+C+mOCCee94jFqscvfewasd=="}
)

dbutils.fs.ls("/mnt/This_is_my_mount_point/")
display(
    spark.read.format("csv").option("inferschema","True").option("header","true").load("/mnt/This_is_my_mount_point/")
)
