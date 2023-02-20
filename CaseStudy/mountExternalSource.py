# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount external Sources ...

# COMMAND ----------

ext_source = "/mnt/ext_source"
source_files_path = f"{ext_source}/source_files"
sparkDeltaTables = f"{ext_source}/sparkDeltaTables"

class MountExternalSource:      
       
    def mountexternalsources():
        print("Checking if Already Mounted ..")
        if any(mount.mountPoint == ext_source for mount in dbutils.fs.mounts()):
            print(f"External Location is already Mounted {ext_source}")
            dbutils.fs.unmount(ext_source)
            dbutils.fs.mount(source = "wasbs://dbadls@storageaccountwtc.blob.core.windows.net/",
            mount_point = f"{ext_source}",
            extra_configs = {"fs.azure.account.key.storageaccountwtc.blob.core.windows.net":"K6UmpyotYd9yjnXJhqJBrbVxv9fN///DvL38V6UVoTs1z659R7U5U09F3/HezxG58RgadhmL7PvX+AStmX+Ubw=="})
            dbutils.fs.ls(ext_source)
            print("External Location has been Mounted")          
        else:
            print(f"Not already Mount {ext_source}")
            dbutils.fs.mount(source = "wasbs://dbadls@storageaccountwtc.blob.core.windows.net/",
            mount_point = f"{ext_source}",
            extra_configs = {"fs.azure.account.key.storageaccountwtc.blob.core.windows.net":"K6UmpyotYd9yjnXJhqJBrbVxv9fN///DvL38V6UVoTs1z659R7U5U09F3/HezxG58RgadhmL7PvX+AStmX+Ubw=="})
            dbutils.fs.ls(ext_source)
            print("External Location has been Mounted")  
    
        
        


# COMMAND ----------

MountExternalSource.mountexternalsources()