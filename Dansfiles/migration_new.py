#from mstrio.access_and_security.privilege import Privilege
import json
from time import sleep
import shutil
import os
from mstrio.connection import Connection
from mstrio.object_management.migration import (
    #bulk_full_migration,
    #bulk_migrate_package,
    PackageType,
    Migration,
    PackageConfig,
    #PackageContentInfo,
    PackageSettings
)
#from mstrio.types import ObjectTypes
#from mstrio.users_and_groups.user import User
#from mstr_robotics import migrate
from mstrio.object_management.migration.package import (
    Action,
    #ImportStatus,
    #ProjectMergePackageTocView,
    #ProjectMergePackageSettings,
    #TranslationAction,
    #PackageCertificationStatus,
    PackageStatus,
    #ValidationStatus,
)

#from mstrio.server.environment import StorageType


path="D:\\shared_drive\\Python\\mstr_robotics\\mstr_robotics\\user_d.json"


with open(path, 'r') as file:
    user_d = json.load(file)

project_id="B7CA92F04B9FAE8D941C3E9B7E0CD754"
#project_id="01770E1B45A0B84E88E5748B465719AD"
target_project_id="00F3103E41FF3743A057C3B5313595B7"
cube_upload_param={}

username=user_d["username"]
password=user_d["password"]
base_url="http://85.214.60.83:8080/MicroStrategyLibrary"

conn = Connection(base_url=base_url,username=username,
                  password=password,project_id=project_id)
conn.headers['Content-type'] = "application/json"



#SAVE_PATH = "D:\\shared_drive\\OM_Packages\\zttr"
#custom_package_path = "D:\\shared_drive\\OM_Packages"
#REPORT_ID = "3C36208948520A629AE0DD88D732C5E5"


# Create connections to both source and target environments
conn = Connection(base_url=base_url,username=username,
                  password=password,project_id=project_id)
conn.headers['Content-type'] = "application/json"

source_conn = Connection(base_url=base_url,username=username,
                  password=password,project_id=project_id)
conn.headers['Content-type'] = "application/json"
target_conn = Connection(base_url=base_url,username=username,
                  password=password,project_id=project_id)
conn.headers['Content-type'] = "application/json"

package_settings = PackageSettings(
    Action.USE_EXISTING,
    PackageSettings.UpdateSchema.RECAL_TABLE_LOGICAL_SIZE,
    PackageSettings.AclOnReplacingObjects.REPLACE,
    PackageSettings.AclOnNewObjects.KEEP_ACL_AS_SOURCE_OBJECT,
)

package_content_info = [
    {"id":"ACB99295488C76AA566DADAE2EC30FC5",
    "type":4,
    "action":"USE_EXISTING",
    "include_dependents":True
        },
    {"id": "FBA328574EC126D4A38B51AFEF5D7856",
     "type": 4,
     "action": "USE_EXISTING",
     "include_dependents": True
     }
]

package_config = PackageConfig(
    package_settings,  package_content_info # [ package_content_from_object]
)

#env_target = source_conn.environment
"""
SAVE_PATH = "D:\\shared_drive\\OM_Packages"
#custom_package_path = "D:\\shared_drive\\OM_Packages"
STORAGE_ALIAS = "DansDev"
STORAGE_LOCATION = SAVE_PATH
env_target.update_storage_service(
    storage_type=StorageType.FILE_SYSTEM, location=STORAGE_LOCATION, alias=STORAGE_ALIAS
)
"""
my_obj_mig = Migration.create_object_migration(
    connection=source_conn,
    toc_view=package_config,
    name="object_mig",
    project_id=source_conn.project_id,
)
while my_obj_mig.package_info.status == PackageStatus.CREATING:
    sleep(2)
    my_obj_mig.fetch()

mstr_mig_folder=source_conn.environment.storage_service.location
package_file = mstr_mig_folder +"/" +my_obj_mig.package_info.storage.path

#my_obj_mig
usr_migration_folder_name="Daniel_Migration"
usr_package_file_name="2024123_Demo"
migration_folder= source_conn.environment.storage_service.location  +"\\"+ usr_migration_folder_name +"\\"+ usr_package_file_name + "\\"

package_file=os.path.normpath(package_file)
my_obj_mig_path=shutil.move(package_file, migration_folder)
print(my_obj_mig_path)
print(package_file)
#SOURCE_FILE_PATH="D:\\shared_drive\\OM_Packages\\mstr\\shared\\migrations\\packages\\67C43ED6E3034D4FAF236FCFABF1F84B.mmp"
from_file_mig = Migration.migrate_from_file(
    connection=source_conn,
    file_path=my_obj_mig_path,
    package_type=PackageType.OBJECT,
    name="object_mig_from_local_storage",
    target_project_name="MicroStrategy Tutorial PreProd",
)
from_file_mig