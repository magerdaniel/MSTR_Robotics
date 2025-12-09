from mstrio.object_management.migration import \
    (

    Migration,
    PackageConfig,
    PackageContentInfo,
    PackageSettings
    )
from mstrio.types import ObjectTypes
from mstr_robotics._helper import msic

class migration():

    def __init__(self):

        self.obj_type=ObjectTypes
        self.i_msic=msic()

    def _set_pack_sett_defaults(self):
         packSett=PackageSettings(
            PackageSettings.DefaultAction.USE_EXISTING,
            PackageSettings.UpdateSchema.RECAL_TABLE_LOGICAL_SIZE,
            PackageSettings.AclOnReplacingObjects.REPLACE,
            PackageSettings.AclOnNewObjects.KEEP_ACL_AS_SOURCE_OBJECT,
                                )
         return packSett

    def _bld_pack_conf(self,pack_type,settings, mig_obj_l= None):
        self._set_pack_sett_defaults()
        return PackageConfig(type=pack_type,settings=settings,
                                 content=mig_obj_l)


    def _bld_migration(self, file_name, source_conn=None, target_conn=None, package_config=None):
        mig = Migration(
            save_path=file_name,
            source_connection=source_conn,
            target_connection=target_conn,
            configuration=package_config,
        )
        return mig

    def create_default_pack(self,file_name,source_conn,mig_obj_l):

        package_config=self._bld_pack_conf(mig_obj_l=mig_obj_l,
                                           settings=self._set_pack_sett_defaults(),
                                           pack_type=PackageConfig.PackageUpdateType.PROJECT)

        mig=self._bld_migration(file_name=file_name, source_conn=source_conn,
                                package_config=package_config)
        mig.create_package()

    def migrate_default_pack(self,file_name,target_conn):
        package_config=self._bld_pack_conf(pack_type=PackageConfig.PackageUpdateType.PROJECT
                                           ,settings=self._set_pack_sett_defaults())

        mig=self._bld_migration(file_name=file_name, target_conn=target_conn,
                                package_config=package_config)
        pass

    def clean_dbl_obj(self,mig_obj_l):
        return self.i_msic.rem_dbl_dict_in_l(mig_obj_l)


    def print_options(self):
        print("Options for Actions:")
        print(self.pack_setts.DefaultAction._member_names_)
        print("Options for update Schema")
        print(self.pack_setts.UpdateSchema._member_names_)
        print("Options for Acls for replacing objetcs:")
        print(self.pack_setts.AclOnReplacingObjects._member_names_)
        print("Options for Acls for new objetcs:")
        print(self.pack_setts.AclOnNewObjects._member_names_)
        for freak in self.obj_type.__dict__["_value2member_map_"].keys():
            print(str(self.obj_type.__dict__["_value2member_map_"][freak])[12:] + " = " + str(freak))

