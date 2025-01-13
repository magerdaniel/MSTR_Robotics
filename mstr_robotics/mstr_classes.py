from mstrio.connection import Connection
from mstrio.api import browsing
from mstrio.types import ObjectTypes, ObjectSubTypes
from mstrio.api import objects as api_obj
from mstrio.object_management import folder
import pandas as pd
from mstr_robotics._helper import msic,str_func
from mstr_robotics.report import cube
from mstr_robotics._lu_data import lu_mstr_md
from mstr_robotics._connectors import mstr_api
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)

def get_conn(base_url,project_id=None, *args,**kwargs):
    conn = Connection(base_url=base_url,project_id=project_id,*args,**kwargs)
    conn.headers['Content-type'] = "application/json"
    return conn


class mstr_global:

    def __init__(self):
        self.i_api_obj = api_obj
        self.i_str_func=str_func()
        self.i_ObjectTypes=ObjectTypes
        self.i_mstr_api=mstr_api()
        self.md_searches=md_searches()

    def get_folder_obj_l(self, conn,  folder_id):
        #reads out the content of a folder
        i_folder = folder.Folder(connection=conn, id=folder_id)
        existing_obj_l = i_folder.get_contents(to_dictionary=True)
        return existing_obj_l

    def get_obj_from_sh_fold(self,conn, folder_id):
        obj_l = self.get_folder_obj_l(conn=conn, folder_id=folder_id)
        obj_det_l=[]
        for obj_d in obj_l:
            if obj_d["type"] == 18:
                obj_det_d = {}
                obj_det_d["project_id"] = conn.project_id
                obj_det_d["id"] = obj_d["target_info"]["id"]
                obj_det_d["name"] = obj_d["target_info"]["name"]
                obj_det_d["type"] = obj_d["target_info"]["type"]
                obj_det_d["type_bez"] = ObjectTypes(obj_d["target_info"]["type"]).name
                obj_det_d["sub_type"] = obj_d["target_info"]["subtype"]
                obj_det_d["sub_type_bez"] = ObjectSubTypes(obj_d["target_info"]["subtype"]).name
                obj_det_l.append(obj_det_d.copy())

        return obj_det_l

    def get_object_info(self, conn,  object_id, type):
        return self.i_api_obj.get_object_info(connection=conn,
                                              id=object_id,
                                              object_type=type,
                                              project_id=conn.headers["X-MSTR-ProjectID"])

    def get_object_info_d(self, conn, object_id, type,*args,**kwargs):
        #brings back the object properties including the folder path
        #managed objetcs are not supported, hence they are
        #simply removed
            obj_d=self.i_api_obj.get_object_info(connection=conn,
                                                  id=object_id,
                                                  object_type=type,
                                                  project_id=conn.headers["X-MSTR-ProjectID"]
                                                  )

            obj_info_d = self.bld_obj_d(conn=conn, obj_d=obj_d.json(),*args,**kwargs)

            return obj_info_d

    def _delete_object(self, conn, existing_obj_l):
        for o in existing_obj_l:
            self.i_api_obj.delete_object(connection=conn, id=o["id"]
                                         , object_type=o["type"])
        return

    def bld_obj_path(self, fld_d, proj_name):
        #brings back the parent folders as a string
        #in the result dict we store the ID's and the names
        path_name = "\\"
        path_ids = "_"
        for f in fld_d:
            # do not include project in the path
            if f["name"] != proj_name:
                path_name += f["name"] + "\\"
                path_ids += f["id"] +"_"

        path_name = self.i_str_func._rem_last_char(path_name)
        path_d={"path_name":path_name,"path_ids":path_ids}
        return path_d

    def get_obj_type_from_l(self,obj_l):
        obj_type_l=[]

        return obj_type_l


    def get_obj_name(self,conn,report_id,type):
        #the REST Api expects the type of the object...
        return self.i_api_obj.get_object_info(conn=conn,
                                      object_id=report_id,
                                      type=type).json()["name"]

    def bld_obj_d(self, conn, obj_d,*args,**kwargs):
        #transforms the out put of the REST API for a
        # #object into the logic of MSTR consultants / stakeholders
        #this means for example, the path as a full string
        val_l = []
        obj_row_d={}
        path=self.bld_obj_path(fld_d=obj_d["ancestors"],proj_name=self.i_mstr_api.get_project_name(conn=conn,project_id=conn.project_id))
        if self.i_str_func._get_first_x_chars(str_=str(path["path_name"]), i=16) != "\\System Objects\\" and \
                    obj_d.get('type') in [1, 2, 3, 4, 6, 8, 10, 11, 12, 13, 14, 15, 18, 23, 34, 55]:
            obj_row_d = {"project_id": conn.headers["X-MSTR-ProjectID"],
                         "id": str(obj_d.get('id'))
                , "version": str(obj_d.get('version'))
                , "name": str(obj_d.get('name'))
                , "path_name": str(path["path_name"])
                , "path_ids":str(path["path_ids"])
                , "type": str(obj_d.get('type'))
                , "type_bez":self.i_ObjectTypes(obj_d["type"]).name
                , "subtype": str(obj_d.get('subtype'))
                , "owner_id": str(obj_d["owner"].get('id'))
                , "owner_name": str(obj_d["owner"].get('name'))
                , "date_modified": str(obj_d["dateModified"])
                , "date_created": str(obj_d["dateCreated"])
                         }
        return obj_row_d

    def pa_get_obj_type_id(self, pa_obj_id):
        # in PA we have to deal with different primary keys
        # here we map the MD object types / names
       #obj_type = {"OBJECT_TYPE_ID": "", "OBJECT_TYPE_BEZ": "None"}
       obj_type_d={}

       for t in lu_mstr_md.lu_object_type(self):

           if str(pa_obj_id)==str(t["PA_OBJ_TYPE_ID"]):
               return t["MD_OBJ_TYPE_ID"]


class md_searches():
    #this class is in the middle of development
    #big challange of seaches is the execution time
    #the size of recrusive dependency searches
    #idea is to provide easy of use search entries as functions
    #an the logic for intermediate cubing or parallel executions
    # is done in the _internalFunctions

    def __init__(self,run_prop_d={}, dpn_prefix="dpn_"):
        self.brow = browsing
        self.i_api_obj=api_obj
        self.cube_it=cube()
        self.i_msic=msic()
        self.i_str_func=str_func()
        self.run_prop_d=run_prop_d
        self.dpn_prefix=dpn_prefix


    def search_for_type_l(self,conn,obj_l,dpn_fg=False,name=None,root_folder_id=None,
                          info_level = "base_path", count_only_fg=True,*args  ,**kwargs):

        kwargs = {"project_id" : conn.headers["X-MSTR-ProjectID"],"info_level":info_level,"count_only_fg":count_only_fg,
                  "dpn_fg":dpn_fg,
                  "mstr_search": { "object_types" : obj_l, "name" : name, "root" : root_folder_id}
                   }

        if dpn_fg == False:
            search_obj_l=self._run_search( conn,  **kwargs)

        return search_obj_l

    def search_for_used_in_obj_direct(self,conn,obj_l,dpn_fg=True,
                                    info_level = "base_path",
                                    count_only_fg=True,*args, **kwargs ):

        kwargs = {"project_id" : conn.headers["X-MSTR-ProjectID"],
                  "info_level": info_level,
                  "count_only_fg":count_only_fg,"dpn_fg":dpn_fg,
                  "mstr_search":{"uses_one_of" : False, "obj_val_uses_object":obj_l,
                                 "uses_recursive" : False
                                }
                 }

        if dpn_fg == False:
            search_obj_l=self._run_search( conn,  **kwargs)
        else:
            search_obj_l = self._run_depn_search(conn,obj_l, **kwargs)

        return search_obj_l

    def _run_search(self, conn , *args, **kwargs):
        search_obj_l = []

        try:
            serach_inst = self.brow.store_search_instance(conn,project_id=conn.project_id, **kwargs["mstr_search"]).json()
            search_obj_l.extend(self._paging_search_l(conn=conn, run_prop_d=self.run_prop_d, dpn_prefix=None,
                                                      serach_inst=serach_inst,
                                                      *args, **kwargs)
                                )

        except Exception as err:
            print(err)

        return search_obj_l

    def _run_depn_search(self, conn,obj_l,cube_to_loop_d=None, *args, **kwargs):
        a = 0
        i = 0
        search_obj_l = []
        mtdi_id = None
        for obj in obj_l:
            # print(obj)
            a += 1
            # i.e. 8D67910B11D3E4981000E787EC6DE8A4;53
            obj_type = str(obj["id"]) + ";" + str(obj["type"])
            if obj_type not in search_obj_l:
                run_prop_d = self.run_prop_d | obj
                mstr_search_d =self.i_str_func.replace_val_by_prefix(
                                                                        dict=kwargs["mstr_search"].copy(),
                                                                        prefix="obj_val_", obj_val=obj_type)
                serach_inst = self.brow.store_search_instance(conn,project_id=conn.project_id, **mstr_search_d).json()
                if serach_inst["totalItems"] > 0:
                    search_obj_l.extend(self._paging_search_l(conn=conn,
                                                              run_prop_d=run_prop_d,
                                                              serach_inst=serach_inst
                                                              , dpn_prefix=self.dpn_prefix, *args,
                                                              **kwargs)
                                        )
                elif kwargs["count_only_fg"]==False:
                        search_obj_l.append(
                            run_prop_d | self._bld_dummy_info( dpn_prefix=self.dpn_prefix, run_prop_d=run_prop_d, *args,
                                                                  **kwargs))
                elif kwargs["count_only_fg"]==True:
                        search_obj_l.append( run_prop_d | {"count_obj":0} )

                if cube_to_loop_d != None and len(search_obj_l) > 50:
                    if i == 0:
                        updatePolicy = "REPLACE"
                    else:
                        updatePolicy = "ADD"
                    i += 1
                    mtdi_id = self._add_search_to_cube(conn, search_obj_l, updatePolicy, cube_to_loop_d, mtdi_id)

        return search_obj_l

    def _add_search_to_cube(self,conn,search_obj_l,updatePolicy,cube_to_loop_d,mtdi_id):
        load_df = pd.DataFrame.from_dict(search_obj_l)
        load_upd_d_l = [{"df": load_df, "tbl_name": "load_df", "update_policy": updatePolicy}]
        mtdi_id = self.cube_it.upload_cube_mult_table(conn, mtdi_id=mtdi_id, tbl_upd_dict=load_upd_d_l,
                                                      cube_name=cube_to_loop_d["cube_name"],
                                                      folder_id=cube_to_loop_d["folder_id"], force=cube_to_loop_d["force"])
        print(str(len(search_obj_l)) + " rows load to cube")
        return  mtdi_id

    def _paging_search_l(self, conn, serach_inst, dpn_prefix, run_prop_d, max_count=100, limit = 50,
                          *args, **kwargs):
        search_obj_l = []
        offset = 0
        count_obj=0

        while serach_inst["totalItems"]>offset:
            try:
                search_result_d=self.brow.get_search_results(connection=conn,
                                                  search_id=serach_inst["id"],
                                                  project_id=conn.headers["X-MSTR-ProjectID"],
                                                  offset=offset, limit=limit)

                full_obj_info_l=self._extract_info_from_search(conn=conn, dpn_prefix=dpn_prefix, run_prop_d=run_prop_d,
                                                               search_result_d=search_result_d.json(), *args, **kwargs)

                if kwargs["count_only_fg"]==False or kwargs["dpn_fg"]==False :
                    search_obj_l.extend(full_obj_info_l)
                else:
                    count_obj+=len(full_obj_info_l)
                    search_obj_l=[run_prop_d|{"count_obj":str(count_obj)}]
                    if count_obj > max_count:
                        offset=serach_inst["totalItems"]+1

                #print(offset)
                offset+=limit
            except Exception as err:
                print(err)
                pass
        return search_obj_l

    def _extract_info_from_search(self, conn, search_result_d,dpn_prefix, run_prop_d, *args, **kwargs):
        #**kwargs["info_level"] defines how much information
        #to keep it simple we define information blocks

        full_obj_info_l = []
        obj_row_d={}
        for obj_d in search_result_d:
            try:
                if not "info_level" in kwargs.keys():
                    kwargs["info_level"]="base"
                if  kwargs["info_level"]=="base":
                    #contains only object information from the search
                    #optimized for performance
                    obj_row_d={"project_id" : conn.headers["X-MSTR-ProjectID"],
                                "id" : obj_d["id"],
                                "name": obj_d["name"],
                                "type" : obj_d["type"],
                                "subtype": obj_d["subtype"],
                                "dateCreated": obj_d["dateCreated"],
                                "dateModified": obj_d["dateModified"],
                                "version": obj_d["version"],
                                "owner_id": obj_d["owner"]["id"],
                                "owner_name": obj_d["owner"]["name"],
                                }

                elif kwargs["info_level"] =="base_path":
                    #include the folder full path as a string
                     obj_row_d = mstr_global().get_object_info_d(conn=conn,
                                 object_id=obj_d["id"],
                                 type=obj_d["type"],
                                 dpn_prefix=dpn_prefix,
                                 run_prop_d=run_prop_d,
                                 *args,**kwargs)

                if len(obj_row_d)!= 0:
                    #add prefix for dependencies
                    obj_row_d = self.i_msic.add_prefix_to_dict_keys(dict=obj_row_d, dpn_prefix=dpn_prefix)
                    full_obj_info_l.append(run_prop_d|obj_row_d)

            except:
                pass


        return full_obj_info_l

    def _bld_dummy_info(self, project_id, dpn_prefix, run_prop_d,*args,**kwargs):
        #to be able to load objects with and without dependencies
        #we need to provide dummy columns into the mtdi - cubes
        # for this we add those dicts to the rows
        obj_row_d={}
        if not "info_level" in kwargs.keys():
            obj_row_d = {"project_id": project_id,
                          "id": None,
                          "name": None,
                          "type": None,
                          "subtype": None,
                          "dateCreated": None,
                          "dateModified": None,
                          "version": None,
                          "owner_id": None,
                          "owner_name": None,
                          }
            obj_row_d=self.i_msic.add_prefix_to_dict_keys(dict=obj_row_d,dpn_prefix=dpn_prefix)

        elif kwargs["info_level"] == "base_path":
            obj_row_d = {"project_id": project_id,
                         "id": None
                        , "version": None
                        , "name": None
                        , "path": None
                        , "type": None
                        , "type_bez": None
                        , "subtype": None
                        , "owner_id": None
                        , "owner_name": None
                        , "date_modified": None
                        , "date_created": None
                        }
            obj_row_d=self.i_msic.add_prefix_to_dict_keys(dict=obj_row_d,dpn_prefix=dpn_prefix)
            obj_row_d = run_prop_d|obj_row_d
        return obj_row_d

    def _bld_objType_l(self, obj_dict_l):
        objType=[]
        for obj in obj_dict_l:
            objType.append(obj["id"] + ";"+str(obj["type"]))
        return objType

    def _exclude_derrived_att(self, att_l):
        clean_att_id_l=[]
        for att in att_l:
            if att["subtype"] not in [3078,3074]:
                clean_att_id_l.append(att["id"])

        return clean_att_id_l


