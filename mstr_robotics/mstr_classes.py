from mstrio.connection import Connection
from mstrio.utils import parser
from mstrio.project_objects.datasets import SuperCube
from mstrio.api import reports
from mstrio.api import browsing
from mstrio.api import objects as api_obj
from mstrio.object_management import folder
from mstrio.project_objects import Report
import pandas as pd
from mstr_robotics.helper import str_func
from mstr_robotics.logger import logger
from mstr_robotics.lu_data import lu_mstr_md
import uuid
str_func = str_func
log = logger


@log(err_name=f'Failed to open the a connection to the I-Server.')
def get_conn(self, base_url, *args,**kwargs):

    conn = Connection(base_url=base_url,*args,**kwargs)
    conn.headers['Content-type'] = "application/json"

    return conn


class rep:

    def __init__(self):
        self.i_reports = reports
        self.i_parser = parser


    @log(err_name="Failed to open change log report")
    def open_Instance(self, conn, project_id, report_id):
        conn.headers["X-MSTR-ProjectID"] = project_id
        rep_instance = self.i_reports.report_instance(connection=conn, report_id=report_id)
        return rep_instance.json()["instanceId"]

    @log(err_name="Faile to set prompt answers for change log report")
    def set_inst_prompt_ans(self, conn, report_id, instance_id, prompt_answ):
        prompt_answ_url = f'{conn.base_url}/api/reports/{report_id}/instances/{instance_id}/prompts/answers'
        ret_prompt_ans = conn.put(prompt_answ_url, data=prompt_answ)
        return ret_prompt_ans

    @log(err_name="Failed to execute or export change log report")
    def report_dict(self, conn, report_id, instance_id):
        #conn.headers["X-MSTR-ProjectID"] = project_id
        report_ds = self.i_reports.report_instance_id(connection=conn, report_id=report_id, instance_id=instance_id)
        s = conn.get(f'{conn.base_url}/api/v2/reports/{report_id}/instances/{instance_id}/sqlView')
        print(s.json())
        report_dict = self.i_parser.Parser(report_ds.json())._Parser__map_attributes(report_ds.json())
        return report_dict

    @log(err_name="Failed to execute or export change log report")
    def report_df(self, conn, report_id, instance_id):
        self.i_po_Report = Report(connection=conn, id=report_id, instance_id=instance_id)
        df=self.i_po_Report.to_dataframe()
        return self.i_po_Report.to_dataframe()

    @log(err_name="Failed to read out report_definition")
    def get_report_def(self, conn, report_id):
        return self.i_reports.report_definition(connection=conn, report_id=report_id)

    @log(err_name='Something is probally wrong with your prompt answers')
    def report_has_data(self, conn, report_id,instance_id):
        report_has_data_fg=False
        report_ds = self.i_reports.report_instance_id(connection=conn, report_id=report_id, instance_id=instance_id)

        if len(report_ds.json()["definition"]["grid"]["rows"][0]["elements"])>0:
            report_has_data_fg=True

        return report_has_data_fg

class mstr_global:

    def __init__(self):
        self.i_api_obj = api_obj
        self.str = str_func
        self.brow = browsing

    @log(err_name='Failed to find / open the given project')
    def set_project_id(self, conn, project_id):
        conn.select_project(project_id)
        return conn

    @log(err_name="Failed to upload cube")
    def cube_upload(self, conn, load_df, tbl_name, updatePolicy="REPLACE", folder_id=None, cube_name=None,
                    mtdi_id=None):
        if mtdi_id == None or mtdi_id =="":
            ds = SuperCube(connection=conn, name=cube_name)
            ds.add_table(name=tbl_name, data_frame=load_df, update_policy=updatePolicy)
            ds.create(folder_id=folder_id)
        else:
            ds = SuperCube(connection=conn, id=mtdi_id)
            ds.add_table(name=tbl_name, data_frame=load_df, update_policy=updatePolicy)
            ds.update()
        return ds.id

    #@log(err_name="Failed to read out objects from Folder")
    def get_folder_obj_l(self, conn, project_id, folder_id):

        #existing_obj_l=conn.get(f'{conn.base_url}/api/folders/{folder_id}?limit=-1')
        i_folder = folder.Folder(connection=conn, id=folder_id)
        existing_obj_l = i_folder.get_contents(to_dictionary=True)
        return existing_obj_l


    @log(err_name="Failed to rename object")
    def _rename_object(self, conn,project_id,object_id,object_type, new_name, desc_str):

        #short_cut_obj = object.Object(connection=conn, type=object_type, id=object_id)
        #"http://rs002312.fastrootserver.de:8080/MicroStrategyLibrary/api/objects/31CB97694D39D44FD8DF1782D1E0D71D?type=18&flags=70"13042
        base_url=f'{conn.base_url}/api/objects/{object_id}?type={object_type}&flags=70'
        try:
            #short_cut_obj.alter(name=str(new_name) , description=str(desc_str))
            body='{'+f'"name":"{new_name}","description":"{str(desc_str)}"'+'}'
            resp=conn.put(base_url,data=str(body))
        except:
            #short_cut_obj.alter(name=str(new_name+ uuid.uuid1().hex), description=str(desc_str))
            body={"name":str(new_name + uuid.uuid1().hex), "description":str(desc_str)}
            body = '{' + f'"name":"{new_name}{uuid.uuid1().hex}","description":"{str(desc_str)}"'+'}'
            resp=conn.put(base_url,data=str(body))

        return resp

    @log(err_name="Didn't find object or could parse it")
    def get_object_info(self, conn, project_id, object_id, type):
        return self.i_api_obj.get_object_info(connection=conn,
                                              id=object_id,
                                              object_type=type,
                                              project_id=project_id)

    @log(err_name="Could not delete object")
    def _delete_object(self, conn, project_id, existing_obj_l):
        self.set_project_id(conn=conn, project_id=project_id)
        for o in existing_obj_l:
            self.i_api_obj.delete_object(connection=conn, id=o["id"]
                                         , object_type=o["type"])
        return

    @log(err_name="Failed to create short cut")
    def _cr_short_cut(self, conn, object_id, object_type, folder_id):
        short_cut_folder_j = f'{{"folderId": "{folder_id}"}}'
        short_cut_url = f'{conn.base_url}/api/objects/{object_id}/type/{str(object_type)}/shortcuts'
        short_cut_obj_j = conn.post(short_cut_url, data=short_cut_folder_j)
        return short_cut_obj_j

    def bld_obj_path(self, fld_d=None, proj_id=None, proj_name=None):
        path_s = "\\"
        for f in fld_d:
            # do not include project in the path
            if f["name"] != proj_name:
                path_s += f["name"] + "\\"

        path_s = self.str._rem_last_char(self, path_s)
        return path_s

    @log(err_name="Project not found")
    def get_project_name(self, conn, project_id):
        project_resp = conn.get(f'{conn.base_url}/api/projects')
        project_name=""
        for p in project_resp.json():
            if p["id"] == project_id:
                project_name= p["name"]

        if project_name!="":
            return project_name
        else:
            raise

    def bld_df_from_search_result(self, conn, search_l):
        full_obj_info_l = []
        for obj in search_l:
            obj_d = self.get_object_info(conn=conn,
                                         project_id=obj["project_id"],
                                         object_id=obj["object_id"],
                                         type=obj["object_type"]
                                         )
            obj_all_d=self.bld_obj_d(conn=conn, obj_d=obj_d.json(), project_id=obj["project_id"])
            if len(obj_all_d.keys())>3:
                full_obj_info_l.append(obj_all_d)
        return pd.DataFrame(full_obj_info_l, columns=list(full_obj_info_l[0].keys()))

    def used_by_obj_rec(self, conn, project_id, obj_l):

        """
        def_search=conn.post(f'{conn.base_url}/api/metadataSearches/results?pattern=4&domain=2&usedByObject={obj_l}&usedByRecursive=true&visibility=ALL')
        #f'{conn.base_url}/api/metadataSearches/results?pattern=4&domain=2&usedByObject=59D59D7741F61B68480572B9B7A05709;4&usedByRecursive=true&visibility=ALL')'
        """
        def_search = self.brow.store_search_instance(connection=conn, project_id=project_id,
                                                     used_by_object=obj_l,
                                                     used_by_one_of=True, used_by_recursive=True)

        get_search = self.brow.get_search_results(connection=conn, project_id=project_id,
                                                  search_id=def_search.json()["id"])
        search_obj_l = []
        max_search_obj = 50
        i = 0
        for o in get_search.json():
            if o["type"] != 11:
                search_obj_l.append({"project_id": project_id,
                                     "object_id": o["id"],
                                     "object_type": o["type"],
                                     "objid_type": o["id"] + ';' + str(o["type"])})
                i += 1
            if i == max_search_obj:
                return search_obj_l
        return search_obj_l

    def used_by_obj_rec_l(self, conn, project_id, obj_l):
        rec_obj_l = []
        for obj in obj_l:
            if obj not in rec_obj_l:
                try:
                    rec_obj_l += self.used_by_obj_rec(conn, project_id, obj)
                except Exception as err:
                    if str(err).find("ERR004")>0:
                        print("The object: "+obj + " does not exist in project: "+project_id )
                    else:
                        print(err)

        return [dict(t) for t in {tuple(d.items()) for d in rec_obj_l}]

    @log(err_name="Faild to read out with object infos")
    def bld_obj_d(self, conn, obj_d, project_id):
        val_l = []
        obj_row_d={}
        path=str(self.bld_obj_path(fld_d=obj_d["ancestors"], proj_id=project_id,
                                            proj_name=self.get_project_name(conn=conn, project_id=project_id)))
        if str_func._get_first_x_chars(self,str_=path,i=16) != "\\System Objects\\" and \
                    obj_d.get('type') in [1, 2, 3, 4, 6, 8, 10, 11, 12, 13, 14, 15, 18, 23, 34, 55]:
            obj_row_d = {"project_id": project_id,
                         "object_id": str(obj_d.get('id'))
                , "version": str(obj_d.get('version'))
                , "object_name": str(obj_d.get('name'))
                , "path": path
                , "type": str(obj_d.get('type'))
                , "type_bez":self.get_obj_type_name(obj_d.get('type'))["OBJECT_TYPE_BEZ"]
                , "subtype": str(obj_d.get('subtype'))
                , "owner_id": str(obj_d["owner"].get('id'))
                , "owner_name": str(obj_d["owner"].get('name'))
                , "date_modified": str(obj_d["dateModified"])
                , "date_created": str(obj_d["dateCreated"])
                         }
            #print(obj_row_d["date_created"] )
        return obj_row_d

    def get_obj_type_name(self,obj_type_id):
       obj_type_d = {"MD_OBJ_TYPE_ID": "", "MD_OBJ_TYPE_BEZ": "None"}
       for t in lu_mstr_md.lu_object_type(self):

           if str(obj_type_id)==str(t["MD_OBJ_TYPE_ID"]):
               obj_type_d["OBJECT_TYPE_ID"]=t["MD_OBJ_TYPE_ID"]
               obj_type_d["OBJECT_TYPE_BEZ"]=t["MD_OBJ_TYPE_BEZ"]

       return obj_type_d

    def get_obj_type_id(self,pa_obj_id):
       #obj_type = {"OBJECT_TYPE_ID": "", "OBJECT_TYPE_BEZ": "None"}
       obj_type_d={}

       for t in lu_mstr_md.lu_object_type(self):

           if str(pa_obj_id)==str(t["PA_OBJ_TYPE_ID"]):
               obj_type_d["OBJECT_TYPE_ID"]=t["MD_OBJ_TYPE_ID"]

       return obj_type_d

    def bld_mstr_obj_guid(self, obj_md_id=None):
        # in the MSTR MD, for what ever reason, they're changin
        # 2276AC06-7A55-473C-9AC4-35A4E28C8021
        # 2276AC06473C7A55A435C49A21808CE2
        mstr_obj_guid = obj_md_id[0:8]
        mstr_obj_guid += obj_md_id[14:18]
        mstr_obj_guid += obj_md_id[9:13]
        mstr_obj_guid += obj_md_id[26:28]
        mstr_obj_guid += obj_md_id[24:26]
        mstr_obj_guid += obj_md_id[21:23]
        mstr_obj_guid += obj_md_id[19:21]
        mstr_obj_guid += obj_md_id[34:36]
        mstr_obj_guid += obj_md_id[32:34]
        mstr_obj_guid += obj_md_id[30:32]
        mstr_obj_guid += obj_md_id[28:30]
        return mstr_obj_guid

    def bld_mstr_obj_md_guid(self, obj_md_id=None):
        # in the MSTR MD, for what ever reason, they're changin
        # 35F616224B5A80B5 FDCA6BA77BC799F9
        # ist: '35F61622-4B5A-80B5-FD-CA-6B-A7-C7-99-F9
        #       35F61622-80B5-4B5A-A76BCA-FDF999C77B
        # soll: '35F61622-80B5-4B5A-A76B-CAFDF999C77B
        mstr_obj_guid = obj_md_id[0:8] + "-"
        mstr_obj_guid += obj_md_id[12:16] + "-"
        mstr_obj_guid += obj_md_id[8:12] + "-"
        mstr_obj_guid += obj_md_id[22:24]
        mstr_obj_guid += obj_md_id[20:22] + "-"
        mstr_obj_guid += obj_md_id[18:20]
        mstr_obj_guid += obj_md_id[16:18]
        mstr_obj_guid += obj_md_id[30:32]
        mstr_obj_guid += obj_md_id[28:30]
        mstr_obj_guid += obj_md_id[26:28]
        mstr_obj_guid += obj_md_id[24:26]

        return mstr_obj_guid
