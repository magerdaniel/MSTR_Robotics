from mstr_robotics.mstr_classes import mstr_global
from mstr_robotics._connectors import mstr_api
from mstr_robotics._helper import str_func

class bld_short_cuts:

    def __init__(self):
        self.str= str_func()
        #self.rep = rep
        self.glob = mstr_global()
        self.mstr_api = mstr_api()

    def bld_short_cut(self,conn,folder_id,object_id,
                      object_type,new_name,object_desc):
        #In Version 11 update 7, it's not possible to define a free name and description
        # when creating a short cut. Thus I create the shurt cut first and rename it later
        short_cut_obj_rep = self.mstr_api.cr_short_cut(conn=conn,object_id=object_id,object_type=object_type,folder_id=folder_id)
        self.mstr_api.rename_object(conn=conn, object_id=short_cut_obj_rep.json()["id"], object_type=18, new_name=new_name, desc_str=object_desc)

    def run_short_cut_build(self, conn,project_id,short_cut_df,folder_id,df_key_cols_d=None):
        #requiers as imput a pandas data frame with 4 colmuns
        # object_id, object_type, object_name, object_desc
        conn.select_project(project_id)
        if not df_key_cols_d:
            df_key_cols_d={"object_id":"Object@GUID",
                           "object_type":"OBJECT_TYPE_ID",
                           "object_name":"Object@Name",
                           "object_desc":"OBJECT_DESC"}

        #before adding new short cuts to the shortcut folder, all shortcuts gonna be deleted
        self.clean_shortcut_folder(conn=conn,folder_id=folder_id)
        short_cut_folder_j = f'{{"folderId": "{folder_id}"}}'
        #iterrating throuh all the objects in the list
        short_cut_df.reset_index()
        for index, obj in short_cut_df.iterrows():
            #short_cut_url = f'{conn.base_url}/objects/{obj[2]}/type/{str(obj[4])}/shortcuts'
            try:
                if obj[df_key_cols_d["object_type"]]!=str(18):
                    print(obj[df_key_cols_d["object_id"]] +"____"+ str(obj[df_key_cols_d["object_type"]]))
                    print("Create short: "+obj[df_key_cols_d["object_name"]]+" of type: "+str(obj[df_key_cols_d["object_type"]]) )
                    self.bld_short_cut(conn=conn
                                       ,folder_id=folder_id
                                       ,object_id=obj[df_key_cols_d["object_id"]]
                                       ,object_type=obj[df_key_cols_d["object_type"]]
                                       ,new_name=obj[df_key_cols_d["object_name"]]
                                       #max lentgh of descriptions in MSTR
                                       ,object_desc=self.str._get_first_x_chars(str_=str(obj[df_key_cols_d["object_desc"]]),i=249)
                                       )
                else:
                    print(f'Obect: {obj[2]} is a shortcut. MSTR does not allow short cuts o short cuts,'
                          f' thus they are not supported'  )
            except Exception as  err:
                print(f'Obect: {obj[2]} hasn not been created. The error is {err}')

        return
    #@logger
    def clean_shortcut_folder(self,conn,folder_id):
        existing_obj_l = self.glob.get_folder_obj_l(conn=conn, folder_id=folder_id)
        obj_l = self._get_col_from_obj_l(dict_l=existing_obj_l, key_col="id")
        self.glob._delete_object(conn=conn,existing_obj_l=existing_obj_l)

    #@logger
    def _get_col_from_obj_l(self, dict_l=None, key_col=None):
        key_val_l = []
        for o in dict_l:
            key_val_l.append(o[key_col])
        return key_val_l