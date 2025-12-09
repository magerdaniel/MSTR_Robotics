import pandas as pd
from mstr_robotics._connectors import mstr_api
from mstr_robotics.read_out_prj_obj import read_report
i_mstr_api=mstr_api()
i_read_report=read_report()

class dossier_global():

    pass

class doss_read_out_det():
    visual_d = {}
    visual_list=[]
    #doss_filt_d_l = []
    #doss_filt_select_d_l = []



    def __init__(self):
        self.vis_obj_l = []
        self.load_d={}




    def read_out_vis_d(self,conn, vis_def_d):
        vis_d = {
            "dossier_id": self.visual_d["dossier_id"],
            "chapter_key": self.visual_d["chapter_key"],
            "vis_key": vis_def_d["key"],
            "name": vis_def_d["name"],
            "isGrid": vis_def_d["isGrid"],
            "visualizationType": vis_def_d["visualizationType"]
        }
        grid_definition = vis_def_d["definition"]["grid"]
        #vis_cont_d=i_read_report.zread_out_grid(vis_d=vis_d, grid_definition=grid_definition,
        #                                        vis_att_l=self.vis_att_l, vis_met_l=self.vis_met_l)
        #print(grid_definition)
        grid_obj_l=i_read_report.read_out_grid(conn=conn,grid_definition=grid_definition)
        vis_obj_l_temp=[]
        for obj in grid_obj_l:
            self.visual_d.update(obj)
            vis_obj_l_temp.append(self.visual_d.copy())
        self.vis_obj_l.extend(vis_obj_l_temp)
        #self.vis_met_l.extend(grid_cont_d["grid_met_l"])

    def read_pages_hier_det(self,conn, chapter, instance_id):
        for page in chapter["pages"]:
            self.visual_d["page_key"] = page["key"]
            self.visual_d["page_name"] = page["name"]
            if "selectors" in page.keys():
                pass

            self.read_visual_hier_det(conn, page=page, instance_id=instance_id)

    def read_visual_hier_det(self,conn,page,instance_id):
        for v in page["visualizations"]:

            self.visual_d["visual_key"] = v["key"]
            self.visual_d["visual_name"] = v["name"]
            self.visual_d["visualizationType"] = v["visualizationType"]
            vis_def_d=i_mstr_api.get_dossier_detail(conn=conn, dossier_id=self.visual_d["dossier_id"],
                                          instance_id=instance_id, chapter_key=self.visual_d["chapter_key"]
                                          , vis_id=self.visual_d["visual_key"])
            self.read_out_vis_d(conn, vis_def_d)

    def run_read_out_doss_hier_det(self, conn, dossier_l):
        self.visual_list = []
        for dossier_id in dossier_l:
            try:
                instance_id=i_mstr_api.create_dossier_instance(conn,dossier_id)
                doss_hier = i_mstr_api.get_dossier_def(conn, dossier_id)
                # print(doss_hier)
                self.visual_d = {}
                self.visual_d["dossier_id"] = dossier_id
                self.visual_d["dossier_name"] = doss_hier['name']
                self.visual_d["error_msg"] = ''
                for chapter in doss_hier["chapters"]:
                    # print(chapter)
                    self.visual_d["chapter_key"] = chapter["key"]
                    self.visual_d["chapter_name"] = chapter["name"]
                    self.read_pages_hier_det(conn=conn,chapter=chapter, instance_id=instance_id)

            except Exception as err:
                #print(err)
                self.visual_d["dossier_id"] = dossier_id
                self.visual_d["dossier_name"] = ""
                self.visual_d["chapter_key"] = ""
                self.visual_d["chapter_name"] = ""
                self.visual_d["page_key"] = ""
                self.visual_d["page_name"] = ""
                self.visual_d["visual_key"] = ""
                self.visual_d["visual_name"] = ""
                self.visual_d["visualizationType"] = ""
                self.visual_d["error_msg"] = err
        #   print(visual_dict)
        return self.vis_obj_l

class doss_read_out():

    def __init__(self):
        self.doss_filt_d_l = []
        self.doss_filt_select_d_l = []

    def doss_hier_to_df(self, conn, dossier_l):
        doss_hier_l = self.run_read_out_doss_hier(conn=conn, dossier_l=dossier_l)
        doss_hier_df = pd.DataFrame.from_dict(doss_hier_l)
        return doss_hier_df

    def read_visual_hier(self, page):
        for v in page["visualizations"]:
            self.visual_d["visual_key"] = v["key"]
            self.visual_d["visual_name"] = v["name"]
            self.visual_d["visualizationType"] = v["visualizationType"]
            self.visual_list.append(self.visual_d.copy())

    def read_pages_hier(self, chapter):
        for page in chapter["pages"]:
            self.visual_d["page_key"] = page["key"]
            self.visual_d["page_name"] = page["name"]
            if "selectors" in page.keys():
                pass

            self.read_visual_hier(page=page)

    def read_out_doss_datasets(self, conn, obj_d, dossier_l):
        dash_data_set_l = []
        for dossier_id in dossier_l:
            doss_def_d = i_mstr_api.get_dossier_def(conn, dossier_id)
            for d in doss_def_d["datasets"]:
                data_set_d = obj_d.copy()
                data_set_d["id"] = obj_d["id"]
                data_set_d["type"] = obj_d["type"]
                data_set_d["dataset_id"] = d["id"]
                dash_data_set_l.append(data_set_d.copy())
        return dash_data_set_l

    def run_read_out_doss_hier(self, conn, dossier_l):
        self.visual_list = []
        for dossier_id in dossier_l:

            doss_hier = i_mstr_api.get_dossier_def(conn, dossier_id)
            # print(doss_hier)
            self.visual_d = {}
            self.visual_d["dossier_id"] = dossier_id
            # self.visual_dict["dossier_name"] = d.name
            self.visual_d["error_msg"] = ''
            try:

                for chapter in doss_hier["chapters"]:
                    # print(chapter)
                    self.visual_d["chapter_key"] = chapter["key"]
                    self.visual_d["chapter_name"] = chapter["name"]
                    self.read_pages_hier(chapter=chapter)

            except Exception as err:

                self.visual_d["chapter_key"] = ""
                self.visual_d["chapter_name"] = ""
                self.visual_d["page_key"] = ""
                self.visual_d["page_name"] = ""
                self.visual_d["visual_key"] = ""
                self.visual_d["visual_name"] = ""
                self.visual_d["visualizationType"] = ""
                self.visual_d["error_msg"] = err
        #   print(visual_dict)
        return self.visual_list

    def read_out_fil_selector(self,page_j_l, chapt_page_d):
        selector_target_d_l = []
        selector_target_obj_d_l = []
        for s in page_j_l:

            selector_d = chapt_page_d
            selector_d["sel_filt_key"] = s["key"]
            selector_d["sel_filt_name"] = s["name"]
            selector_d["summary"] = s["summary"]
            selector_d["selector_type"] = s["selectorType"]
            selector_d["display_style"] = s["displayStyle"]
            selector_d["has_all_option"] = s["hasAllOption"]
            # selector_d["multi_selection_allowed"]=s["multiSelectionAllowed"]
            if s["selectorType"] in ["attribute_element_list", "metric_qualification"]:
                selector_d["source"] = s["source"]
            elif s["selectorType"] == "object_replacement":
                selector_d["availableObjectItems"] = s["availableObjectItems"]
            for t in s["targets"]:
                selector_d["target_key"] = t["key"]
                selector_target_d_l.append(selector_d.copy())
            if len(s["targets"]) == 0:
                # print(selector_d)
                selector_target_d_l.append(selector_d.copy())

        for st in selector_target_d_l:
            sel_target_d = st
            if st["selector_type"] in ["attribute_element_list", "metric_qualification"]:
                # print(st)
                sel_target_d["target_object_id"] = st["source"]["id"]
                sel_target_d["target_object_name"] = st["source"]["name"]
                if st["source"]["type"]==12:
                    sel_target_d["target_object_type"] = "attribute"
                if st["source"]["type"]==4:
                    sel_target_d["target_object_type"] = "metric"

                sel_target_d.pop("source")
                if "availableObjectItems" in sel_target_d.keys():
                    sel_target_d.pop("availableObjectItems")
                selector_target_obj_d_l.append(sel_target_d.copy())

            elif st["selector_type"] == "object_replacement":
                availableObjectItems_l = st["availableObjectItems"]

                if "source" in sel_target_d.keys():
                    sel_target_d.pop("source")
                for obj in availableObjectItems_l:
                    sel_target_d["target_object_id"] = obj["id"][1:33]
                    sel_target_d["target_object_name"] = obj["name"]
                    if obj["id"][:1] == "U":
                        sel_target_d["target_object_type"] = "attribute"
                    elif obj["id"][:1] == "i":
                        sel_target_d["target_object_type"] = "metric"

                    if "availableObjectItems" in sel_target_d.keys():
                        sel_target_d.pop("availableObjectItems")
                    selector_target_obj_d_l.append(sel_target_d.copy())

        return selector_target_obj_d_l

    def read_doss_hier_selectors(self,chapter_d,doss_filt_select_d):
        chapt_page_select_d_l=[]
        for page_d in chapter_d["pages"]:
            doss_filt_select_d["page_key"] = page_d["key"]
            doss_filt_select_d["page_name"] = page_d["name"]

            if "selectors" in page_d.keys():
                page_select_l=self.read_out_fil_selector(page_j_l=page_d["selectors"], chapt_page_d=doss_filt_select_d)
                chapt_page_select_d_l.extend(page_select_l)
        return chapt_page_select_d_l


    def run_read_out_doss_filt_sel(self, conn, dossier_id_l):

        for dossier_id in dossier_id_l:

            doss_hier = i_mstr_api.get_dossier_def(conn, dossier_id)
            #print(doss_hier)
            try:
                doss_filt_sel_d = {}
                doss_filt_sel_d["dossier_id"] = dossier_id
                doss_filt_sel_d["dossier_name"] = doss_hier['name']
                doss_filt_sel_d["error_msg"] = ''
                filt_list = []
                for chapter_d in doss_hier["chapters"]:
                    doss_filt_sel_d["chapter_key"] = chapter_d["key"]
                    doss_filt_sel_d["chapter_name"] = chapter_d["name"]
                    doss_filt_d=doss_filt_sel_d.copy()

                    self.doss_filt_d_l.extend(
                            #self.read_doss_hier_filter(chapter_d=chapter_d, doss_filt_select_d=doss_filt_d)
                            self.read_out_fil_selector(page_j_l=chapter_d["filters"],
                                                                      chapt_page_d=doss_filt_d)
                                    )
                    doss_sel_d=doss_filt_sel_d.copy()
                    self.doss_filt_select_d_l.extend(self.read_doss_hier_selectors(chapter_d=chapter_d, doss_filt_select_d=doss_sel_d)
                                             )
            except Exception as err:
                print(err)
                #print(doss_hier)

        return {"dos_filt_d_l":self.doss_filt_d_l,
                "page_selector_d_l":self.doss_filt_select_d_l}


    def add_obj_selector_to_viz(self, conn, doss_vis_obj_df,selector_df):
        doss_vis_obj_df["on_grid_fg"] = True
        df_filt_obj_filt=selector_df[selector_df["selector_type"]=="object_replacement"]
        dos_vis_df=doss_vis_obj_df[["dossier_id", "dossier_name","error_msg", "chapter_key", "chapter_name", "page_key", "page_name","visual_key", "visual_name", "visualizationType", "project_id"]].drop_duplicates()
        joint_df=pd.merge(dos_vis_df,
        df_filt_obj_filt[["dossier_id", "chapter_key", "page_key", "target_key","target_object_id","target_object_name","target_object_type"]],
                 left_on=["dossier_id", "chapter_key", "page_key", "visual_key"],
                 right_on=["dossier_id", "chapter_key", "page_key", "target_key"],
                 how='right'
                )
        joint_df=joint_df.rename(columns={"target_object_id": "object_id", "target_object_name": "object_name","target_object_type":"type"})
        joint_df=joint_df.drop('target_key', axis=1)
        joint_df["row_col_fg"]="row"
        joint_df["row_col_nr"]=-1
        joint_df["form_id"]=""
        joint_df["form_name"]=""
        joint_df["on_grid_fg"] = False
        doss_vis_obj_df=pd.concat([doss_vis_obj_df, joint_df], ignore_index=True).drop_duplicates()
        return doss_vis_obj_df