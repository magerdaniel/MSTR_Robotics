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

    """
    @staticmethod
    def read_vis_metrics(obj, vis_d, row_col_fg, row_col_nr):
        metrics = []
        for met in obj["elements"]:
            met_d = vis_d.copy()
            met_d["metric_id"] = met["id"]
            met_d["metric_name"] = met["name"]
            met_d["type"] = met["type"]
            met_d["row_col_fg"] = row_col_fg
            met_d["row_col_nr"] = row_col_nr
            metrics.append(met_d)
        return metrics

    @staticmethod
    def read_vis_attribute(obj, vis_d, row_col_fg, row_col_nr):
        att_d = vis_d.copy()
        att_d["attribute_id"] = obj["id"]
        att_d["attribute_name"] = obj["name"]
        att_d["type"] = obj["type"]
        att_d["row_col_fg"] = row_col_fg
        att_d["row_col_nr"] = row_col_nr
        for form in obj["forms"]:
            att_d["form_id"] = form["id"]
            att_d["form_name"] = form["name"]
        return att_d

       #return {"vis_att_l": vis_att_l, "vis_met_l": vis_met_l}

    def read_out_grid(self,vis_d, grid_definition):
        if "rows" in grid_definition:
            for row in grid_definition["rows"]:
                if row["type"] == "templateMetrics":
                    self.vis_met_l.extend(self.read_vis_metrics(obj=row, vis_d=vis_d, row_col_fg="row", row_col_nr=1))
                elif row["type"] == "attribute":
                    self.vis_att_l.append(self.read_vis_attribute(obj=row, vis_d=vis_d, row_col_fg="row", row_col_nr=1))

        if "columns" in grid_definition:
            for col in grid_definition["columns"]:
                if col["type"] == "templateMetrics":
                    self.vis_met_l.extend(self.read_vis_metrics(obj=col, vis_d=vis_d, row_col_fg="col", row_col_nr=1))
                elif col["type"] == "attribute":
                    self.vis_att_l.append(self.read_vis_attribute(obj=col, vis_d=vis_d, row_col_fg="col", row_col_nr=1))
    """

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
            instance_id=i_mstr_api.create_dossier_instance(conn,dossier_id)
            doss_hier = i_mstr_api.get_dossier_def(conn, dossier_id)
            # print(doss_hier)
            self.visual_d = {}
            self.visual_d["dossier_id"] = dossier_id
            self.visual_d["dossier_name"] = doss_hier["name"]
            self.visual_d["error_msg"] = ''
            try:

                for chapter in doss_hier["chapters"]:
                    # print(chapter)
                    self.visual_d["chapter_key"] = chapter["key"]
                    self.visual_d["chapter_name"] = chapter["name"]
                    self.read_pages_hier_det(conn=conn,chapter=chapter, instance_id=instance_id)

            except Exception as err:
                print(err)
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

