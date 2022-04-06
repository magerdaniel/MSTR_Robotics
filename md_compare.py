import pandas as pd

from mstrio.project_objects.datasets.cube import _Cube
from mstrio.connection import Connection
from mstrio.project_objects.datasets import SuperCube


def _tbl_only(s_only_df=None, both_df=None, chk_lvl_1=None):
    common = s_only_df.merge(both_df, on=[chk_lvl_1])
    s_only_df = s_only_df[~s_only_df.physical_table_name.isin(common.physical_table_name)]
    return s_only_df

def chk_prj_tbl(left_p,right_p,df,chk_lvl_1):
    #build df for common and non common tables
    tbl_df = df[[chk_lvl_1,"_merge"]].drop_duplicates()
    tbl_both_df=tbl_df.query('_merge=="both"').drop_duplicates()
    tbl_lf_only_df=tbl_df.query('_merge=="left_only"').drop_duplicates()
    tbl_rg_only_df=tbl_df.query('_merge=="right_only"').drop_duplicates()

    #first step is to check for tables which are only in one project
    #find tables which are only in LEFT / right project
    #add all information missing tables
    only_tbl=_tbl_only(s_only_df=tbl_lf_only_df,both_df=tbl_both_df,chk_lvl_1=chk_lvl_1)
    only_tbl=only_tbl.append(_tbl_only(s_only_df=tbl_rg_only_df,both_df=tbl_both_df,chk_lvl_1=chk_lvl_1))
    only_tbl_df=df.merge(only_tbl, how='inner', on=chk_lvl_1)
    only_tbl_df=_coalesce_merge(common_tbl_col= only_tbl_df)
    only_tbl_df=only_tbl_df.astype({"_merge":"str"})
    only_tbl_df.loc[only_tbl_df['_merge'] == "right_only", '_merge'] = right_p
    only_tbl_df.loc[only_tbl_df['_merge'] == "left_only", '_merge'] = left_p
    return only_tbl_df

def build_diff_files(left_p,right_p,df=None,chk_lvl_1=None):

    only_tbl_df=chk_prj_tbl(left_p=left_p,right_p=right_p,df=df,chk_lvl_1=chk_lvl_1)
    #column check only for tables existing in both projects
    common_tbl_col=df[~df.physical_table_name.isin(only_tbl_df.physical_table_name)]
    #common_tbl_col.to_excel("common_tbl_col.xlsx")
    common_col=common_tbl_col.query('_merge !="both" ')
    common_tbl_diff_pd=_coalesce_merge(common_tbl_col=common_tbl_col)
    common_col=common_tbl_diff_pd.astype({"_merge":"str"})

    common_col.loc[common_col['_merge'] == "right_only", '_merge'] = right_p
    common_col.loc[common_col['_merge'] == "left_only", '_merge'] = left_p
    diff_update_l=[]
    diff_update_l.append({"col_diff":common_col})
    diff_update_l.append({"tbl_diff":only_tbl_df})
    return diff_update_l

def _coalesce_merge(common_tbl_col=None):
    select_str=""
    for col in common_tbl_col.columns:
        if "_x" in col:
            common_tbl_col[''+col[:-2]+'']=common_tbl_col[''+ col[:-2] +'_x'].mask(pd.isnull, common_tbl_col[''+ col[:-2] +'_y'])
            common_tbl_col=common_tbl_col.drop(columns=[col[:-2] +'_x', col[:-2] +'_y'])
        elif "_y" in col:
          pass

    return common_tbl_col

def get_cube_data(conn=None,project_id=None,mtdi_id=None):
    conn.headers["X-MSTR-ProjectID"] = project_id
    cbe=_Cube(connection=conn,id=mtdi_id)
    df_cbe=cbe.to_dataframe(multi_df=True)
    df_left= df_cbe[0].query('env =="prod"') #L_EMP_EDU
    df_right=df_cbe[0].query('env =="dev"') #L_CONTRACTOR
    left_p="prod only"
    right_p="dev only"
    chk_lvl_1="physical_table_name"
    join_col=["base_url",
              #"project_guid",
              "expression",
              "object_guid",
              "physical_table_name"
              ]
    print(cbe)

    df = df_left.merge(df_right, how='outer', on=join_col, indicator=True)
    diff_files=build_diff_files(df=df,chk_lvl_1=chk_lvl_1,left_p=left_p,right_p=right_p)
    return diff_files

def upload_data(conn,project_id,mtdi_id=None,load_df=None, tbl_name=None, update_policy=None,tbl_upd_dict=None,
                cube_name=None, folder_id=None):

    if mtdi_id ==None:
        ds = SuperCube(connection=conn, name=cube_name)
        for t in tbl_upd_dict:
            ds.add_table(name=list(t.keys())[0],
                         data_frame=t[list(t.keys())[0]],
                         update_policy="Replace")
        ds.create(folder_id=folder_id)
    else:
        ds = SuperCube(connection=conn, id=mtdi_id)
        for t in tbl_upd_dict:
            ds.add_table(name=list(t.keys())[0],
                         data_frame=t[list(t.keys())[0]],
                         update_policy=update_policy)
        ds.update()

    mtdi_id=ds.id
    return ds.id

