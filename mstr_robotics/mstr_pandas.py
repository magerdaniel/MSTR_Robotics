import pandas as pd

class df_helper():

    def clean_double_col(self, df, postfix_left="_x", postfix_right="_y"):
        # postfix_left gets removed
        # postfix_right gets deleted
        doubles_y = [col for col in df.columns if col.endswith(postfix_right)]
        df.drop(columns=doubles_y, inplace=True)
        df.columns = [col.replace(postfix_left, '') for col in df.columns]
        return df

    def rem_att_id_forms(self,df,postfix="@ID"):
        df.columns = [col.replace(postfix, '') for col in df.columns]
        return df

    def flag_folder_df(self,flag_path_d_l, obj_df, path="path_ids", merge_key_l=['project_id', 'id'],
                       flag_folder_id="flag_folder_id"):
        # most user objetcs in mstr projects are organized in folders
        # the standard function, which reads out the object definition
        # provides a strin with the folder names AND
        # a string with the obj_guid1_obj_guid2_obj_guidx obj_guid1_obj_guid2_obj_guidx
        # the code below check, if a guid of a folder is in the obj_guid1_obj_guid2_obj_guidx
        # if so it adds a value in the variable column flag_path
        #
        flaged_obj_all_l = []
        flag_path_df = pd.DataFrame.from_dict(flag_path_d_l)
        for folder_id in flag_path_df["flag_folder_id"]:
            flag_path_df = pd.DataFrame.from_dict(flag_path_d_l)
            unique_str = flag_path_df[flag_folder_id].unique()
            matching_rows_df = obj_df[obj_df[path].str.contains('|'.join(unique_str))]
            flaged_obj_df = pd.merge(matching_rows_df, flag_path_df, left_on=matching_rows_df[path]
                                     .str.extract(f'({"|".join(unique_str)})')[0],
                                     right_on=flag_folder_id, how="outer")

            flaged_obj_all_l.append(pd.merge(obj_df, flaged_obj_df, on=merge_key_l, how='left'))
        flaged_obj_all_df = pd.concat(flaged_obj_all_l, ignore_index=True)
        flaged_obj_all_df = self.clean_double_col(df=flaged_obj_all_df)

        return flaged_obj_all_df

    def add_prefix_col_to_df(self,df, prefix_col_d):
        i = 0
        for k in prefix_col_d:
            if k not in list(df.keys()):
                df.insert(i, k, prefix_col_d[k])
                i += 1
        return df