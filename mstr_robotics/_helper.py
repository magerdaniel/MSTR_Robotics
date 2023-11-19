class str_func:

    def rem_braket(self, exp):
        if exp[:1] =="(":
            exp = exp.replace("(", "")
            exp = exp.replace(")", "")
        return exp

    def rem_curly(self, att):
        if att[:1] == "{":
            att = att.replace("{", "")
            att = att.replace("}", "")
        return att

    def _get_last_chars(self, str_, i=1):
        return str_[-i]

    def _get_first_x_chars(self, str_, i=1):
        return str_[0:i]

    def _rem_last_char(self, str_, i=1):
        return str_[:-i]

    def replace_val_by_prefix(self,dict, prefix, obj_val):
        for key in list(dict.keys()):
            if key.startswith(prefix):
                dict[key] = obj_val
                dict[key[len(prefix):]] =dict.pop(key)
        return dict
    # @logger
    def bld_mstr_obj_guid_sql_server(self, obj_md_id=None):
        # if you running your meta data on an MS SQL Server
        # object_id are stored in a strange way
        #this function transforns it into
        #a the orignal object string
        #from: 2276AC06-7A55-473C-9AC4-35A4E28C8021
        #to: 2276AC06473C7A55A435C49A21808CE2
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
        #transforms an object_guid into SQL Server unique string
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

class msic():

    def get_dict_with_id_in_l(self,dict_l,search_l,key="id"):
        #this function extracts dicts within in a list
        #where a (key)- column is an element
        #of the list search_l
        new_dict_l = []
        for d in dict_l:
            if d[key] in search_l:
                new_dict_l.append(d)
        return new_dict_l

    def get_key_form_dict_l(self,dict_l,key="id"):
        #if you communicating over REST with MSTR, you of often
        #get list of dictionary, where you only need the object_id's.
        #this function parses the id values and returns the as a list
        key_l=[]
        for d in dict_l:
           key_l.append(d[key])
        return key_l

    def get_comon_val_l(self,list_1,list_2):
       return list(set(list_1).intersection(list_2))

    def list_to_dict(self,list_in_l, col_l):
        #merge do lists into one dict
        #col_l are gonne be the keys
        #the elements of list_in_l are the values
        #matchin criteria is the order
        dict_l = []
        dict_d = {}
        for row in list_in_l:
            i = 0
            for col in col_l:
                dict_d[col] = row[i]
                i += 1
            dict_l.append(dict_d.copy())
        return dict_l

    def add_prefix_to_dict_keys(self,dict, dpn_prefix):
        #adds a prefix to the keys of a dict
        # use case here is to distinct i.e. between the object_id and the depn_object_id
        new_dict = {}
        if dpn_prefix==None:
            return dict
        for key, value in dict.items():
            new_key = dpn_prefix + key
            new_dict[new_key] = value
        return new_dict

"""
class sql_server_obj():
    pass


class ZZZ_df_helper():

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


    def flag_folder_df(self,flag_path_d_l, obj_df, obj_path="path", flag_path="flag_path"):
        flag_path_df = pd.DataFrame.from_dict(flag_path_d_l)
        unique_str = flag_path_df[flag_path].unique()
        matching_rows_df = obj_df[obj_df['path'].str.contains('|'.join(unique_str))]
        flaged_obj_df = pd.merge(matching_rows_df, flag_path_df, left_on=matching_rows_df['path']
                                 .str.extract(f'({"|".join(unique_str)})')[0],
                                 right_on='flag_path')
        return flaged_obj_df
"""