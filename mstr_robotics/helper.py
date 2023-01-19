class str_func:
    def _get_last_chars(self, str_, i=1):
        return str_[-i]

    def _get_first_x_chars(self, str_, i=1):
        return str_[0:i]

    def _rem_last_char(self, str_, i=1):
        return str_[:-i]


    # @logger
    def _bld_mstr_obj_guid(self, obj_md_id=None):
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
