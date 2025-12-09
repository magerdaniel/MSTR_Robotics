import pandas as pd
from mstrio.connection import Connection
from mstrio.users_and_groups import UserGroup
from mstrio.users_and_groups import User
from mstrio.api import users, usergroups, browsing
from mstrio.api import reports
from mstrio.project_objects import Report
from mstrio.project_objects.datasets import SuperCube
from mstrio.object_management import Folder
import urllib3

print("hello")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

username = ""
password = ""
# server="rs002312.fastrootserver.de"
server = ""
port = "8443"
pa_server = ""
# project /folder where new cubes are stored
cube_project_id = "81D05F2D424C210FCC04C5B8F93E20DA"
cube_folder_id = "DF2158D24B28235E2F6A989D0EC4C5B8"
mtdi_id = None
# data source for cube user_all_groups

pa_project_id = "4DFD1AE74BDB0A0E671D239F7824FA66"
pa_report_id = "65362F1241F365685D31F88FED885D70"

folder_l = ["D3C7D461F69C4610AA6BAA5EF51F4125"]
base_url = "https://" + server + ":" + port + "/MicroStrategyLibrary/api"
pa_base_url = "https://" + pa_server + ":" + port + "/MicroStrategyLibrary/api"

# pandas setting to see the whole table
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)

user_cube_id = "7F195EA24FF5FFBB260271A8C3068938"
user_mapped_groups_cube_id = "A601D8484A76E1CD19F076A62AEFFA05"
user_all_groups_cube_id = None  # "B4454EDB49D4820079CC20863288F03E"
obj_acl_user_cube_id = None
user_address_df_cube_id = None
obj_acl_user_group_cube_id = "58761E6A42E11D4E066DB8AB33F90DCE"
all_group_parents_cube_id = "F1B8C7434781B867666BC7A650FEAC6B"
lu_user_all_groups_cube_id = "E2D86A2E4D8BF8BEA4D7A4A132DC045A"
lu_user_mapped_groups_cube_id = "95C161634E6B8435A73123B1F6309B28"

user_df = None
user_sec_filter_cube_id = None
user_df = None
usergroup_df = None
user_mapped_groups_df = None
user_all_groups_df = None
all_group_parents_df = None
obj_acl_df = None
user_sec_filter_df = None

proj_folder_l = [
    {"project_id": "81D05F2D424C210FCC04C5B8F93E20DA", "folder_id": "98FE182C2A10427EACE0CD30B6768258"},
    {"project_id": "81D05F2D424C210FCC04C5B8F93E20DA", "folder_id": "95C3B713318B43D490EE789BE27D298C"}
]


class read_out_users():
    # get all user data
    def get_user_prop(self, conn, user_id):
        user_prop = User(connection=conn, id=user_id)
        user_prop_l = [user_prop.id,
                       user_prop.name,
                       user_prop.username,
                       user_prop.enabled,
                       user_prop.password_modifiable,
                       user_prop.standard_auth,
                       str(user_prop.date_created),
                       str(user_prop.date_modified)
                       ]

        return user_prop_l

    def read_out_users(self, conn, user_l):
        all_user_prop = []
        for u in user_l:
            all_user_prop.append(self.get_user_prop(conn=conn, user_id=u))
        return all_user_prop

    def get_user_df(self, conn, user_l):
        # export users to df
        # as imput a list of users GUID is needed
        user_cols = ["id", "name", "username", "enabled", "password_modifiable", "standard_auth", "date_created",
                     "date_modified"]
        all_user_l = self.read_out_users(conn=conn, user_l=user_l)
        user_df = pd.DataFrame(data=all_user_l, columns=user_cols)
        return user_df

    def get_user_addrs(self, conn, user_l):
        addr_l = []
        for user_id in user_l:
            addr_d_l = users.get_addresses(connection=conn, id=user_id).json()
            for addr in addr_d_l["addresses"]:
                addr_d = {"user_id": user_id}
                addr_d["addr_id"] = addr["id"]
                addr_d["addr_name"] = addr["name"]
                addr_d["addr_deliveryMode"] = addr["deliveryMode"]
                addr_d["addr_deviceId"] = addr["deviceId"]
                addr_d["addr_value"] = addr["value"]
                addr_d["addr_isDefault"] = addr["isDefault"]
                addr_l.append(addr_d.copy())
        address_df = pd.DataFrame.from_dict(addr_l)
        return address_df


class read_out_user_sec_filters():
    # reads out the security filter for
    # a single user for all projects
    # it takes 3 seconds per user, thus it takes a while for the 3.000 users
    # *********************** Attention**************
    #  this is one of the most critical parts of the read out
    #  and it can and should be simplyfied and tuned
    # *********************************************
    def get_proj_user_sec_filter(self, conn, user_id):
        u_sec_filter = conn.get(f'{conn.base_url}/api/users/{user_id}/securityFilters?offset=0&limit=-1')
        user_proj_secfilter_l = []
        print(user_id)
        for p in u_sec_filter.json()["projects"]:
            # this logic is checks if there is at least one sec_filter applied
            if len(p) > 2:
                for sec_f in p["securityFilters"]:
                    secfilter_l = [user_id, p["id"], p["name"], sec_f["id"], sec_f["name"]]
                    if "inherited" in sec_f:
                        if sec_f["inherited"] == True:
                            secfilter_l.append(sec_f["inheritedFrom"]["id"])
                            secfilter_l.append(sec_f["inheritedFrom"]["name"])
                        else:
                            secfilter_l.append("x0000000000000000000000000000000")
                            secfilter_l.append("x0000000000000000000000000000000")
                    else:
                        # secfilter applied at user level
                        secfilter_l.append("x0000000000000000000000000000000")
                        secfilter_l.append("x0000000000000000000000000000000")
                    user_proj_secfilter_l.append(secfilter_l)

        return user_proj_secfilter_l

    def get_user_secfilter_df(self, conn, user_l):
        # controls the read_out_loop
        # for securityfilter
        sec_filter_cols = ["user_id", "project_id", "project_name", "sec_filter_id", "sec_filter_name",
                           "inherit_grp_id", "inherit_grp_name"]
        all_user_sec_filter = []
        for u in user_l:
            all_user_sec_filter.extend(self.get_proj_user_sec_filter(conn=conn, user_id=u))
        if len(all_user_sec_filter[0]) > 0:
            all_user_sec_filter_df = pd.DataFrame(data=all_user_sec_filter, columns=sec_filter_cols)
            return all_user_sec_filter_df
        else:
            return None


class read_out_parents():
    # this clas reads out the parent groups
    # for users and user groups

    def get_user_mapped_groups(self, conn, user_id):
        # reads out the groups where the user is mapped to
        parent_groups = users.get_memberships(connection=conn, id=user_id, fields="id")
        user_parent_groups = []
        for ug in parent_groups.json():
            parent_group = []
            parent_group.append(user_id)
            parent_group.append(ug["id"])
            user_parent_groups.append(parent_group)
        return user_parent_groups

    def get_all_user_mapped_groups_df(self, conn, user_l):
        # loops a list of users to fetch mapped groups
        user_parent_groups = []
        for u in user_l:
            user_parent_groups.extend(self.get_user_mapped_groups(conn=conn, user_id=u))
        user_mapped_groups_df = pd.DataFrame(data=user_parent_groups, columns=["user_id", "mapped_group_id"])
        return user_mapped_groups_df

    def get_group_parents(self, conn, usergroup_id):
        # fetch the parents of a usergroup
        usergroup_parents = []
        try:
            parents_resp = usergroups.get_memberships(connection=conn, id=usergroup_id)
            parents_l = read_out_usergroup.get_usergroup_id(self, members_resp=parents_resp).json()
            for ug in parents_l["id"]:
                user_usergroup = []
                user_usergroup.append(usergroup_id)
                user_usergroup.append(ug)
                usergroup_parents.append(user_usergroup)
        except:
            pass
        return usergroup_parents

    def get_all_group_parents_df(self, conn, usergroup_l):
        # loops a list of usergroups to fetch there parents
        all_usergroup_parents = []
        for u in usergroup_l:
            all_usergroup_parents.extend(self.get_group_parents(conn=conn, usergroup_id=u))
        usergroup_mapped_groups_df = pd.DataFrame(data=all_usergroup_parents,
                                                  columns=["usergroup_id", "parent_group_id"])
        return usergroup_mapped_groups_df


class read_out_usergroup():

    def get_all_usergroup_df(self, conn, user_group_l):
        # read out properties of a list of usergroups
        user_group_prop_l = []
        user_group_cols = ["id", "type", "abbreviation", "subtype", "dateCreated", "dateModified", "version", "acg"]
        for ug in user_group_l:
            user_group_prop_l.append([ug["id"],
                                      ug["type"],
                                      ug["name"],
                                      ug["subtype"],
                                      ug["dateCreated"],
                                      ug["dateModified"],
                                      ug["version"],
                                      ug["acg"]])
        all_usergroup_df = pd.DataFrame(data=user_group_prop_l, columns=user_group_cols)
        return all_usergroup_df

    def get_user_group_members_l(self, conn, user_group_id):
        # returns a list of user_group GUID
        u_grp = UserGroup(connection=conn, id=user_group_id)
        user_l = []
        for u in u_grp.list_members():
            if u["enabled"] == True:
                user_l.append(u["id"])
        return user_l


class read_acl():
    count = 0
    obj_acl = []
    all_obj_acl = []

    def read_content_acl(self, folder_obj):
        all_folder_obj_acl = []
        # get all objects within a folder as dict
        for content in folder_obj.get_contents(to_dictionary=True):
            # get all object properties of a object as JSON
            prop_resp = self.fetch_obj_acl(conn, obj_id=content["id"], obj_type=content["type"])

            if prop_resp.ok:
                obj_path = self.bld_obj_path(prop_resp.json()["ancestors"])
                obj_acl = prop_resp.json()["acl"]
                # loop trough the ACL for users & userGroups
                for acl in obj_acl:
                    obj_acl = []
                    obj_acl.append(conn.project_id)
                    obj_acl.append(content["id"])
                    obj_acl.append(content["name"])
                    obj_acl.append(content["type"])
                    obj_acl.append(content["owner"]["id"])
                    obj_acl.append(content["owner"]["name"])
                    obj_acl.append(obj_path["loc_folder_id"])
                    obj_acl.append(obj_path["path_str"])
                    obj_acl.extend(list(acl.values()))
                    all_folder_obj_acl.append(obj_acl)

        return all_folder_obj_acl

    def bld_obj_path(self, obj_ancestors):
        # in REST the folder path isn't defined as string
        path_str = ""
        for o in obj_ancestors:
            path_str += o["name"] + "\\"
            if o["level"] == 1:
                fol_d = {"path_str": path_str, "loc_folder_id": o["id"]}
        return fol_d

    def fetch_obj_acl(self, conn, obj_id, obj_type):
        # read out the obj properties
        prop_resp = conn.get(f'{conn.base_url}/api/objects/{obj_id}?type={obj_type}')
        return prop_resp

    def get_all_folders(self, conn, project_id, root_folder_id):
        # executes a MSTR search and calls the results
        conn.select_project(project_id)
        obj_type = 8  # object_type
        folder_resp = browsing.store_search_instance(connection=conn, project_id=project_id, object_types=[obj_type],
                                                     root=root_folder_id)
        folder_resp = browsing.get_search_results(connection=conn, project_id=project_id,
                                                  search_id=folder_resp.json()["id"])
        folder_l = self.parse_folder_l(folder_resp=folder_resp)
        return folder_l

    def parse_folder_l(self, folder_resp):
        folder_l = []
        for f in folder_resp.json():
            folder_l.append(f["id"])
        return folder_l

    def loop_entry_folders(self, conn, proj_folder_l):
        # the read out logic is to fetch all subfolders, starting from root_folder_id
        # after that, we fetch the objects of each folder
        # input is a list of dictionaries
        # project_id /root_folder
        all_obj_acl = []
        for f in proj_folder_l:

            folder_id = f["folder_id"]
            project_id = f["project_id"]
            conn.select_project(cube_project_id)
            # get back all subfolders
            folder_l = self.get_all_folders(conn=conn, root_folder_id=folder_id, project_id=project_id)

            for f in folder_l:
                # open the folder as an object
                # pass over the open object as an argument
                acl_l = self.read_content_acl(Folder(connection=conn, id=f))
                all_obj_acl.extend(acl_l)

            cols = ["project_id", "content_id", "content_name", "content_type", "owner_id", "owner_name",
                    "loc_folder_id", "path_str", "deny", "type", "rights", "trusteeId", "trusteeName",
                    "trusteeType", "trusteeSubtype", "inheritable"]
            obj_acl_df = pd.DataFrame(data=all_obj_acl, columns=cols)
        return obj_acl_df


conn = get_conn(base_url=base_url, username=username, password=password, project_id=cube_project_id, login_mode=16,
                ssl_verify=False)
conn.headers['Content-type'] = "application/json"
conn.headers['Cookie'] = f'JSESSIONID={conn._session.cookies["JSESSIONID"]}'

pa_conn = get_conn(base_url=pa_base_url, username=username, password=password, project_id=pa_project_id, login_mode=16,
                   ssl_verify=False)
pa_conn.headers['Content-type'] = "application/json"
pa_conn.headers['Cookie'] = f'JSESSIONID={pa_conn._session.cookies["JSESSIONID"]}'
conn.select_project(cube_project_id)

r_o_usergroup = read_out_usergroup()
r_o_user = read_out_users()
r_o_parents = read_out_parents()
r_io_data = io_data()
r_read_acl = read_acl()
user_l = r_o_usergroup.get_user_group_members_l(conn=conn, user_group_id="C82C6B1011D2894CC0009D9F29718E4F")
user_group_l = conn.get(f'{conn.base_url}/api/usergroups?offset=0&limit=-1').json()

"""
# read out user
print("user_df")
user_df = r_o_user.get_user_df(conn=conn, user_l=user_l)
conn.select_project(cube_project_id)
if user_df is not None:
    r_io_data.cube_upload(conn=conn, load_df=user_df, tbl_name="user_df", updatePolicy="REPLACE",
                          folder_id=cube_folder_id,
                          cube_name="user_df", to_attribute=user_df.columns.tolist(),
                          mtdi_id=user_cube_id)

# read out usergroup
usergroup_df = r_o_usergroup.get_all_usergroup_df(conn=conn, user_group_l=user_group_l)
print("usergroup_df")
if usergroup_df is not None:
    r_io_data.cube_upload(conn=conn, load_df=usergroup_df, tbl_name="usergroup_df", updatePolicy="REPLACE",
                          folder_id=cube_folder_id, cube_name="lu_user_all_groups",
                          to_attribute=usergroup_df.columns.tolist(),
                          mtdi_id=lu_user_all_groups_cube_id)

    r_io_data.cube_upload(conn=conn, load_df=usergroup_df, tbl_name="usergroup_df", updatePolicy="REPLACE",
                          folder_id=cube_folder_id, cube_name="lu_user_mapped_groups",
                          to_attribute=usergroup_df.columns.tolist(),
                          mtdi_id=lu_user_mapped_groups_cube_id)

# read out user parents
user_mapped_groups_df = r_o_parents.get_all_user_mapped_groups_df(conn=conn, user_l=user_l)
print("user_mapped_groups_df")
if user_mapped_groups_df is not None:
    r_io_data.cube_upload(conn=conn, load_df=user_mapped_groups_df, tbl_name="user_mapped_groups_df",
                          updatePolicy="REPLACE", folder_id=cube_folder_id, cube_name="user_mapped_groups_df",
                          to_attribute=user_mapped_groups_df.columns.tolist(), mtdi_id=user_mapped_groups_cube_id)
"""
# read_out folder acl
obj_acl_df = r_read_acl.loop_entry_folders(conn=conn, proj_folder_l=proj_folder_l)
print("obj_acl_df")
if obj_acl_df is not None:
    r_io_data.cube_upload(conn=conn, load_df=obj_acl_df[obj_acl_df["trusteeSubtype"] == 8704], tbl_name="obj_acl_df",
                          updatePolicy="REPLACE", folder_id=cube_folder_id, cube_name="obj_acl_user_df_1",
                          to_attribute=obj_acl_df.columns.tolist(), mtdi_id=obj_acl_user_cube_id)

    r_io_data.cube_upload(conn=conn, load_df=obj_acl_df[obj_acl_df["trusteeSubtype"] == 8705], tbl_name="obj_acl_df",
                          updatePolicy="REPLACE", folder_id=cube_folder_id, cube_name="obj_acl_user_group_df",
                          to_attribute=obj_acl_df.columns.tolist(), mtdi_id=obj_acl_user_group_cube_id)

# read out all usergroups
instance_id = r_io_data.open_instance(conn_=pa_conn, project_id=pa_project_id, report_id=pa_report_id)
user_all_groups_df = r_io_data.report_df(conn=pa_conn, report_id=pa_report_id, instance_id=instance_id)
print("user_all_groups_df")
if user_all_groups_df is not None:
    r_io_data.cube_upload(conn=conn, load_df=user_all_groups_df, tbl_name="user_all_groups_df", updatePolicy="REPLACE",
                          folder_id=cube_folder_id, cube_name="user_all_groups_df_new",
                          to_attribute=user_all_groups_df.columns.tolist(),
                          mtdi_id=user_all_groups_cube_id)

# read out all group parents
usergroup_df = r_o_usergroup.get_all_usergroup_df(conn=conn, user_group_l=user_group_l)
all_group_parents_df = r_o_parents.get_all_group_parents_df(conn=conn, usergroup_l=list(usergroup_df["id"]))
print(all_group_parents_df)
if all_group_parents_df is not None:
    r_io_data.cube_upload(conn=conn, load_df=all_group_parents_df, tbl_name="all_group_parents_df",
                          updatePolicy="REPLACE",
                          folder_id=cube_folder_id, cube_name="all_group_parents",
                          to_attribute=all_group_parents_df.columns.tolist(),
                          mtdi_id=all_group_parents_cube_id)

# read out user adress

user_address_df = r_o_user.get_user_addrs(conn=conn, user_l=user_l)
if user_address_df is not None:
    r_io_data.cube_upload(conn=conn, load_df=user_address_df, tbl_name="user_address_df",
                          updatePolicy="REPLACE",
                          folder_id=cube_folder_id, cube_name="user_address_df",
                          to_attribute=user_address_df.columns.tolist(),
                          mtdi_id=user_address_df_cube_id)

# read out secfilters
read_user_sec = read_out_user_sec_filters()
# user_l=["6D8A5F8240106806523B50BE249957C6","38DFBC8E433AF2F30162AFB4D1B8B63C","79CE4B934B5D04EBCF6699A9E00C6BD7"]
user_sec_filter_df = read_user_sec.get_user_secfilter_df(conn=conn, user_l=user_l)
print("user_sec_filter_df")
if user_sec_filter_df is not None:
    try:
        r_io_data.cube_upload(conn=conn, load_df=user_sec_filter_df, tbl_name="user_sec_filter_df",
                              updatePolicy="REPLACE",
                              folder_id=cube_folder_id, cube_name="user_sec_filter_df_J1",
                              to_attribute=user_sec_filter_df.columns.tolist(),
                              mtdi_id=None)
    except:
        r_io_data.cube_upload(conn=conn, load_df=user_sec_filter_df, tbl_name="user_sec_filter_df",
                              updatePolicy="REPLACE",
                              folder_id=cube_folder_id, cube_name="user_sec_filter_df_Jnew",
                              to_attribute=user_sec_filter_df.columns.tolist(),
                              mtdi_id=None)

# read out data from PA
pa_rep_Account_Jobs_Per_Type_id = "F6E00A7249E1112CECBB6B8A59C843F9"
instance_id = r_io_data.open_instance(conn_=pa_conn, project_id=pa_project_id,
                                      report_id=pa_rep_Account_Jobs_Per_Type_id)
user_all_groups_df = r_io_data.report_df(conn=pa_conn, report_id=pa_rep_Account_Jobs_Per_Type_id,
                                         instance_id=instance_id)
print("user_all_groups_df")
if user_all_groups_df is not None:
    r_io_data.cube_upload(conn=conn, load_df=user_all_groups_df, tbl_name="user_all_groups_df", updatePolicy="REPLACE",
                          folder_id=cube_folder_id, cube_name="user_all_groups_df",
                          to_attribute=user_all_groups_df.columns.tolist(),
                          mtdi_id=user_all_groups_cube_id)

conn.close()
pa_conn.close()