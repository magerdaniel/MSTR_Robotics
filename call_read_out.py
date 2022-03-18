import read_out_md
import pandas
import openpyxl
conf=md_analytics_conf
read=read_out_md
pd=pandas

base_url="https://mstr.dwapplications.com:8443/MicroStrategyLibrary/api/"
#MicroStrategy Tutorial
ldprj=['5B30C5AB11E9C111DD5D0080EFA5420F']
username='hacker_admin'
password='h4cker@!'
#information to create new cube
fld_id='75028E138C4733B6C4DFE9964AE0F125'
cbe_name='MD_ANALYTICS_CUBE1'
updatePolicy="Add"
#existing cube to simply add data
mtdi_id="77F5EE183A4D298CB66F5587E3F9F9D1"
#open a new connection (mstrio)
conn=read.get_conn(base_url,username=username,password=password,project_id=ldprj)
#call read out function
read.runreadout(conn=conn,chkprj_l=ldprj,fld_id=fld_id,updatePolicy=updatePolicy,cube_name=cube_name,mtdi_id=mtdi_id)
