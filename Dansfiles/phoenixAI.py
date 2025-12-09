import pandas as pd
from mstrio.connection import Connection
from mstrio.project_objects.datasets import super_cube
from datetime import datetime, timedelta
import numpy as np
import xgboost as xgb
import warnings
import sys

warnings.filterwarnings("ignore")

class prepare_data():
    def add_week_id(self,df,dateTime_col):
        df['week_id'] = df["LastDayOfWeek"].dt.isocalendar().week.values.astype(int)
        df['week_id'] = df['week_id'].apply(lambda x: ('00' + str(x))[-2:])
        df['week_id'] = df['year_id'].astype(str) + df['week_id']
        df['week_id'] = df['week_id'].astype(int)
        return df

    def add_date(self,df,dateTime_col):
        df["date"]=df[dateTime_col].dt.date
        return df

    def add_year_id(self,df,dateTime_col):
        df["year_id"]=df[dateTime_col].dt.year
        return df

    def add_time_cols(self,df,dateTime_col):
        df=self.add_date(df,dateTime_col)
        df=self.add_year_id(df,dateTime_col)
        df=self.add_week_id(df,dateTime_col)
        return df

    def lag_col(self,data_df,target_col):
        data_df['lag_1'] = data_df[target_col].shift(1)
        data_df['lag_2'] = data_df[target_col].shift(2)
        data_df['lag_3'] = data_df[target_col].shift(3)
        data_df['rollmean_2'] = data_df[target_col].rolling(2).mean().shift(1)
        data_df['rollmean_3'] = data_df[target_col].rolling(3).mean().shift(1)
        data_df['rollsd_2'] = data_df[target_col].rolling(2).std().shift(1)
        data_df['rollsd_3'] = data_df[target_col].rolling(3).std().shift(1)
        return data_df

    def comp_lags(self,data_df,lag_cols):

        for i in range(len(lag_cols)):
            span = i + 1
            cols = lag_cols[:span]
            data_df[f'lag_sd_to_{cols[i]}'] = data_df[cols].std(axis=1)
            data_df[f'lag_max_1to{cols[i]}'] = data_df[cols].max(axis=1)
            data_df[f'lag_diff_1to{cols[i]}'] = data_df[lag_cols[0]] - data_df[lag_cols[span - 1]]
            data_df[f'lag_div_1to{cols[i]}'] = data_df[lag_cols[0]] / data_df[lag_cols[span - 1]]
        return data_df

    def add_all_lag_cols(self,data_df,lag_cols,target_col,feat_cols):
        data_df=self.lag_col(data_df,target_col)
        data_df=self.comp_lags(data_df,lag_cols=feat_cols)
        return data_df

class pred_calls:
    i_prepare_data=prepare_data()
    i_xgb = xgb
    i_train_d=[]

    def predict_row(self,xgb_model,data_df,target_col,lag_cols
                    ,lag_rows,time_col,pred_time_col_id,feat_cols, *args,**kwargs):
        new_row_df=data_df.tail(lag_rows)
        new_row_df=self.i_prepare_data.add_all_lag_cols(new_row_df,lag_cols,target_col,feat_cols)
        #print(pred_week_id)
        new_row_df = new_row_df[new_row_df[time_col] == pred_time_col_id]

        forecast = xgb_model.predict(new_row_df[feat_cols])
        new_row_df[target_col] = np.round(forecast, 2)
        if "comp_col" in kwargs:
            new_row_df[kwargs["comp_col"]] = np.round(forecast, 2)
        data_df.loc[data_df[time_col] == pred_time_col_id] = new_row_df
        return data_df

    def check_model(self,xgb_model,data_df,target_col,comp_col
                    ,lag_rows,feat_cols,time_col,pred_time_col_id):
        check_row_df = data_df[data_df[time_col] == pred_time_col_id]
        forecast = xgb_model.predict(check_row_df[feat_cols])
        check_row_df[comp_col] = np.round(forecast, 2)
        data_df.loc[data_df[time_col] == pred_time_col_id] = check_row_df
        return data_df

    def set_train(self,data_df,forecast_start,feat_cols,target_col,time_col):
        train_d={}
        #print(data_df.columns)
        train_d["X_train"]  = data_df[data_df[time_col] < forecast_start][feat_cols]
        train_d["X_val"]    = data_df[data_df[time_col] >= forecast_start][feat_cols]
        train_d["y_train"]  = data_df[data_df[time_col] < forecast_start][target_col]
        train_d["y_val"]    = data_df[data_df[time_col] >= forecast_start][target_col]
        self.i_train_d=train_d
        return train_d

    def set_XGBRegressor(self,params={ "max_depth" : 6
                                        , "eta" : 0.1
                                        , "colsample_bytree":0.7
                                        , "gamma":0.1
                                        , "n_estimators":100
                                        , "early_stopping_rounds":100
                                        , "objective":'reg:linear'
                                        , "booster":'gbtree'
                                        }):

        # i_xgb.XGBClassifier(n_estimators=200)
        """
        xgb_model = self.i_xgb.XGBRegressor(objective='reg:linear', booster='gbtree', eta=eta, max_depth=max_depth,
                                       colsample_bytree=colsample_bytree
                                       , gamma=gamma
                                       , n_estimators=n_estimators
                                       , early_stopping_rounds=early_stopping_rounds
                                       , eval_metric=['rmse', 'mae'])
        """
        xgb_model = self.i_xgb.XGBRegressor(**params)
        return xgb_model

    def xgb_fit(self,xgb_model, train_d):
        return xgb_model.fit(train_d["X_train"], train_d["y_train"], eval_set=[(train_d["X_val"], train_d["y_val"])], verbose=1)

    def loop_pred(self,xgb_model,data_df,predict_df,target_col,lag_cols,cnt_week
                  ,lag_rows,time_col,feat_cols,*args,**kwargs):
        # predict the future
        data_df = pd.concat([data_df, predict_df], ignore_index=True)
        data_df = data_df.sort_values(time_col).reset_index(drop=True)
        for row, time_col_id in predict_df.iterrows():
            # print(week_id["week_id"])
            data_df = self.predict_row(xgb_model=xgb_model
                                  , data_df=data_df
                                  , target_col=target_col
                                  , lag_cols=lag_cols
                                  , time_col=time_col
                                  , lag_rows=lag_rows + cnt_week
                                  , pred_time_col_id=time_col_id[time_col]
                                  , feat_cols=feat_cols
                                  , *args,**kwargs)
        return data_df

    def check_pred(self,xgb_model
                    , data_df
                    ,target_col
                    ,feat_cols
                    , lag_rows
                    , cnt_week
                    ,time_col="week_id"
                    ,comp_col = "check_pred" ):
        # test_the past
        data_df[comp_col] = ""
        time_id_l = data_df[time_col].tolist()[lag_rows:]
        for week_id in time_id_l:
            # print(week_id["week_id"])
            data_df = self.check_model(xgb_model
                                  , data_df=data_df
                                  , target_col=target_col
                                  , feat_cols=feat_cols
                                  , lag_rows=lag_rows + cnt_week
                                  , comp_col=comp_col
                                  , time_col=time_col
                                  , pred_time_col_id=week_id)
        return data_df

class mstr():

    def get_conn(base_url, project_id=None, *args, **kwargs):
        conn = Connection(base_url=base_url, project_id=project_id, *args, **kwargs)
        conn.headers['Content-type'] = "application/json"
        return conn

    def re_build_df(self,data_df):
        values_list = data_df.values.tolist()
        columns_list = data_df.columns.tolist()
        data_df = pd.DataFrame(values_list, columns=columns_list)
        return data_df

    def cube_upload_1_table(self, conn, load_df, tbl_name,to_attribute, updatePolicy="REPLACE",
                            folder_id=None, cube_name=None, mtdi_id=None, force=False):
        if mtdi_id == None or mtdi_id =="":
            ds = super_cube.SuperCube(connection=conn, name=cube_name)
            ds.add_table(name=tbl_name, data_frame=load_df, update_policy=updatePolicy,to_attribute=to_attribute)
            ds.create(folder_id=folder_id,force=force,)
        else:
            ds = super_cube.SuperCube(connection=conn, id=mtdi_id)
            ds.add_table(name=tbl_name, data_frame=load_df, update_policy=updatePolicy)
            ds.update()
        return ds.id

