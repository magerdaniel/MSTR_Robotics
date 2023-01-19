import datetime
import urllib3

def logger(err_name):
    def mstr_classes(func):
        def try_func(*args, **kwargs):

            try:
                result = func(*args, **kwargs)
                #log_step = {"step": func.__name__, "result": result, "exe_time": datetime.datetime}
                #print(log_step)
                return result
            except Exception as err:
                desc = ""
                if func.__name__ == 'get_conn':
                    kwargs["conn_det"].pop("password", None) #remove password
                if func.__name__ == 'cube_upload':
                    kwargs.pop("load_df",None) #remove password
                desc = f'{err_name}. Pls check: {kwargs} '
                log_step = {"step": func.__name__, "desc": desc, "err": err, "exe_time": datetime.datetime}
                print(log_step)
                raise SystemExit()
        return try_func
    return mstr_classes