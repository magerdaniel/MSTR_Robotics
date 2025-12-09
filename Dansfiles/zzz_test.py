class test_loop():
    def __init__(self):
        self.prased_d=[]

    def extract_keys_values(self,json_data, parent_key=""):
        """Recursively extract all keys and their values from JSON."""
        # print("hallo Welt")
        results = []
        # print(json_data)

        if isinstance(json_data, dict):
            for k, v in json_data.items():

                full_key = f"{parent_key}.{k}" if parent_key else k

                if isinstance(v, dict):
                    pass
                    # print(str(k) )
                    #print(str(k) + ":" + str(v))
                    self.prased_d.append({k:v})
                else:
                    pass
                    # print(str(k)+":"+str(v) )
                    self.prased_d.append({k: v})
                # print(v)
                results.append((full_key, v))
                # dict_d=
                # results.append(({k:v}))
                results.extend(self.extract_keys_values(v, full_key))

        elif isinstance(json_data, list):
            for index, item in enumerate(json_data):
                # print(index)
                # print(item)
                results.extend(self.extract_keys_values(item, f"{parent_key}[{index}]"))

        return results

i_test_loop=test_loop()
# Get all keys
#data = {"T": message_check_l}
data={"key_d":{"key_d1":["a1","b1"]},"key_str":"value_str","key_int":1232,"key_l":["a","b","c"]}
keys_list = i_test_loop.extract_keys_values(json_data=data)
#print("Extracted Keys:", keys_list)
print(i_test_loop.prased_d)