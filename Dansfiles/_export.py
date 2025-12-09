import json


class file_io():
    #this class handels th io
    # with the folder sytems

    def write_JSON_to_file(self,data,file_name):
        #writes data to file,
        #used here forREGAM logs
        with open(file_name, "w") as outfile:
            outfile.write(json.dumps(data, indent=4))

    def write_list_to_JSON_to_file(self, obj_l, file_name_key="file_name"):
        for obj in obj_l:
            try:
                file_name=obj["file_name_key"]
                self.write_JSON_to_file(data=obj.pop(file_name_key) ,file_name=file_name)
            except:
                print(obj)

    def load_JSON_files(self,file_path):
        with open(file_path, 'r') as file:
            rag_obj_l = json.load(file)

        return rag_obj_l

class get_obj_JSON():

    def extract_obj_JSON(self, conn,obj_id,obj_type_id):
        pass
        """
        get_attribute(connection: Connection, id: str, changeset_id: Optional[str] = None,
        show_expression_as: Optional[List[str]] = None,
        show_potential_tables: Optional[str] = None, show_fields: Optional[str] = None,
        fields: Optional[str] = None):
        """
        return