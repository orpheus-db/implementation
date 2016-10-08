import json
import datetime

class MetadataManager(object):
    def __init__(self, conn):
    # def __init__(self,user):
        # file path is in some format of 'user'.
        # The simpliest is "~/user/"
        self.file_path = ".."
        self.connector = conn

    # Read metadata
    def load_meta(self):
        print "load meta"
        with open(self.connector.meta_info, 'r') as f:
            meta_info = f.readline()
        return json.loads(meta_info)

    # Commit metadata
    def commit_meta(self, new_meta):
        print "commit_data"
        open(self.connector.meta_info, 'w').close()
        f = open(self.connector.meta_info, 'w')
        f.write(json.dumps(new_meta))
        f.close()

    def update(self, to_table,from_table,vlist):
        print "update metadata."
        _meta = self.load_meta()
        _meta['table_map'][to_table] = from_table, vlist
        _meta['table_created_time'][to_table] = str(datetime.datetime.now())
        self.commit_meta(_meta)

    def load_modified(self):
        '''
        @summary: helper function to load modified information from disk
        @param connector: the connector obj
        @result: return dictionary of JSON format
        '''
        with open(self.connector.meta_modifiedIds, 'r') as f:
            meta_modifiedIds = f.readline()
        return json.loads(meta_modifiedIds)

    def load_modified_id(self,table_name):
        _meta = self.load_meta()
        _modified=self.load_modified()
        modified_id = []
        if table_name not in _meta['merged_tables']:
            try:
                modified_id = _modified[table_name]
            except KeyError:
                error_msg = "Table" + table_name + "does not have changes, nothing to commit "
                raise ValueError(error_msg)
                return
        return modified_id

    def load_parent_id(self,table_name):
        try:
            _meta = self.load_meta()
            parent_vid_lis = _meta['table_map'][table_name]
            # print type(parent_vid_lis)
            # print type(parent_vid_lis[1])
            # parent_vid = "\'{%s}\'" % ",".join(str(x) for x in parent_vid_lis)
            return parent_vid_lis
        except KeyError:
            parent_vid_lis = None
            error_msg = KeyError.args
            raise ValueError(error_msg)
            return

    def load_table_create_time(self,table_name):
        # load the table creat time
        try:
            _meta = self.load_meta()
            create_time = _meta['table_created_time'][table_name]
            return create_time
        except KeyError:
            print "----- ERROR ------: %s" % "created time must be completed"