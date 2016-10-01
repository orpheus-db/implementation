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

