import json

class MetadataManager(object):
    def __init__(self, conn,user):
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

    def update(self, data):
        print "update metadata."
        self.commit_meta(data)

