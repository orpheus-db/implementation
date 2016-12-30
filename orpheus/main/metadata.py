import json
import datetime
import orpheus_exceptions as sys_exception

class MetadataManager(object):
    #TODO: refactor this class to static class for performance issue
    def __init__(self, config):
    # def __init__(self,user):
        # file path is in some format of 'user'.
        # The simpliest is "~/user/"
        try:
            self.file_path = ".."
            self.meta_info = config['meta_info']
            self.meta_modifiedIds = config['meta_modifiedIds']
        except KeyError as e:
            raise sys_exception.BadStateError("Context missing field %s, abort" % e.args[0])

    # Read metadata
    def load_meta(self):
        # print "load meta"
        with open(self.meta_info, 'r') as f:
            meta_info = f.readline()
        return json.loads(meta_info)

    # Commit metadata
    def commit_meta(self, new_meta):
        open(self.meta_info, 'w').close()
        f = open(self.meta_info, 'w')
        f.write(json.dumps(new_meta))
        f.close()
        print "Metadata committed"

    # can change to static method
    def update(self, to_table, to_file, dataset, vlist, old_meta):
        if to_table:
            self.update_tablemap(to_table, dataset, vlist, old_meta)
        if to_file:
            self.update_filemap(to_file, dataset, vlist, old_meta)
        # return old_meta

    def update_tablemap(self, to_table, dataset, vlist, old_meta):
        print "update metadata."
        old_meta['table_map'][to_table] = dataset, vlist
        old_meta['table_created_time'][to_table] = str(datetime.datetime.now())
        # self.commit_meta(_meta)
        return old_meta

    def update_filemap(self, to_file, dataset, vlist, old_meta):
        old_meta['file_map'][to_file] = dataset, vlist
        # keep track of time?
        return old_meta

    def load_modified(self):
        with open(self.meta_modifiedIds, 'r') as f:
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


    def load_parent_id(self,table_name, mapping='table_map'):
        parent_vid_lis = None
        try:
            _meta = self.load_meta()
            parent_vid_lis = _meta[mapping][table_name]
            # print type(parent_vid_lis)
            # print type(parent_vid_lis[1])
            # parent_vid = "\'{%s}\'" % ",".join(str(x) for x in parent_vid_lis)
            return parent_vid_lis
        except KeyError:
            raise sys_exception.BadStateError("Metadata information missing field %s, abort" % e.args[0])
            return None

    def load_table_create_time(self,table_name):
        # load the table creat time
        try:
            _meta = self.load_meta()
            create_time = _meta['table_created_time'][table_name]
            return create_time
        except KeyError:
            # print "----- ERROR ------: %s" % "created time must be completed"
            return None