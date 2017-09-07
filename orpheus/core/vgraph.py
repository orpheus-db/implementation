import orpheus_exceptions as sys_exception
import os
import json

#the version graph at the frontend
class VersionGraph(object):
    def __init__(self, config, request):
        try:
            self.request = request
            self.vGraph_json = config['vGraph_json']
            if not self.vGraph_json.endswith("/"):
                self.vGraph_json += "/"
        except KeyError as e:
            raise sys_exception.BadStateError("Context missing field %s, abort" % e.args[0])

    def __gen_json_object(self, vid, hasChildren):
        data = {}
        data['name'] = int(vid)
        if hasChildren:
            data['children'] = []
        return data

    def init_vGraph_json(self, dataset, vid):
        fpath = self.vGraph_json + dataset
        data = self.__gen_json_object(vid,True)
        print "data: %s \n path:%s \n" % (data, fpath)
        f = open(fpath, 'w')
        f.write(json.dumps(data))
        f.close()

    def delete_vGraph_json(self, dataset):
        fpath = self.vGraph_json + dataset
        try:
            os.remove(fpath)
        except OSError:
            pass

    # Find the JSON object in data format whose vid = pvid
    def __insert_into_parent_node(self, data, pvid, new_node):
        visited, stack = set(), [(data['name'], data['children'])]
        while stack:
            node = stack.pop()
            vid = int(node[0])

            if vid not in visited:
                visited.add(vid)
            if int(vid) == int(pvid):
                node[1].append(new_node)
                return True
            for child in node[1]:
                if child['name'] not in visited:
                    stack.append((child['name'], child['children']))
        return False

    def update_vGraph_json(self, dataset, vid, parents):
        # Load corresponding JSON file
        try:
            fpath = self.vGraph_json + dataset
            data = json.loads(open(fpath).read())

            new_node = self.__gen_json_object(vid, True)

            if len(parents) > 1:
                new_node['parent_'] = []
                for vid in parents[1:]:
                    new_node['parent_'].append(self.__gen_json_object(vid, False))
            primary_parent_vid = int(parents[0])

            success = self.__insert_into_parent_node(data, primary_parent_vid, new_node)
            if not success:
                raise KeyError
            f = open(fpath, 'w')
            f.write(json.dumps(data))
            f.close()
        except KeyError:
            from django.contrib import messages
            messages.error(self.request,"Could not find its parent JSON object")
            raise KeyError
            return