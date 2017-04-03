from utility import Node, BiPartiteGraph

class PartitionOptimizer(object):
		def __init__(self, conn): #TODO
			self.conn = conn
			self.versio_tree = dict{}
			self.root = None
			self.bipartiteGraph = None
			self.tolerance = 0
			self.storage_threshold = 0
			self.cur_checkout_cost = 0

		# initialize the version tree and the corresponding BipartiteGraph
		def _initialize(self):
			versionTable = "dataset1_versiontable" #TODO
			graph_rcnt, graph_ecnt, graph_vlist = 0, 0, set()
			root, node = None, None
			version_tree = dict()

			sql = "SELECT vid, rCnt, parents, children, commonRCnt FROM  %s" % versionTable
			self.conn.cursor.execute(sql)
			for x in self.conn.cursor.fetchall():
				vid, rCnt, parents, children, commonRCnt = x[0], x[1], x[2], x[3], x[4]

				max_index = 0
				if (len(parents) > 1):
					max_idx = commonRCnt.index(max(commonRCnt))

				graph_vlist.append(vid)
				graph_rcnt += rCnt - commonRCnt[max_idx]
				graph_ecnt += rCnt

				node = Node(vid, children, parent[max_idx], commonRCnt[max_idx])
				self.version_tree[vid] = node

				if len(parents) == 1 and parents[0] == -1:
					self.root = node

			self.bipartiteGraph = BipartiteGraph(graph_vlist, graph_rcnt, graph_ecnt)

		def _is_valid_edge(self, commonRCnt, rCnt, delta):
			return commonRCnt <= self.delta * rCnt

		# traverse through the tree starting at the Root
		# and pick the edge with the smallest number of commonRCnt(aka weight)
		def _pick_edge(self, root, rCnt, delta):
			minNode, minCommonRCnt = None, 0
			visited, stack = set(), [root]
			while stack:
				node = stack.pop()
				if node not in visited:
					visited.add(node)	
					for vid in node.getChildren():
						stack.append(self.version_tree[vid])
					if node.getParent() == None:
						continue
					commonRnt = node.getCommonRnt()
					if (_is_valid_edge(commonRCnt, rCnt, delta) and commonRCnt < minCommonRCnt):
						minNode = node
						minCommonRCnt = commonRnt
			return minNode

		# return the delta value of the given interval
		def _nextDelta(self, left, right):
			return 0.5 * (left + right)

		# construct the subTree and bipartiteGraph at the split_node
		def _splitGraph(self, split_node):
			visited, stack = set(), [split_node]
			vlist, rCnt, eCnt, commonRCnt = set(), 0, 0, 0

			while stack:
				node = stack.pop()
				if node not in visited:
					visited.add(node)
					for vid in node.getChildren():
						stack.append(version_tree[vid])
					# ======== Update bipartiteGraph statistics
					cur_vid = node.getVid()
					vlist.append(cur_vid)
					eCnt += node.getRCnt()
					rCnt += node.getRCnt() - node.getCommonRnt()
					commonRCnt += node.getCommonRnt()
			
			version_tree[split_node.getParent()].setChildren(None)

			newBipartiteGraph = BipartiteGraph(vlist, rCnt, eCnt)
			partitions.add(newBipartiteGraph)
			# Find the old partition it is in, and 
			# update the old partition statistics
			parentBipartiteGraph = None
			for bipartiteGraph in partitions:
				if bipartiteGraph.getVList() & vlist:
					bipartiteGraph.update(bipartiteGraph.getVList() - vlist,
										  biPartiteGraph.getVCnt() - vCnt, 
										  biPartiteGraph.getRCnt() - rCnt + split_node.getCommonRnt(),
										  biPartiteGraph.getECnt() - eCnt)
					parentBipartiteGraph = biPartiteGraph

			return newBipartiteGraph, parentBipartiteGraph
			# TODO: Err if there is more than one such bipartite Graph 

		# read index table into a dictionary of format (vid: {rlist})
		# def _load_index_table():
		# 	table_name = "dataset1_indextable"
		# 	sql = "SELECT * FROM %s ;" % table_name
		# 	self.conn.cursor.execute(sql)
		# 	return dict(self.conn.cursor.fetchall())

		# the fundamental approximate algorithm
		def _approx(self, root, bipartiteGraph, delta, partitions):
			if bipartiteGraph.calc_threshold() > delta:
				return
			split_node = _pick_edge(root, bipartiteGraph.getRCnt(), delta)
			newBipartiteGraph, parentBipartiteGraph = _splitGraph(self, split_node)
			_approx(split_node, newBipartiteGraph, delta)
			_approx(root, parentBipartiteGraph, delta)
			return 


		# def _calc_storage_threshold(partitions):
		# 	storage = 0
		# 	for bipartiteGraph in partitions:
		# 		storage += partitions.getRCnt()
		# 	return storage

		# # check if current average checkout cost > \mu * optimal one (opt_partition())
		# def exceeds_tolarated_checkout_cost():
		# 	# return curCheckoutCost > \mu * opt_partition()
		# 	return False

		# # invoked during the commit
		# # if commonCnt < prev_delta * |R| and S < gamma,
		# # 	create a new version (partition?)
		# # else
		# # 	add it to its parent partition table
		# # update_partition_table()
		# # compare_with_opt()
		# # migration()
		# def commit_assign():
		# 	return None
		# # update the partition table with the corresponding partition mapping
		# def update_partition_table():
		# 	return None
		# #migration from one partition scheme to the opt one
		# def migration():
		# 	return None

		def opt_partition():
			_initialize()

			left = self.bipartiteGraph.calc_threshold()
			right = 1
			
			#binary search for best \delta
			while left < right:
				partitions = {} # each element is a bipartiteGraph
				delta = __nextDelta(left, right)
				__approx(self.root, self.bipartiteGraph, delta, partitions)
				cur_storage = __calc_storage_threshold(partitions)
				if cur_storage < self.storage_threshold:
					left = delta
				else:
					right = delta

		# 	# return optimal checkout cost

		# # return the corresponding partition table of the given vid
		# # TODO: fix the table_name
		# def partition_lookup(self, table_name, vid):
		# 	## schema of the partitioning table
		# 	## pid, {vid, ...},rCnt,vCnt (optional)
		# 	sql = "SELECT pid FROM %s WHERE %s = ANY(vlist)" % (table_name, vid)
		# 	self.conn.cursor.execute(sql)
		# 	res = [x[0] for x in self.conn.cursor.fetchall()]
		# 	if len(res) != 1:
		# 		print "=========== ERROR in partition_lookup() ==============="
		# 		#TODO: change to Exception
		# 	#print res
		# 	return res

		# def unitTest(self):
		# 	table_name = "dataset1_indextable";
		# 	sql = "SELECT * FROM %s ;" % table_name
		# 	self.conn.cursor.execute(sql)
		# 	d = dict(self.conn.cursor.fetchall())
		# 	print d
		# 	#return res
