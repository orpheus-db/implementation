from utility import Node, BiPartiteGraph

class PartitionOptimizer(object):
		def __init__(self, conn):
			self.conn = conn
			self.d = __load_index_table(self)
			self.version_tree = []
			self.partition = {} # a 2D array, each of which is a vlist of partitions
			self.tolerance = 0
			self.storage_threshold = 0
			self.cur_checkout_cost = 0

		# return the delta value of the given interval
		def __nextDelta(left, right):
			return 0.5 * (left + right)

		# find the subtree with the root node equals vid
		def __find_subTree(self, vid):
			subTree = []
			for i in versionTree:

		# the fundamental approximate algorithm
		def __approx(versionT, bipartiteGraph, delta):
			if bipartiteGraph.calc_threshold() > delta:
				return sth...

			split_vid = __pick_edge()
			__find_subTree()


		# read index table into a dictionary of format (vid: {rlist})
		def __load_index_table():
			table_name = "dataset1_indextable"
			sql = "SELECT * FROM %s ;" % table_name
			self.conn.cursor.execute(sql)
			return dict(self.conn.cursor.fetchall())

		# Read the version graph and convert it to a tree
		# Convert from the version graph to the version tree
		def __construct_version_tree():
			table_name = "dataset1_versiontable"
			sql = "SELECT vid, parent, children, commonRCnt" % table_name
			self.conn.cursor.execute(sql)
			for x in self.conn.cursor.fetchall():
				vid, parent, children, commonRCnt = x[0], x[1], x[2], x[3]
				if (len(parent) > 1):
					max_idx = commonRCnt.index(max(commonRCnt))
				else:
					max_idx = 0
				node = Node(vid, children, parent[max_idx], commonRCnt[max_idx])
				self.versionTree.append(node)

		def __is_valid_edge(self, commonRCnt):
			return commonRCnt <= self.delta * self.bipartiteGraph.getRCnt()

		# pick the edge to cut
		def __pick_edge(self):
			vid, maxCommonRCnt = 0
			for node in self.versionTree:
				commonRCnt = getCommonRCnt()
				if (__is_valid_edge(commonRCnt) and commonRCnt > maxCommonRCnt):
					vid = node.getVid()
					node.getCommonRCnt() = maxCommonRCnt
			return vid

		# check if current average checkout cost > \mu * optimal one (opt_partition())
		def exceeds_tolarated_checkout_cost():
			# return curCheckoutCost > \mu * opt_partition()
			return False

		# invoked during the commit
		# if commonCnt < prev_delta * |R| and S < gamma,
		# 	create a new version (partition?)
		# else
		# 	add it to its parent partition table
		# update_partition_table()
		# compare_with_opt()
		# migration()
		def commit_assign():
			return None
		# update the partition table with the corresponding partition mapping
		def update_partition_table():
			return None
		#migration from one partition scheme to the opt one
		def migration():
			return None

		def opt_partition():
			#initial interval of delta
			eCnt = sum([len(i) for i in self.d.values()])
			vCnt = len(self.d)
			s = set()
			for i in self.d.values():
				s |= set(i)
			rCnt = len(s)

			bipartiteGraph = BipartiteGraph(rCnt, vCnt, eCnt)

			left = float(eCnt)/(rCnt * vCnt); right = 1
			delta = __nextDelta(left, right)

			while left < right:
				__approx(versionT, bipartiteGraph, delta)

			return None
			# return optimal checkout cost

		# return the corresponding partition table of the given vid
		# TODO: fix the table_name
		def partition_lookup(self, table_name, vid):
			## schema of the partitioning table
			## pid, {vid, ...},rCnt,vCnt (optional)
			sql = "SELECT pid FROM %s WHERE %s = ANY(vlist)" % (table_name, vid)
			self.conn.cursor.execute(sql)
			res = [x[0] for x in self.conn.cursor.fetchall()]
			if len(res) != 1:
				print "=========== ERROR in partition_lookup() ==============="
				#TODO: change to Exception
			#print res
			return res

		def unitTest(self):
			table_name = "dataset1_indextable";
			sql = "SELECT * FROM %s ;" % table_name
			self.conn.cursor.execute(sql)
			d = dict(self.conn.cursor.fetchall())
			print d
			#return res
