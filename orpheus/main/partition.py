from utility import Node, BipartiteGraph
from orpheus_const import Constants as const
import sys
import copy

class Partitioner(object):
	def __init__(self, conn, dataset=None, storage_threshold=-1, tolerance=-1): #TODO
		self.conn = conn
		self.tolerance = tolerance # TODO A User defined parameter
		self.storage_threshold = storage_threshold # TODO A User defined parameter
		self.dataset = dataset
		self._cur_partitions = None
		self._opt_partitions = None
		self._prev_delta = None
		self._version_tree = None

	# Retrieve the total rCnt among all partitions in the table
	def __calc_cur_storageCost(self):
		partition_table = const.getPartitiontableName(self.dataset)
		sql = "SELECT SUM(rCnt) FROM %s;" % partition_table

		self.conn.cursor.execute(sql)
		cur_storage = self.conn.cursor.fetchall()[0][0]
		return cur_storage

	def __calc_cur_checkoutCost(self):
		partition_table = const.getPartitiontableName(self.dataset)

		sql = "SELECT SUM(cardinality(vlist) * rCnt) FROM %s;" % partition_table

		self.conn.cursor.execute(sql)
		cur_checkoutCost = self.conn.cursor.fetchall()[0][0]
		return cur_checkoutCost

	# Construct a Node given the vid
	def __construct_node(self, vid):
		version_table = const.getVersiontableName(self.dataset)
		sql = "SELECT vid, num_records, parents, children, commonRCnt FROM  %s WHERE vid = %s" % (version_table, vid)

		self.conn.cursor.execute(sql)
		for x in self.conn.cursor.fetchall():
			vid, rCnt, parents, children, commonRCnt = x[0], x[1], x[2], x[3], x[4]

		max_idx = 0
		if (len(parents) > 1):
			max_idx = commonRCnt.index(max(commonRCnt))

		return Node(vid, rCnt, children, parents[max_idx], commonRCnt[max_idx])

	# Return the partition instance given the vid
	def __construct_partition(self, vid):
		partition_table = const.getPartitiontableName(self.dataset)
		sql = "SELECT pid, vlist, rCnt, eCnt FROM  %s WHERE vlist @> ARRAY[%s]" % (partition_table, vid)
		self.conn.cursor.execute(sql)
		for x in self.conn.cursor.fetchall():
			pid, vlist, rCnt, eCnt = x[0], x[1], x[2], x[3]

		return BipartiteGraph(pid, vlist, rCnt, eCnt)

	# insert a new partition entry
	def __insert_partition(self, node):
		partition_table = const.getPartitiontableName(self.dataset)
		sql = "INSERT INTO %s (vlist, rCnt, eCnt) VALUES ('{%s}', %s, %s) RETURNING pid;" % (partition_table, node.getVID(), node.getRCnt(), node.getRCnt())
		self.conn.cursor.execute(sql)
		cur_pid = self.conn.cursor.fetchall()[0][0]
		self.conn.connect.commit()
		return cur_pid


	def __update_partition_table(self, parent_pID, node):
		partition_table = const.getPartitiontableName(self.dataset)

		sql = "UPDATE %s SET vlist=array_append(vlist, %s), rCnt=rCnt+%s, eCnt=eCnt+%s WHERE pid=%s;" \
						% (partition_table, node.getVID(), node.getRCnt() - node.getCommonRCnt(), node.getRCnt(), parent_pID);
		self.conn.cursor.execute(sql)
		self.conn.connect.commit()


	# Estimate the commonRCnt between two version list
	def __est_commontRCnt(self, vlist1, vlist2):
		commonVlist = sorted(vlist1 & vlist2)
		commonRCnt = self._version_tree[commonVlist.pop()].getRCnt()

		for vid in commonVlist:
			node = self._version_tree[vid]
			commonRCnt += node.getRCnt() - node.getCommontRCnt()
		return commonRCnt

	# Create an empty datatable with the same schema as other partitioned datatable
	def __create_datatable(self, cur_pid, prev_pid):
	  	# create an empty datatable
	  	new_datatable_name, prev_datatatble_name = const.getPublicSchema() + const.getDatatableName(self.dataset, cur_pid), const.getPublicSchema() + const.getDatatableName(self.dataset, prev_pid)
	  	sequence_name = new_datatable_name + "_rid_seq"
	  	sql = "CREATE TABLE %s (LIKE %s INCLUDING ALL);" % (new_datatable_name, prev_datatatble_name)
	  	sql += "CREATE SEQUENCE IF NOT EXISTS %s;" % sequence_name
	  	sql += "ALTER SEQUENCE %s START WITH 1;" % sequence_name
	  	sql += "ALTER TABLE %s ALTER COLUMN rid SET DEFAULT nextval('%s');" % (new_datatable_name, sequence_name)
	  	self.conn.cursor.execute(sql)
	  	self.conn.connect.commit()

	# Find the migration mapping from the optimal partition to the cur partition
	def __calc_partition_mapping(self):
		mapping = dict() # key <- opt_PID, val <- prev_PID
		pendingPIDs = set()

		for op in self._opt_partitions.values():
			min_PID, min_commonRCnt = -1, sys.maxint
			for pp in self._cur_partitions.values():
				diff = op.getRCnt() + pp.getRCnt() - 2 * self.__est_commontRCnt(op.getVList(), pp.getVList())
				if diff < min_commonRCnt:
					min_PID = pp.getPID()
					min_commonRCnt = diff

			if min_commonRCnt > op.getRCnt():
				pendingPIDs.add(op.getPID()) # itself is a single partition, i.e. no mapping from the pp
			else:
				mapping[op.getPID()] = min_PID

		max_prev_pid = max(self._cur_partitions.keys())
		tmp_pid = max_prev_pid+1
		for opPID in pendingPIDs:
			mapping[opPID] = tmp_pid
			self.__create_datatable(temp_pid, max_prev_pid)
			tmp_pid += 1

		return mapping

	def __migrate_partitiontable(self):
		partition_table = const.getPartitiontableName(self.dataset)
		# SQL drop the old table
		sql = "DELETE FROM %s;" % partition_table

		# Insert into new partitions
		for op in self._opt_partitions.values():
			sql += "INSERT INTO %s VALUES (%s);" % (partition_table, op.print_all())
		self.conn.cursor.execute(sql)
		self.conn.connect.commit()

	# return all distinct rids associated with any vid in the vlist
	def __get_rlist(self, vlist):
		rlist = set()

		verstion_table = const.getVersiontablName(self.dataset)
		sql = "SELECT rlist FROM %s WHERE vid = ANY(ARRAY[%s])" % (version_table, vlist)
		self.conn.cursor.execute(sql)

		for x in self.conn.cursor.fetchall():
			rlist |= set(x[0])
		return rlist

	def __delete_datatable(self, delete_prlist):
		sql = ""
		for pid in delete_prlist:
			datatable = const.getDatatableName(self.dataset, pid)
			# Delete records for prevPID
			sql += "DELETE FROM %s WHERE rid = ANY(ARRAY[%s]);" % (datatable, delete_prlist[pid])
		# TODO: Better off to drop the table if after deletion, the table is empty
		self.conn.cursor.execute(sql)
		self.conn.connect.commit()

		# Migrate datatable based on the current partitioning scheme
		# via INSERT & DELETE sqls
	def __migrate_datatable(self, mapping):
		partition_table = const.getPartitiontableName(self.dataset)
		delete_prlist = dict() # pid -> rlist

		for opt_PID in mapping:
			to_datatable = const.getDatatableName(self.dataset, opt_PID)
			cur_PID = mapping[opt_PID]
			cur_vlist = self._cur_partitions[cur_PID].getVList()
			opt_vlist = self._opt_partitions[opt_PID].getVList()

			cur_rlist = self.__get_rlist(cur_vlist - opt_vlist)
			opt_rlist = self.__get_rlist(opt_vlist - cur_vlist)
			commonRList = cur_rlist & opt_rlist
			insert_rlist = opt_rlist - commonRList
			delete_prlist[cur_PID] = cur_rlist - commonRList

			# Find cur_pIDs for each of the insert_rlist
			diff_vlist = opt_vlist - cur_vlist
			plist, vlist = set(), set()
			for p in self._cur_partitions.values():
				if p.getVList() & diff_vlist:
					plist.add(p.getPID())
					vlist |= (p.getVList() & diff_vlist)
			if vlist != diff_vlist:
				print "====== ERROR IN MIGRATION DATATABLE ======="

			sql = ""
			for pid in plist:
				from_datatable = const.getDatatableName(self.dataset, pid)
				sql += "INSERT INTO %s (SELECT * FROM %s WHERE rid = ANY(ARRAY[%s])) ON CONFLICT(rid) DO NOTHING;" % (to_datatable, from_datatable, insert_rlist)

			self.conn.cursor.execute(sql)
			self.conn.connect.commit()

		self.__delete_datatable(delete_prlist)

	# migration from one partition scheme to the opt one
	def __migration(self, pid):
		partition_mapping = self.__calc_partition_mapping()
		self.__migrate_datatable(partition_mapping)
		self.__migrate_partitiontable()
		return partition_mapping.keys()[partition_mapping.values().index(pid)]

	# Load data from partition table to the self._cur_partitions
	def __construct_cur_partitions(self):
		self._cur_partitions = dict()
		partition_table = const.getPartitiontableName(self.dataset)
		sql = "SELECT pid, vlist, rCnt, eCnt FROM %s;" % partition_table
		self.conn.cursor.execute(sql)
		for x in self.conn.cursor.fetchall():
			pid, vlist, rCnt, eCnt = x[0], x[1], x[2], x[3]
			self._cur_partitions[pid] = BipartiteGraph(pid, vlist, rCnt, eCnt)

	# Given the vid, assign the version to the correct partition
	# Trigger partition migration if needed
	# TODO: The current vid must be committed to the versiontable already
	def partition_assign(self, vid, cur_delta):
		cur_node = self.__construct_node(vid)

		parent_node = self.__construct_node(cur_node.getParentVID())
		parent_partition = self.__construct_partition(parent_node.getVID())
		parent_pid = parent_partition.getPID()
		post_partition_rCnt = parent_partition.getRCnt() + cur_node.getRCnt() - cur_node.getCommonRCnt()
		cur_storageCost = self.__calc_cur_storageCost()
		post_storage = cur_storageCost + cur_node.getRCnt() - cur_node.getCommonRCnt()
		cur_storage_threshold = cur_storageCost * self.storage_threshold

		pid = -1
		if cur_node.getCommonRCnt() <= cur_delta * post_partition_rCnt and post_storage < cur_storage_threshold:
			# add as a new partition
			pid = self.__insert_partition(cur_node)

		else:
			# update the partition entry its parent resides in
			self.__update_partition_table(parent_partition.getPID(), cur_node)
			pid = parent_partition.getPID()
		# Compare with the current number
		op = Optimizer(self.conn, self.dataset, cur_storage_threshold)
		if self.__calc_cur_checkoutCost() > self.tolerance * op.cal_opt_checkoutCost():
			self._opt_partitions = op.get_opt_partitions()
			self._version_tree = op.get_version_tree()
			self.__construct_cur_partitions()
			pid = self.__migration(pid)
			cur_delta = op.get_cur_delta()
		elif pid != parent_pid:
			self.__create_datatable(pid, parent_pid)

		return pid, parent_pid, parent_node.getVID(), cur_delta
	# init the partition table
	# no threshold checking since there is only one version as of now
	def partition_init(self):
		vid = 1
		cur_node = self.__construct_node(vid)
		pid = self.__insert_partition(cur_node)
		return pid

	# return all pids from the partition table
	@classmethod
	def get_cur_plist(self, cursor, dataset):
		partition_table = const.getPartitiontableName(dataset)

		sql = "SELECT pid FROM %s;" % partition_table
		cursor.execute(sql)
		plist = [x[0] for x in cursor.fetchall()]
		return plist

	# Given the vlist, return the pid each of the versions resides in
	@classmethod
	def plist_lookup(self, vlist, conn, dataset):
		partition_table = const.getPartitiontableName(dataset)
		plist = []
		for vid in vlist:
			sql = "SELECT pid FROM %s WHERE vlist @> ARRAY[%s];" % (partition_table, vid)
			conn.cursor.execute(sql)
			plist.append(conn.cursor.fetchall()[0][0])
		return plist

class Optimizer(object):
	def __init__(self, conn, dataset, cur_storage_threshold):
		self.conn = conn
		self.dataset = dataset
		self.cur_storage_threshold = cur_storage_threshold
		self._opt_partitions = dict() # pid -> Partition
		self._version_tree = dict() # vid -> Node
		self._init_partition = None # all current versions are in one partition
		self._root = None
		self._delta = None

	# initialize the version tree and the corresponding BipartiteGraph
	def __initialize(self):
		version_table = const.getVersiontableName(self.dataset)
		partition_rCnt, partition_eCnt, partition_vlist = 0, 0, set()
		node = None

		sql = "SELECT vid, num_records, parents, children, commonRCnt FROM %s;" % version_table
		self.conn.cursor.execute(sql)

		for x in self.conn.cursor.fetchall():
			vid, rCnt, parents, children, commonRCnt = x[0], x[1], x[2], x[3], x[4]

			max_idx = 0
			if (len(parents) > 1):
				max_idx = commonRCnt.index(max(commonRCnt))
			partition_vlist.add(vid)
			partition_rCnt += rCnt - commonRCnt[max_idx]
			partition_eCnt += rCnt

			node = Node(vid, rCnt, children, parents[max_idx], commonRCnt[max_idx])
			self._version_tree[vid] = node
			if node.isRoot():
				self._root = node

		self._init_partition = BipartiteGraph(1, partition_vlist, partition_rCnt, partition_eCnt)

	def __is_valid_edge(self, commonRCnt, rCnt, delta):
		return commonRCnt <= delta * rCnt

	# traverse through the tree starting at the Root
	# and pick the edge with the smallest number of commonRCnt(aka weight)
	def __pick_edge(self, root, rCnt, delta):
		minNode, minCommonRCnt = None, sys.maxint
		visited, stack = set(), [root]
		while stack:
			node = stack.pop()
			if node.getVID() not in visited:
				visited.add(node.getVID())
			for vid in node.getChildrenVList():
				stack.append(self._version_tree[vid])
			commonRCnt = node.getCommonRCnt()
			if (not node.isRoot() and self.__is_valid_edge(commonRCnt, rCnt, delta) and commonRCnt < minCommonRCnt):
				minNode = node
				minCommonRCnt = commonRCnt
		return minNode

	# return the delta value of the given interval
	def __nextDelta(self, left, right):
		return 0.5 * (left + right)

	# construct the subTree and bipartiteGraph at the split_node
	# each element in the partitions parameter of type {} is a bipartiteGraph
	def __splitGraph(self, split_node):
		visited, stack = set(), [split_node]
		vlist, rCnt, eCnt = set(), 0, 0
		while stack:
			node = stack.pop()
			if node.getVID() not in visited:
				visited.add(node.getVID())
			for vid in node.getChildrenVList():
				stack.append(self._version_tree[vid])
				# ======== Update bipartiteGraph statistics
			vlist.add(node.getVID())
			eCnt += node.getRCnt()
			rCnt += node.getRCnt() - node.getCommonRCnt()

		# Find the old partition it is in, and
		# update the old partition statistics
		parentPartition = None
		for op in self._opt_partitions.values():
			#if the bipartite graph contains overlapped vlist with the vlist in the tree
			if split_node.getVID() in op.getVList():
				op.update(op.getVList() - vlist, op.getRCnt() - rCnt, op.getECnt() - eCnt)
				parentPartition = op

		maxPID = 0
		if self._opt_partitions: # if this is not empty now
			maxPID = max(self._opt_partitions.keys())
		newPartition = BipartiteGraph(maxPID+1, vlist, rCnt + split_node.getCommonRCnt(), eCnt)
		self._opt_partitions[maxPID+1] = newPartition


		# Take care of the trees
		parent_vid = split_node.getParentVID()
		self._version_tree[parent_vid].setAsLeaf()
		split_node.setAsRoot()

		return newPartition, parentPartition
		# TODO: Err if there is more than one such bipartite Graph

	# Calculate the storage cost of the opt_partition
	def __calc_opt_partition_storageCost(self):
		storage_cost = 0
		for p in self._opt_partitions.values():
			storage_cost += p.getRCnt()
		return storage_cost

	# the fundamental approximate algorithm
	def __approx(self, root, partition, delta):
		if partition.calc_threshold() > delta or len(partition.getVList()) == 1:
			return
		split_node = self.__pick_edge(root, partition.getRCnt(), delta)

		if not split_node.isRoot():
			newPartition, parentPartition = self.__splitGraph(split_node)
			self.__approx(split_node, newPartition, delta)
			self.__approx(root, parentPartition, delta)
		return

	def __construct_opt_partition(self):
		# construct graph and tree
		self.__initialize()

		left = self._init_partition.calc_threshold()
		right = 1.0

		init_partition_back, init_version_tree_back = copy.deepcopy(self._init_partition), copy.deepcopy(self._version_tree)

		# binary search for the best delta
		while left < right:
			# variables cleaning up
			self._init_partition, self._version_tree = copy.deepcopy(init_partition_back), copy.deepcopy(init_version_tree_back)
			self._opt_partitions = dict()
			self._opt_partitions[self._init_partition.getPID()] = self._init_partition
			delta = self.__nextDelta(left, right)
			self.__approx(self._root, self._init_partition, delta)
			opt_storage = self.__calc_opt_partition_storageCost()
			if opt_storage < self.cur_storage_threshold:
				if opt_storage > 0.99 * self.cur_storage_threshold:
					break
				else:
					left = delta
			else:
				right = delta
		self._delta = delta

	def cal_opt_checkoutCost(self):
		self.__construct_opt_partition()
		checkoutCost = 0
		for op in self._opt_partitions.values():
			checkoutCost += op.getVCnt() * op.getRCnt()
		return checkoutCost

	def get_opt_partitions(self):
		return self._opt_partitions

	def get_version_tree(self):
		return self._versiont_tree

	def get_cur_delta(self):
		return self._delta