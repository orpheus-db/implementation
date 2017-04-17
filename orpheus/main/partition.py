from utility import Node, BipartiteGraph
import sys

class Partition(object):
		def __init__(self, conn, storage_threshold, tolerance): #TODO
			self.conn = conn
			self.tolerance = tolerance # TODO A User defined parameter
			self.storage_threshold = storage_threshold # TODO A User defined parameter
			self._cur_partitions = None
			self._opt_partitions = None
			self._prev_delta = None
			self._version_tree = None

		# Retrieve the total rCnt among all partitions in the table
		def __calc_cur_storageCost(self):
			partition_table ="dataset1_partitiontable"
			sql = "SELECT SUM(rCnt) FROM %s" % partition_table

			self.conn.cursor.execute(sql)
			cur_storage = self.conn.cursor.fetchall()[0][0]
			return cur_storage

		def __calc_cur_checkoutCost(self):
			partition_table ="dataset1_partitiontable" #TODO

			sql = "SELECT SUM(cardinality(vlist) * rCnt) FROM %s;" % partition_table

			self.conn.cursor.execute(sql)
			cur_checkoutCost = self.conn.cursor.fetchall()[0][0]
			return cur_checkoutCost

		# Construct a Node given the vid
		def __construct_node(self, vid):
			version_table = "dataset1_version" #TODO
			sql = "SELECT vid, rCnt, parents, children, commonRCnt FROM  %s WHERE vid = %s" % (version_table, vid)

			self.conn.cursor.execute(sql)
			for x in self.conn.cursor.fetchall():
				vid, rCnt, parents, children, commonRCnt = x[0], x[1], x[2], x[3], x[4]

			max_idx = 0
			if (len(parents) > 1):
				max_idx = commonRCnt.index(max(commonRCnt))

			return Node(vid, rCnt, children, parent[max_idx], commonRCnt[max_idx])

		# Return the partition instance given the vid
		def __get_partition(self, vid):
			partition_table = "dataset1_partition" #TODO
			sql = "SELECT pid, vlist, rCnt, eCnt FROM  %s WHERE vlist @> ARRAY[%s]" % (partition_table, vid)
			self.conn.cursor.execute(sql)
			for x in self.conn.cursor.fetchall():
				pid, vlist, rCnt, eCnt = x[0], x[1], x[2], x[3]

			return BipartiteGraph(pid, vlist, rCnt, eCnt)

		# insert a new partition entry
		def __insert_partition(self, node):
			partition_table = "dataset1_partition"
			sql = "INSERT INTO %s (vlist, rCnt, eCnt) VALUES (%s, %s, %s) RETURNING pid;" % (partition_table, node.getVID(), node.getRCnt(), node.getRCnt())
			self.conn.cursor.execute(sql)
			cur_pid = self.conn.cursor.fetchall()[0][0]
			self.conn.connect.commit()

			return cur_pid

		def __update_partition_table(self, pID, cur_node):
			partition_table = "dataset1_partition" #TODO

			sql = "UPDATE %s SET vlist=array_append(vlist, %s), rCnt=rCnt+%s, eCnt=eCnt+%s WHERE pid=%s;"
							% (partition_table, cur_node.getVID(), cur_node.getRCnt() - cur_node.getCommonRCnt(), cur_node.getRCnt(), pID);
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

		def __create_datatable(self, cur_pid, prev_pid):
			datatable_prefix = "dataset1_datatale_p%s"
			sql = "CREATE TABLE %s AS TABLE %s WITH NO DATA;" % (datatable_prefix+cur_pid, datatable_prefix+prev_pid)
			self.conn.cursor.execute(sql)
			self.conn.connect.commit()

		# Find the migration mapping from the optimal partition to the cur partition
		def __calc_partition_mapping(self):
			mapping = dict() # key <- opt_PID, val <- prev_PID
			pendingPIDs = set()
			for op in self._opt_partitions.values():
				min_PID = -1, min_commonRCnt = sys.maxint
				for pp in self._cur_partitions.values():
					diff = op.getRCnt() + pp.getRCnt() - 2 * self.__est_commontRCnt(op.getVList(), pp.getVList())
					if diff < min_commonRCnt:
						min_PID = pp.getPID()
						min_commonRCnt = diff

				if min_commonRCnt > op.getRCnt():
					pendingPIDs.add(op) # itself is a single partition, i.e. no mapping from the pp
				else:
					mapping[op.getPID()] = min_PID

			max_prev_pid = max(self._cur_partitions.keys()), tmp_pid = max_prev_pid + 1
			for op in pendingPIDs:
				mapping[op.getPID()] = tmp_pid
				self.__create_datatable(temp_pid, max_prev_pid)
				tmp_pid ++

			return mapping

		def __migrate_partitiontable(self):
			partition_table = "dataset1_partitiontable"
			# SQL drop the old table
			sql = "DELETE FROM %s;" % partition_table

			# Insert into new partitions
			for p in self.opt_partitions.values():
				sql += "INSERT INTO %s (vlist, rCnt, eCnt) VALUES (%s);" % (partition_table, p.print())
			self.conn.cursor.execute(sql)
			self.conn.connect.commit()

		# given the pid, return the rlist
		def __get_rlist(self, vlist):
			rlist = set()
			verstion_table = "dataset1_versiontable" # TODO
			sql = "SELECT rlist FROM %s WHERE vid <@ %s" % (version_table, vlist)
			self.conn.cursor.execute(sql)

			for x in self.conn.cursor.fetchall():
				rlist.add(x[0][0])
			return rlist

		def __delete_datatable(self, delete_prlist):
			sql = ""
			for pid in delete_prlist:
				datatable = "dataset1_datatable_p%s" % pid
				# Delete records for prevPID
				sql += "DELETE FROM %s WHERE rid = ANY(ARRAY[%s]);" % (datatable, delete_prlist[pid])
			self.conn.cursor.execute(sql)
			self.conn.connect.commit()

		# Update datatable
		def __migrate_datatable(self, mapping):
			partition_table = "dataset1_partitiontable"
			pendingPIDs = set(), delete_prlist = dict() #pid -> rlist

			for opt_PID in mapping:
				prev_PID = mapping[opt_PID]
				prev_rlist = self.__get_rlist(self._cur_partitions[prev_PID].getVList())
				cur_rlist = self.__get_rlist(opt_partitions[opt_PID].getVList())
				commonRList = prev_rlist & cur_rlist
				insert_rlist = cur_rlist - commonRList
				delete_prlist[prev_PID] = prev_rlist - commonRList

				diff_vlist = opt_partitions[opt_PID].getVList() - self._cur_partitions[prev_PID].getVList()
				plist = set(), vlist = set()
				for p in self._cur_partitions.values():
					if p.getVList() && diff_vlist:
						plist.add(p.getPID())
						vlist.add(p.getVList() && diff_vlist)
				if vlist != diff_vlist:
					print "====== ERROR IN MIGRATION DATATABLE ======="

				sql = ""
				for pid in plist:
					from_datatable = "dataset1_datatable_p%s" % pid
					sql += "INSERT INTO %s (SELECT * FROM %s WHERE rid = ANY(ARRAY[%s])) ON CONFLICT(rid) DO NOTHING;" % (datatable, from_datatable, insert_rlist)

				self.conn.cursor.execute(sql)
				self.conn.connect.commit()

			self.__delete_datatable(delete_prlist)

		#migration from one partition scheme to the opt one
		def __migration(self):
			partition_mapping = self.__calc_partition_mapping()
			self.__migrate_datatable(prev_PID, cur_PID)
			self.__migrate_partitiontable()

		def __construct_cur_partition(self):
			partition_table = "dataset1_partition" #TODO
			sql = "SELECT pid, vlist, rCnt, eCnt FROM  %s;" % (partition_table, vid)
			self.conn.cursor.execute(sql)
			for x in self.conn.cursor.fetchall():
				pid, vlist, rCnt, eCnt = x[0], x[1], x[2], x[3]
				self._cur_partitions[pid] = BipartiteGraph(pid, vlist, rCnt, eCnt)

		def partition_assign(self, vid):
			cur_node = self.__construct_node(vid)

			parent_node = self.__construct_node(cur_node.getParentVID())
			parent_partition = self.__get_partition(parent_node.getVID())

			post_partition_rCnt = parent_partition.getRCnt() + cur_node.getRCnt() - cur_node.getCommonRCnt()
			post_storage = self.__calc_cur_storageCost() + cur_node.getRCnt() - cur_node.getCommonRCnt()

			# TODO: self.delta has been updated correctly?
			pid = -1
			if cur_node.getCommonRCnt() <= self._delta * post_partition_rCnt and post_storage < self.storage_threshold:
				# add as a new partition
				pid = self.__insert_partition(cur_node)

			else:
				# update the partition entry its parent resides in
				self.__update_partition_table(parent_partition.getPID(), cur_node)
				pid = parent_partition.getPID()

			# TODO: update corresponding index table and datatable_p%s % pid

			#Compare with the current number
			op = Optimizer()
			if self.__calc_cur_checkoutCost() > self.tolerance * op.cal_opt_checkoutCost():
				self._opt_partitions = op.get_opt_partitions()
				self._version_tree = op.get_version_tree()
				self.__construct_cur_partition()
				self.__migration()


class Optimizer(object):
		def __init__(self):
      self._opt_partitions = dict() # pid -> Partition
      self._version_tree = dict() # vid -> Node
      self._init_partition = None # all current versions are in one partition
      self._root = None
      self._delta = None

    # initialize the version tree and the corresponding BipartiteGraph
    def __initialize(self):
      version_table = "dataset1_versiontable" #TODO
      partition_rCnt, partition_eCnt, partition_vlist = 0, 0, set()
      node = None

      sql = "SELECT vid, rCnt, parents, children, commonRCnt FROM %s;" % version_table
      self.conn.cursor.execute(sql)

      for x in self.conn.cursor.fetchall():
        vid, rCnt, parents, children, commonRCnt = x[0], x[1], x[2], x[3], x[4]

        max_idx = 0
        if (len(parents) > 1):
          max_idx = commonRCnt.index(max(commonRCnt))

        partition_vlist.append(vid)
        partition_rCnt += rCnt - commonRCnt[max_idx]
        partition_eCnt += rCnt

        node = Node(vid, rCnt, children, parent[max_idx], commonRCnt[max_idx])
        self._version_tree[vid] = node

        if node.isRoot():
          self._root = node

      self._init_partition = BipartiteGraph(-1, partition_vlist, partition_rCnt, partition_eCnt)


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
            stack.append(self.version_tree[vid])
          commonRCnt = node.getCommonRCnt()
          if (self.__is_valid_edge(commonRCnt, rCnt, delta) and commonRCnt < minCommonRCnt):
            minNode = node
            minCommonRCnt = commonRnt
      return minNode

    # return the delta value of the given interval
    def __nextDelta(self, left, right):
      return 0.5 * (left + right)

    # construct the subTree and bipartiteGraph at the split_node
    # each element in the partitions parameter of type {} is a bipartiteGraph
    def __splitGraph(self, split_node):
      visited, stack = set(), [split_node]
      vlist, rCnt, eCnt = set(), 0, 0

      # Take care of the trees
      split_node.setAsRoot()
      parent_vid = split_node.getParentVID()
      self._version_tree[parent_vid].setAsLeaf()

      while stack:
        node = stack.pop()
        if node.getVID() not in visited:
          visited.add(node.getVID())
          for vid in node.getChildrenVList():
            stack.append(self._version_tree[vid])
          # ======== Update bipartiteGraph statistics
          vlist.add(node.getVID())
          eCnt += node.getRCnt()
          rCnt += node.getRCnt() - node.getCommonRnt()

      maxPID = -1
      if not self._opt_partitions:
      	maxPID = max(self._opt_partitions.keys())
      newPartition = BipartiteGraph(maxPID+1, vlist, rCnt, eCnt)
      self._opt_partitions[maxPID+1] = newPartition

      # Find the old partition it is in, and
      # update the old partition statistics
      parentPartition = None
      for op in self._opt_partitions.values():
        #if the bipartite graph contains overlapped vlist with the vlist in the tree
        if op.getVList() & split_node.getVID():
          op.update(op.getVList() - vlist, op.getRCnt() - rCnt + split_node.getCommonRnt(), op.getECnt() - eCnt)
          parentPartition = op

      return newPartition, parentPartition
      # TODO: Err if there is more than one such bipartite Graph


    # Calculate the storage cost of the current opt_partition
    def __calc_partition_storageCost(self):
    	storage_cost = 0
      for p in self._opt_partitions.values():
      	storage_cost += p.getRCnt()
      return storage_cost

    # the fundamental approximate algorithm
    def __approx(self, root, partition, delta):
      if partition.calc_threshold() > delta:
        return
      split_node = self.__pick_edge(root, partition.getRCnt(), delta)
      newPartition, parentPartition = self.__splitGraph(split_node)
      self.__approx(split_node, newPartition, delta)
      self.__approx(root, parentPartition, delta)
      return

    def __construct_opt_partition(self):
      # construct graph and tree
      self.__initialize()

      left = self._init_partition.calc_threshold()
      right = 1.0
      init_partition_back = self._init_partition, init_version_tree_back = self._version_tree

      # binary search for the best delta
      while left < right:
      	# variables cleaning up
        self._opt_partitions = dict()
        self._init_partition = init_partition_back, self._version_tree = init_version_tree_back

        delta = self.__nextDelta(left, right)
        self.__approx(self._root, self._init_partition, delta)
        cur_storage = self.__calc_partition_storageCost()

        if cur_storage < self.storage_threshold:
          if cur_storage > 0.99 * self.storage_threshold:
            break
          else:
            left = delta
        else:
          right = delta
      #TODO: record delta to a file..
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