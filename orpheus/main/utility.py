# Each node corresponds a version in a tree (not a graph)
class Node(object):
	def __init__(self, vid=-1, rCnt=-1, childrenVlist=None, parentVid=-1, commonRCnt=-1):
		self.vid = vid
		self.rCnt = rCnt
		self.childrenVlist = childrenVlist
		self.parentVid = parentVid
		self.commonRCnt = commonRCnt

	def getVID(self):
		return self.vid

	def getRCnt(self):
		return self.rCnt

	def getChildrenVList(self):
		return self.childrenVlist

	def getParentVID(self):
		return self.parentVid

	def getCommonRCnt(self):
		return self.commonRCnt

	def setAsRoot(self):
		self.parentVid = -1
		self.commonRCnt = 0

	def setAsLeaf(self):
		self.childrenVlist = []

	def isRoot(self):
		if self.parentVid == -1:
			return True
		return False

# Each partition can be represented as a bipratiteGraph
class BipartiteGraph(object):
	def __init__(self, pid=-1, vlist=None, rCnt=-1, eCnt=-1):
		self.pid = pid
		self.vlist = vlist
		self.rCnt = rCnt
		self.eCnt = eCnt

	def getPID(self):
		return self.pid

	def getVList(self):
		return self.vlist

	def getRCnt(self):
		return self.rCnt

	def getECnt(self):
		return self.eCnt

	def getVCnt(self):
		return len(self.vlist)

	def update(self, vlist, rCnt, eCnt):
		self.vlist = vlist
		self.rCnt = rCnt
		self.eCnt = eCnt

	def calc_threshold(self):
		return float(self.eCnt) / (self.rCnt * len(self.vlist))

	def print_all(self):
		return "%s, %s, %s, %s" % (self.pid, self.vlist, self.rCnt, self.eCnt)





