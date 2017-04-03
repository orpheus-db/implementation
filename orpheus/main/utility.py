class Node(object):

    def __init__(self, vid, rCnt, children, parent, commonRCnt):
      self.vid = vid
      self.rCnt = rCnt
      self.children = children
      if len(parents) == 1 and parents[0] == -1:
        self.parent = None
      else:
        self.parent = parent
      self.commonRCnt = commonRCnt

    def getVid(self):
      return self.vid

    def getRCnt(self):
      return self.rCnt

    def getChildren(self):
      return self.children

    def getParent(self):
      return self.parent

    def getCommonRCnt(self):
      return self.commonRCnt

    def setParent(self, parent):
      self.parent = parent 
      
    def setChildren(self, children):
      self.children = children

# Each BipartiteGraph corresponding to a partition
class BipartiteGraph(object):

  def __init__(self, vlist=set(), rCnt, eCnt):
      self.vlist = vlist
      self.rCnt = rCnt
      self.eCnt = eCnt

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
    return float(self.eCnt) / (self.rCnt + len(self.vlist))


