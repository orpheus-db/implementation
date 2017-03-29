class Node(object):
    def __init__(self, vid, children, parent, commonRCnt):
      self.vid = vid
      self.children = children
      self.parent = parent
      self.commonRCnt = commonRCnt
    def getChildren(self):
      return self.children
    def getParent(self):
      return self.parent
    def getCommonRCnt(self):
      return self.commonRCnt
    def getVid(self):
      return self.vid

class BipartiteGraph(object):
  def __init__(self, rCnt, vCnt, eCnt):
      self.rCnt = rCnt
      self.vCnt = vCnt
      self.eCnt = eCnt3
  def getRCnt(self):
      return self.rCnt
  def getvCnt(self):
      return self.vCnt
  def geteCnt(self):
      return self.eCnt
  def calc_threshold(self):
      return float(self.eCnt) / (self.rCnt + self.vCnt)



