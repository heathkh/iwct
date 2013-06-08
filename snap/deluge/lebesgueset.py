# lebesgue.py # tested with python 2.6 and 3.1 on a linux platform.
# source file: algebre.tm
# license: MIT license (http://en.wikipedia.org/wiki/MIT_License)
# copyright (c) 2010 Gribouillis at www.daniweb.com
""" Module lebesgue
This module defines a class LebesgueSet which instances represent
finite unions of intervals of real numbers. Set operations on these
finite unions of intervals include intersection, union, set difference,
symetric difference (xor), set complement.
Topological operations like testing wether a point is interior
or on the boundary, or adherent to the set are provided as well as
access to certain quantities, like the set's upper and lower bound
and the Lebesgue measure of the set.
Of course, the class is improperly named LebesgueSet because
a Lebesgue Set in mathematics is a wild set which can be much more
complicated than a finite union of intervals. However, conceptually,
it's best to consider that the intervals are nor opened nor closed,
and that an instance represents a union of intervals up to a set of
measure 0.
In this class, infinite values are represented by using the python value Float("infinity"). Infinite floats can be tested with math.isinf().
"""
import unittest 
import itertools 
from bisect import bisect 
import operator 
import sys 
if sys .version_info >=(3 ,):
  xrange =range 
class LebesgueSet (object ):
  _inf =float ('infinity')# can be tested with math.isinf()
  _minf =-_inf 
  UNION ,INTER ,XOR =range (3 )
  def __init__ (self ,points ,left_infinite =False ,fast =False ):
    """Create a new LebesgueSet object.
    @ points: a sequence of real numbers
    @ left_infinite : a boolean, True if the first interval is
                      semi-infinite ]-infinity, .[
    @ fast : can be set to True if the sequence of points is
             already sorted and has no duplicates.
    """
    if not fast :
      points =sorted (set (points ))
    self .points =list (points )
    self .left_infinite =bool (left_infinite )
    
  def intervals (self ):
    start =0 
    p =self .points 
    n =len (p )
    if self .left_infinite :
      if n :
        yield (self ._minf ,p [0 ])
        start =1 
      else :
        yield (self ._minf ,self ._inf )
        return 
    while start +1 <n :
      yield (p [start ],p [start +1 ])
      start +=2 
    if start <n :
      yield (p [start ],self ._inf )
      
  def __str__ (self ):
    return str (list (self .intervals ()))
  
  def __nonzero__ (self ):
    return self .left_infinite or self .points
   
  def __invert__ (self ):
    """Compute the set's complement"""
    return LebesgueSet (self .points ,not self .left_infinite ,fast =True )
  
  def is_bounded (self ):
    return not (self .left_infinite or (len (self .points )&1 ))
  
  def lower_bound (self ):
    if self .left_infinite :
      return self ._minf 
    elif not self .points :
      return self ._inf 
    else :
      return self .points [0 ]
    
  def upper_bound (self ):
    if self .left_infinite ^(len (self .points )&1 ):
      return self ._inf 
    elif not self .points :
      return self ._minf 
    else :
      return self .points [-1 ]
    
  def zoom (self ,center =0.0 ,factor =1.0 ):
    if factor ==0.0 :
      points =()
      left_infinite =False 
    else :
      points =(center +factor *(x -center )for x in self .points )
      if factor >0.0 :
        left_infinite =self .left_infinite 
      else :
        left_infinite =(self .upper_bound ()==self ._inf )
        points =reversed (points )
    return LebesgueSet (points ,left_infinite ,fast =True )
  
  def measure (self ):
    """self.measure() -> a finite or infinite float.
    Compute the Lebesgue measure of the set self. If this
    measure is infinite, return float('infinity')."""
    if self .is_bounded ():
      p = self.points
      return sum ( [float(p[i+1]) - p[i] for i in xrange(0,len(p),2)] )
    else :
      return self ._inf 
    
  def status (self ,x_real ):
    """self.status(x) returns -1, 0, 1 depending on x being outside,
    on the boundary, or inside the set self (x is a real number)"""
    i =bisect (self .points ,x_real )
    if (i >0 )and x_real ==self .points [i -1 ]:
      return 0 
    return 1 if self .left_infinite ^(i &1 )else -1 
  
  def is_interior (self ,x ):
    """True if x is in the topological interior of the set self."""
    return self .status (x )==1 
  
  def is_exterior (self ,x ):
    """True if x is in the topological interior of the complement."""
    return self .status (x )==-1 
  
  def is_boundary (self ,x ):
    """True if x is one end of one of the intervals"""
    return self .status (x )==0
   
  def is_adherent (self ,x ):
    """True if x is in one of the closed intervals defining self."""
    return self .status (x )>=0 
  
  def deltas (self ,negated =False ):
    infinite =self .left_infinite ^(1 if negated else 0 )
    assert infinite in (0 ,1 )
    for i ,x in enumerate (self .points ):
      yield (x ,-1 if (infinite ^(i &1 ))else 1 )
      
  @classmethod 
  def operate (cls ,op ,family ):
    """Compute the union, intersection or xor of a family of sets.
    @ op : one of LebesgueSet.UNION or .INTER or .XOR
    @ family : a non empty sequence of LebesgueSet's.
    A real number x belongs to the xor of the family if it belongs
    to an odd number of members of the family.
    """
    family =tuple (family )
    value =sum (u .left_infinite for u in family )
    P =[]
    if op ==cls .XOR :
      left_infinite =bool (value &1 )
      L =sorted (itertools .chain (*(u .points for u in family )))
      for k ,group in itertools .groupby (L ):
        if len (tuple (group ))&1 :
          P .append (k )
    else :
      inter =(op ==cls .INTER )
      if inter :
        value =len (family )-value 
      left_infinite =not (value )if inter else bool (value )
      L =sorted (itertools .chain (*(u .deltas (inter )for u in family )))
      for k ,group in itertools .groupby (L ,key =operator .itemgetter (0 )):
        group =tuple (group )
        new_value =value +sum (delta [1 ]for delta in group )
        if not (value and new_value ):
          P .append (k )
        value =new_value 
    return LebesgueSet (P ,left_infinite ,fast =True )
  
  def __and__ (self ,other ):
    """self & other -> intersection of self and other."""
    return self .operate (self .INTER ,(self ,other ))
  
  def __or__ (self ,other ):
    """self | other, self + other -> union of self and other."""
    return self .operate (self .UNION ,(self ,other ))
  __add__ =__or__ 
  
  def __xor__ (self ,other ):
    """self ^ other -> symetric difference of self and other."""
    return self .operate (self .XOR ,(self ,other ))
  
  def __sub__ (self ,other ):
    """self - other -> set difference."""
    return self &~other 
  
  @classmethod 
  def union (cls ,family ):
    """union of a family of Lebesgue sets."""
    return cls .operate (cls .UNION ,family )
  
  @classmethod 
  def inter (cls ,family ):
    """intersection of a family of Lebesgue sets"""
    return cls .operate (cls .INTER ,family )
  
  @classmethod 
  def xor (cls ,family ):
    """xor of a family of Lebesgue sets."""
    return cls .operate (cls .XOR ,family )
  
class LebesgueTestCase (unittest.TestCase ):
  """Test class for LebesgueSets."""
  def setUp (self ):
    import random 
    self .gauss =random .gauss 
    self .randint =random .randint 
    self .nbr_families =100 
    self .nbr_test_points =1000 
    
  def random_point (self ):
    return self .gauss (0 ,10 )
  
  def random_union (self ):
      points =(self .random_point ()for i in range (20 ))
      left_infinite =self .randint (0 ,1 )
      return LebesgueSet (points ,left_infinite )
  def random_family (self ):
    return [self .random_union ()for i in range (3 )]
  
  def runTest (self ):
    itt =itertools 
    for i in xrange (self .nbr_families ):
      family =self .random_family ()
      points =set (itt .chain (*(u .points for u in family )))
      union_res =LebesgueSet .union (family )
      inter_res =LebesgueSet .inter (family )
      xor_res =LebesgueSet .xor (family )
      for j in xrange (self .nbr_test_points ):
        while True :
          x =self .random_point ()
          if not x in points :
            break 
        status_list =list (u .status (x )for u in family )
        us_ ,is_ ,xs_ =(u .status (x )
        for u in (union_res ,inter_res ,xor_res ))
        u_expected =1 if any (s ==1 for s in status_list )else -1 
        i_expected =1 if all (s ==1 for s in status_list )else -1 
        x_expected =1 if sum (s ==1 for s in status_list )&1 else -1 
        self .assertEqual (us_ ,u_expected ,"Union failed")
        self .assertEqual (is_ ,i_expected ,"Intersection failed")
        self .assertEqual (xs_ ,x_expected ,"Xor failed")
        
if __name__ =="__main__":
  unittest .main ()