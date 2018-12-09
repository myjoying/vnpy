import numpy as np
from bson import ObjectId

class CtCycleArray():
    """
    支持下标操作，但不支持切片操作
    """
    def __init__(self, size = 100):
        self.arraysize = size
        self.realsize = 0
        self.data = []
        
    def __str__(self):
        return "realsize="+str(self.realsize) + "capacity="+str(self.arraysize) + " " + str(self.data)
        
    def append(self,obj):
        if self.realsize >= self.arraysize:
            first = self.data[0]
            self.data.remove(self.data[0])
        self.data.append(obj)
        self.realsize += 1
    
    def size(self):
        return self.realsize
    
    def __getitem__(self, i):
        if i<0 or self.realsize<=self.arraysize:
            return self.data[i]
        else:
            index = i-(self.realsize-self.arraysize)
            if index>=0:
                return self.data[index] 
            else:
                return self.data[self.realsize]
            
    def __len__(self):
        return self.realsize
    
class CtCycleArray2():
    """
    支持下标操作，但不支持切片操作
    """
    def __init__(self, size = 100):
        self.arraysize = size
        if size>100:
            self.capacity = size + 100;
        else:
            self.capacity = 2*size
        self.capacity_used = 0
        self.realsize = 0
        self.data = []
        
    def __str__(self):
        return "realsize="+str(self.realsize) + "useful size=" + str(self.arraysize) + "capacity="+str(self.capacity) + " " + str(self.data)
        
    def append(self,obj):
        if self.capacity_used >= self.capacity:
            self.data[0:self.arraysize] = self.data[self.capacity-self.arraysize:self.capacity]
            self.capacity_used = self.arraysize-1
            
        self.data.append(obj)
        self.realsize += 1
        self.capacity_used += 1
    
    def size(self):
        return self.realsize
    
    def __getitem__(self, i):
        
        if self.realsize<=self.arraysize:
            if i<0 and abs(i)>self.realsize:
                raise IndexError
            if i>=0 and i>self.realsize:
                raise IndexError
            return self.data[i]
        else:
            if i<0:
                if abs(i)>self.arraysize:
                    raise IndexError
                else:
                    return self.data[i]
                
            if i>=0 and (i>self.realsize or i<self.arraysize-self.arraysize):
                raise IndexError
            
            return self.data[i-(self.realsize-self.arraysize)]
        
            
    def __len__(self):
        return self.realsize    

if __name__ == '__main__':
    
    a = CtCycleArray2(2)
    a.append(1)
    a.append(2)
    a.append(3)
    b = len(a)
    d = np.size(a)