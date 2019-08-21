from itertools import combinations 
main_tup = []
even = []
odd = []
rm_tuple = []

def rSubset(arr, r): 
    return list(combinations(arr, r))

def getData(num):
    input_num = num[0]
    check_num = num[1:]
    
    arr = range(1,input_num + 1) 
    r = 2
    main_tup.append(rSubset(arr, r))
    
    for i in range(1,9):
        if(i%2 == 0):
            even.append(i)
        else:
            odd.append(i)
    
    for i in range(len(odd)):
        if i<=len(odd) - r:
            rm_tuple.append((odd[i],even[i+1]))
        
    for i in range(len(even)):
        if i<=len(even) - r:
            rm_tuple.append((even[i],odd[i+1]))
    
    for i in main_tup[0]:
        for j in check_num:
            if(i[0] == j or i[1] == j):
                rm_tuple.append(i)
            if(i[1] > i[0] + r):
                rm_tuple.append(i)                
    
                
    return set(main_tup[0]) - set(rm_tuple)
    
    
 getData([8, 1, 4, 8])
