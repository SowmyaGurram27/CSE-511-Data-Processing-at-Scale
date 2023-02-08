#!/usr/bin/env python
# coding: utf-8

# In[1]:


from unqlite import UnQLite

db = UnQLite('sample.db')
data = db.collection('data')

import math


# In[2]:


# Graded Cell, PartID: o1flK
def FindBusinessBasedOnCity(cityToSearch,saveLocation1,collection):
    business_file = filter(lambda user: user['city'] == cityToSearch, collection)
    with open(saveLocation1, "w") as file:
        for business in business_file:
            name = business['name']
            full_address = business['full_address']
            city = business['city']
            state = business['state']
            file.write("{}${}${}${}\n".format(name, full_address, city, state))
    file.close()

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):

    def DistanceFunction(lat2, lon2, lat1, lon1):
        import math
        R = 3959
        g1 = math.radians(lat1)
        g2 = math.radians(lat2)
        delta_g = math.radians(lat2-lat1)
        delta_l = math.radians(lon2-lon1)
        a = (math.sin(delta_g/2) * math.sin(delta_g/2)) + (math.cos(g1) * math.cos(g2) * math.sin(delta_l/2) * math.sin(delta_l/2))
        c = 2 * math.atan2(math.sqrt(a),math.sqrt(1-a))
        d = R * c
        
        return d

    business_Name= []
    
    lat1 = myLocation[0]
    lon1 = myLocation[1]
    
    for ind in range(len(collection.all())):
        res = collection.fetch(ind)
        
        lat2 = res['latitude']
        lon2 = res['longitude']
        
        dist = DistanceFunction(lat2, lon2, lat1, lon1)
        if(dist <= maxDistance):
            for value in res['categories']:
                if(value == categoriesToSearch[0]):
                    business_Name.append(res['name'])

    file1 = open(saveLocation2, "w")
    for name in business_Name:
        file1.write(name + '\n')
    file1.close()
    pass


# In[3]:


true_results = ["VinciTorio's Restaurant$1835 E Elliot Rd, Ste C109, Tempe, AZ 85284$Tempe$AZ", "P.croissants$7520 S Rural Rd, Tempe, AZ 85283$Tempe$AZ", "Salt Creek Home$1725 W Ruby Dr, Tempe, AZ 85284$Tempe$AZ"]

try:
    FindBusinessBasedOnCity('Tempe', 'output_city.txt', data)
except NameError as e:
    print ('The FindBusinessBasedOnCity function is not defined! You must run the cell containing the function before running this evaluation cell.')
except TypeError as e:
    print ("The FindBusinessBasedOnCity function is supposed to accept three arguments. Yours does not!")
    
try:
    opf = open('output_city.txt', 'r')
except FileNotFoundError as e:
    print ("The FindBusinessBasedOnCity function does not write data to the correct location.")
    
lines = opf.readlines()
if len(lines) != 3:
    print ("The FindBusinessBasedOnCity function does not find the correct number of results, should be 3.")
    
lines = [line.strip() for line in lines]
if sorted(lines) == sorted(true_results):
    print ("Correct! You FindBusinessByCity function passes these test cases. This does not cover all possible test edge cases, however, so make sure that your function covers them before submitting!")


# In[4]:


true_results = ["VinciTorio's Restaurant"]

try:
    FindBusinessBasedOnLocation(['Buffets'], [33.3482589, -111.9088346], 10, 'output_loc.txt', data)
except NameError as e: 
    print ('The FindBusinessBasedOnLocation function is not defined! You must run the cell containing the function before running this evaluation cell.')
except TypeError as e:
    print ("The FindBusinessBasedOnLocation function is supposed to accept five arguments. Yours does not!")
    
try:
    opf = open('output_loc.txt','r')
except FileNotFoundError as e:
    print ("The FindBusinessBasedOnLocation function does not write data to the correct location.")

lines = opf.readlines()
if len(lines) != 1:
    print ("The FindBusinessBasedOnLocation function does not find the correct number of results, should be only 1.")

if lines[0].strip() == true_results[0]:
    print ("Correct! Your FindBusinessBasedOnLocation function passes these test cases. This does not cover all possible edge cases, so make sure your function does before submitting.")


# In[ ]:





# In[ ]:





# In[ ]:




