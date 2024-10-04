import geocoder

def get_state(lat, long):  
    g = geocoder.osm([lat, long], method='reverse')    
    return g.state

lat = 32.5467483
long = -86.5315257


state = get_state(lat, long)
print(state)
