import math

def haversine_m(lat1, lon1, lat2, lon2):
    """Return distance between two lat/lon points in meters"""
    R = 6371000  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = math.sin(d_phi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(d_lambda/2)**2
    c = 2*math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R*c

def grid_key(lat, lon, precision=2):
    """
    Simple geo-grid key. precision=2 → ~1 km grid
    """
    lat_grid = round(lat, precision)
    lon_grid = round(lon, precision)
    return f"{lat_grid}:{lon_grid}"
