''' 
Interface for breaking complex geographic polygons into multiple bounding boxes which
can be used to generate sub-datasets representing those complex geographic polygons i.e. counties, states
'''

from smcity.analytics.worker import ComplexFilter

class PolygonStrategy(ComplexFilter):
    '''
    Interface for breaking complex geographic polygons into multiple bounding boxes which
    can be used to generate sub-datasets representing those complex geographic polygons 
    i.e. counties, states
    '''

    def get_bounding_box(self):
        '''
        @returns Bounding box to use as an initial filter when trying to retrieve data inside the polygon
        @returnType dictionary containing 'min_lat', 'min_lon', 'max_lat', 'max_lon'
        '''
        raise NotImplementedError()

    def get_outline(self):
        '''
        @returns A list of (lon, lat) points that created a circuit outline of the polygon
        @returnType list of (lon/float, lat/float)
        '''
        raise NotImplementedError()
