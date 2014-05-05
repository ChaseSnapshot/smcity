''' Interface for generating data set displays. '''

class DisplayTransformer:
    '''' Interface for generating data set displays. '''

    def plot_points(self, data_set_id, encoded_display=None):
        '''
        Generates a data display that is a simple plotting of points.

        @param data_set_id Dataset to plot
        @paramType string/uuid
        @param encoded_display Display to add the points to; If None, a new display is constructed
        @paramType string
        @returns encoded display content
        @returnType string
        '''
        raise NotImplementError()

    def plot_polygon(self, polygon_strategy, encoded_display=None, properties=None):
        '''
        Generates a display for the provided polygon.
 
        @param polygon_strategy Polygon to plot
        @paramType smcity.polygons.PolygonStrategy
        @param encoded_display Display to add the points to; If None, a new display is constructed
        @paramType string
        @returns encoded display content
        @returnType string
        '''
        raise NotImplementedError()
