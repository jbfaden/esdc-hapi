
package org.hapiserver;

import java.util.Iterator;

/**
 * Interface that provides the record source.
 * @author jbf
 */
public interface HapiRecordSource {

    /**
     * if true, then this data source should only be called for intervals identified by the 
     * iterator.
     * @return 
     */
    public boolean hasGranuleIterator();
    
    /**
     * return the iterator that identifies the intervals to load.  This only needs to be
     * implemented if hasGranuleIterator returns true.
     * @param start the seven component start time [ Y, m, d, H, M, S, N ]
     * @param stop the seven component stop time [ Y, m, d, H, M, S, N ]
     * @return the granule iterator which returns [ start Y, m, d, H, M, S, N, stop Y, m, d, H, M, S, N ]
     */
    public Iterator<int[]> getGranuleIterator( int[] start, int[] stop );
    
    /**
     * when true is returned, the data source will handle the parameter subsetting.  For
     * example, a CDF file may have hundreds of parameters in on data set, but in general only
     * a few will be used.  Only one of the getIterator methods should be implemented.
     * @return true if the source will handle the subsetting.
     */
    public boolean hasParamSubsetIterator( );
    
    /**
     * return the iterator the subsets the parameters, or throw an IllegalArgumentException when this is not supported.
     * @param start
     * @param stop
     * @param params
     * @return the iterator
     */
    public Iterator<HapiRecord> getIterator( int[] start, int[] stop, String[] params);
    
    /**
     * return the iterator that returns all the parameters, or throw an IllegalArgumentException when this is not supported.
     * @param start the start time
     * @param stop the stop time
     * @return the iterator
     */
    public Iterator<HapiRecord> getIterator( int[] start, int[] stop );
    
    /**
     * return null or a isotime time stamp for the interval, to be used when caching data.  When the
     * source has a granuleIterator, this will only be called for each granule.
     * Clients can send a timeStamp indicating the oldest granule they have cached, and the
     * server may then indicate that the cached data should be used.
     * @param start seven component start time
     * @param stop seven component stop time
     * @return the isotime for the range, or null.
     */
    public String getTimeStamp( int[] start, int[] stop );
    
    /**
     * use of this source is complete and any resources used can be disposed of.
     */
    public void doFinalize();
    
}
