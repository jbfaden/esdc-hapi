
package org.hapiserver.source.cdaweb;

import java.util.Iterator;
import org.hapiserver.HapiRecord;

/**
 * convert AvailabilitySource iterator into a granule iterator
 * @author jbf
 */
public class AvailabilityIterator implements Iterator<int[]> {

    private final Iterator<HapiRecord> it;
    private String file; // the current file

    public AvailabilityIterator( Iterator<HapiRecord> it ) {
        this.it= it;
    }
    
    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public int[] next() {
        HapiRecord hr= it.next();
        String start= hr.getIsoTime(0);
        String stop= hr.getIsoTime(1);
        this.file= hr.getString(2);
        
        return new int[] { 
            Integer.parseInt(start.substring(0,4)),
            Integer.parseInt(start.substring(5,7)),
            Integer.parseInt(start.substring(8,10)),
            Integer.parseInt(start.substring(11,13)),
            Integer.parseInt(start.substring(14,16)),
            Integer.parseInt(start.substring(17,19)),
            0,
            Integer.parseInt(stop.substring(0,4)),
            Integer.parseInt(stop.substring(5,7)),
            Integer.parseInt(stop.substring(8,10)),
            Integer.parseInt(stop.substring(11,13)),
            Integer.parseInt(stop.substring(14,16)),
            Integer.parseInt(stop.substring(17,19)),
            0
        };
    }

    /**
     * return the file for the current interval.  This should be called after next().
     * @return the file associated with the time returned in next().
     */
    String getFile() {
        return this.file;
    }

    
}
