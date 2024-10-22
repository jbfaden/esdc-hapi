
package org.hapiserver;

import java.util.Iterator;

/**
 * convenient code so that implementations need only implement hasGranuleIterator, hasParamSubsetIterator and
 * implementing methods.
 * @author jbf
 */
public abstract class AbstractHapiRecordSource implements HapiRecordSource {
    
    @Override
    public abstract boolean hasGranuleIterator();
    
    @Override
    public Iterator<int[]> getGranuleIterator(int[] start, int[] stop) {
        throw new UnsupportedOperationException("Not implemented"); 
    }
    
    @Override
    public abstract boolean hasParamSubsetIterator( );
    
    @Override
    public Iterator<HapiRecord> getIterator(int[] start, int[] stop, String[] params) {
        throw new UnsupportedOperationException("Not implemented"); 
    }

    @Override
    public Iterator<HapiRecord> getIterator(int[] start, int[] stop) {
        throw new UnsupportedOperationException("Not implemented"); 
    }

    @Override
    public String getTimeStamp(int[] start, int[] stop) {
        return null;
    }
    
    @Override
    public void doFinalize() {
    }

}
