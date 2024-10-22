
package org.hapiserver.source;

import java.text.ParseException;
import java.util.Iterator;
import org.hapiserver.TimeUtil;
import org.hapiserver.URITemplate;

/**
 *
 * @author jbf
 */
public class AggregationGranuleIterator implements Iterator<int[]> {

    String[] result;
    int next=0;
    URITemplate uriTemplate;
    
    public AggregationGranuleIterator( String fileFormat, int[] start, int[] stop ) {
        this.uriTemplate= new URITemplate(fileFormat);
        
        try {
            result= URITemplate.formatRange( fileFormat,
                TimeUtil.isoTimeFromArray( start ),
                TimeUtil.isoTimeFromArray( stop ) );
        } catch (ParseException ex) {
            throw new RuntimeException(ex);
        }
            
           
    }
    @Override
    public boolean hasNext() {
        return result.length>next;
    }

    @Override
    public int[] next() {
        try {
            int i= next;
            next++;
            return uriTemplate.parse(result[i]);
        } catch (ParseException ex) {
            throw new RuntimeException(ex);
        }
    }

}
