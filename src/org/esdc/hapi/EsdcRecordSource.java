
package org.esdc.hapi;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.Iterator;
import org.hapiserver.AbstractHapiRecordSource;
import org.hapiserver.HapiRecord;
import org.hapiserver.TimeUtil;
import org.hapiserver.source.SourceUtil;

/**
 * https://soar.esac.esa.int/soar-sl-tap/tap/sync?
 *     REQUEST=doQuery&LANG=ADQL&FORMAT=json
 *     &QUERY=SELECT+filename,+filepath+FROM+v_sc_data_item+
 *     WHERE+begin_time%3E%272020-08-29+00:00:00%27+
 *     AND+end_time%3E%272020-09-01+00:00:00%27+
 *     AND+data_item_id+LIKE+%27solo_L2_rpw-lfr-surv-asm%25%27
 * 
 * wget -O - 'https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=json&QUERY=SELECT+begin_time,end_time,filename,+filepath+FROM+v_sc_data_item+WHERE+begin_time%3E%272020-08-29+00:00:00%27+AND+end_time%3C%272020-09-30+00:00:00%27+AND+data_item_id+LIKE+%27solo_L2_rpw-lfr-surv-asm%25%27'

 * @author jbf
 */
public class EsdcRecordSource extends AbstractHapiRecordSource {

    String id;
    
    public EsdcRecordSource( String id ) {
        this.id=id;
    }
    
    @Override
    public boolean hasGranuleIterator() {
        return true;
    }

    private static class EsdcGranuleIterator implements Iterator<int[]> {

        private final Iterator<String> iter;
        
        private EsdcGranuleIterator( Iterator<String> iter ) {
            this.iter= iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext(); // TODO: I suspect the first record must be read since it is a header.
        }

        @Override
        public int[] next() {
            try {
                String line= iter.next();
                String[] ss= SourceUtil.stringSplit(line);
                if ( ss[0].startsWith("begin") && iter.hasNext() ) { 
                    line= iter.next();
                    ss= SourceUtil.stringSplit(line);
                }
                return TimeUtil.parseISO8601TimeRange( ss[0]+"/"+ ss[1] );
            } catch (ParseException ex) {
                throw new RuntimeException(ex);
            }
        }
        
    }
    
    @Override
    public Iterator<int[]> getGranuleIterator(int[] start, int[] stop) {
        
        // https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery
        //   &LANG=ADQL&FORMAT=json
        //   &QUERY=SELECT+filename,+filepath+FROM+v_sc_data_item
        //    +WHERE+((instrument=%27MAG%27)+OR+(instrument=%27EPD%27))
        //    +AND+begin_time%3E%272020-08-29+00:00:28%27
        // https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=json&QUERY=SELECT+filename,+filepath,file_format+FROM+v_sc_data_item&begin_time%3E%272020-08-29+00:00:28%27&end_time%3E%272020-09-06+00:00:28%27&file_format=%27CDF%27
        String begin= TimeUtil.formatIso8601Time(start);
        String end=  TimeUtil.formatIso8601Time(stop);
        String url= "https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=csv&QUERY=SELECT+begin_time,end_time,filename,+filepath+FROM+v_sc_data_item+WHERE+begin_time%3E%27"+begin+"%27+AND+end_time%3C%27"+end+"%27+AND+data_item_id+LIKE+%27"+id+"%25%27";
        
        try {
            Iterator<String> iter= org.hapiserver.source.SourceUtil.getFileLines(new URL(url));
            return new EsdcGranuleIterator(iter);
            
        } catch (MalformedURLException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        
        
    }

    
    @Override
    public boolean hasParamSubsetIterator() {
        return true;
    }

    @Override
    public Iterator<HapiRecord> getIterator(int[] start, int[] stop, String[] params) {
        throw new IllegalArgumentException("not yet implemented");
    }
    
    
    
    public static void main( String[] args ) throws ParseException {
        //String id= "solo_L2_rpw-lfr-surv-asm";
        //String id= "solo_L2_mag-srf";
        String id= "solo_L2_swa-eas-pad-psd";
        EsdcRecordSource rs= new EsdcRecordSource(id);
        System.err.println( rs.hasGranuleIterator() );
        Iterator<int[]> iter= rs.getGranuleIterator( TimeUtil.parseISO8601Time("2020-08-29T00:00:00"), TimeUtil.parseISO8601Time("2020-09-30T00:00:00") );
        while ( iter.hasNext() ) {
            int[] i= iter.next();
            System.err.println(TimeUtil.formatIso8601TimeRange(i) );
        }
    }
    
    
}
