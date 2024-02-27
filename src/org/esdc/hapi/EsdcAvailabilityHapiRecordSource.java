
package org.esdc.hapi;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.Iterator;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.hapiserver.AbstractHapiRecord;
import org.hapiserver.AbstractHapiRecordSource;
import org.hapiserver.HapiRecord;
import org.hapiserver.TimeUtil;
import org.hapiserver.source.SourceUtil;

/**
 *
 * @author jbf
 */
public class EsdcAvailabilityHapiRecordSource extends AbstractHapiRecordSource {

    private String id;
    
    public EsdcAvailabilityHapiRecordSource( String availId, JSONObject info ) {
        this.id= availId.substring("availability/".length());
    }
    
    @Override
    public boolean hasGranuleIterator() {
        return false;
    }

    @Override
    public boolean hasParamSubsetIterator() {
        return false;
    }

    private static class SubsetIterator implements Iterator {

        String next;
        Iterator<String> iter;
        String contains;
        
        public SubsetIterator( Iterator<String> iter, String contains ) {
            this.contains= contains+"_";
            this.iter= iter;
            if ( iter.hasNext() ) {
                this.next= iter.next();
                while ( this.next!=null && !this.next.contains(this.contains) ) {
                    this.next= this.iter.next();
                } 
            } else {
                this.next= null;
            }
        }
        
        @Override
        public boolean hasNext() {
            return next!=null;
        }

        @Override
        public Object next() {
            String result= this.next;
            this.next= iter.next();
            while ( this.next!=null && !this.next.contains(this.contains) ) {
                this.next= this.iter.next();
            }
            return result;
        }
        
    }
    
    @Override
    public Iterator<HapiRecord> getIterator(int[] start, int[] stop) {

        // https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=json&QUERY=SELECT+filename,+filepath,file_format+FROM+v_sc_data_item&begin_time%3E%272020-08-29+00:00:28%27&end_time%3E%272020-09-06+00:00:28%27&file_format=%27CDF%27
        String begin= TimeUtil.formatIso8601Time(start);
        String end=  TimeUtil.formatIso8601Time(stop);                                                                                                                                                                                                                 // "+id+"%25%27+ORDER+BY+begin_time+ASC";        
        String url= "https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=csv&QUERY=SELECT+begin_time,end_time,filename,+filepath+FROM+v_sc_data_item+WHERE+end_time%3E%27"+begin+"%27+AND+begin_time%3C%27"+end+"%27+AND+data_item_id+LIKE+%27"+id+"%25%27+ORDER+BY+begin_time+ASC";
                
        try {
            Iterator<String> iter= org.hapiserver.source.SourceUtil.getFileLines(new URL(url));
            Iterator<String> subsetIterator= new SubsetIterator(iter,this.id);
            return new HapiRecordIterator(subsetIterator);
        
        } catch (MalformedURLException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } 
    
    
    }
    
    public class HapiRecordIterator implements Iterator {

        Iterator<String> iter;
        
        public HapiRecordIterator( Iterator<String> iter ) {
            this.iter= iter;
        }
        
        @Override
        public boolean hasNext() {
            return this.iter.hasNext();
        }

        @Override
        public HapiRecord next() {
            String rec= this.iter.next();
            String[] fields= SourceUtil.stringSplit(rec);
                
            return new AbstractHapiRecord() {
                @Override
                public int length() {
                    return 3;
                }

                @Override
                public String getIsoTime(int i) {
                    String f= fields[i];
                    int n1= f.length()-1;
                    if ( f.charAt(0)=='"' && f.charAt(n1)=='"' ) {
                        return f.substring(1,n1);
                    } else {
                        if ( f.length()==21 ) {
                            return f + "00Z";
                        } else {
                            return f;
                        }
                    }
                }

                @Override
                public int getInteger(int i) {
                    return Integer.parseInt(fields[i]); 
                }

                @Override
                public String getString(int i) {
                    return fields[i];
                }

            };

        }
        
    }
    
    public static void main( String[] args ) throws ParseException, IOException, JSONException {
        //String id= "solo_L2_rpw-lfr-surv-asm";
        //String id= "solo_L2_mag-srf";
        //String id= "solo_L2_swa-eas-pad-psd";
        String id= "availability/solo_L2_mag-rtn-normal";

        //String t= "2023-09-01/2023-10-01";
        //String t= "2023-10-31T00:00Z/2023-11-01T00:00Z";
        String t= "2023-01-01T00:00Z/2023-01-03T00:00Z";
        
        int[] timeRange= TimeUtil.parseISO8601TimeRange(t);
        
        EsdcAvailabilityHapiRecordSource rs= new EsdcAvailabilityHapiRecordSource(id,null);
        
        Iterator<HapiRecord> iter2= rs.getIterator( TimeUtil.getStartTime(timeRange), TimeUtil.getStopTime(timeRange) );
        //Iterator<HapiRecord> iter2= rs.getIterator( TimeUtil.getStartTime(timeRange), TimeUtil.getStopTime(timeRange), new String[] { "Epoch","SWA_EAS_PAD_PSD_Data" } );
        
        while ( iter2.hasNext() ) {
            System.err.println( iter2.next().getIsoTime(0) );
        }
    }

    
}
