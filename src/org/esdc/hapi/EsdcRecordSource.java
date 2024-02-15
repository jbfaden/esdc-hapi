
package org.esdc.hapi;

import gov.nasa.gsfc.spdf.cdfj.CDFException;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
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
 *
 * EsdcGranuleIterator will show the files for a time range, also keeping
 * track of this internally.  The getIterator method will use this information
 * to fine the CDF files.
 * 
 * "solo_L1_swa-pas-mom_20201020"
 * "solo_L2_mag-rtn-normal-1-minute_20230121" 
 * @author jbf
 */
public class EsdcRecordSource extends AbstractHapiRecordSource {

    private static Logger logger= Logger.getLogger("hapi.esdc");
    
    String id;
    
    private Map<String,String> files = new HashMap<>();
        
    JSONObject info;
    String root;
    
    public EsdcRecordSource( String id, JSONObject info ) {
        logger.entering("EsdcRecordSource","constructor");
        this.id= id;
        this.info= info;
        logger.exiting("EsdcRecordSource","constructor");
    }

    private File getCdfFile( String filename ) throws IOException {
        logger.entering("EsdcRecordSource","getCdfFile",filename);
        boolean local= false;
        if ( local ) {
            return  new File(filename);
        } else {
            int i= filename.lastIndexOf("/");
            String ff= filename.substring(i+1);
            URL url = new URL( "https://soar.esac.esa.int/soar-sl-tap/data"
                + "?retrieval_type=PRODUCT"
                + "&QUERY=SELECT+filepath,filename"
                + "+FROM+soar.v_sc_repository_file"
                + "+WHERE+filename=%27"+ff + "%27" );

            String u= System.getProperty("user.name"); // getProcessId("000");
            File p= new File( "/home/tomcat/tmp/esdc/"+u+"/" );

            if ( !p.exists() ) {
                if ( !p.mkdirs() ) {
                    logger.warning("fail to make download area");
                }
            }

            File f= new File( p, ff );
            if ( f.exists() ) {
                //long ageMillis= System.currentTimeMillis()-f.lastModified();
                //if ( ageMillis<00000 ) {
                    logger.exiting("EsdcRecordSource","getCdfFile",filename);
                    return f;
                //}
            }
            
            logger.exiting("EsdcRecordSource","getCdfFile",filename);
            File file= SourceUtil.downloadFile( url, f );
            return file;
                
        }
    }
    
    
    @Override
    public boolean hasGranuleIterator() {
        return true;
    }

    private class EsdcGranuleIterator implements Iterator<int[]> {

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
            logger.entering("EsdcRecordSource.EsdcGranuleIterator","next");
            try {
                String line= iter.next();
                String[] ss= SourceUtil.stringSplit(line);
                if ( ss[0].startsWith("begin") && iter.hasNext() ) { 
                    line= iter.next();
                    ss= SourceUtil.stringSplit(line);
                }
                String timeRange=  ss[0]+"/"+ ss[1];
                files.put( timeRange, ss[3] + "/" + ss[2] );
                logger.exiting("EsdcRecordSource.EsdcGranuleIterator","next");
                return TimeUtil.parseISO8601TimeRange( timeRange );
            } catch (ParseException ex) {
                throw new RuntimeException(ex);
            }
        }
        
    }
    
    @Override
    public Iterator<int[]> getGranuleIterator(int[] start, int[] stop) {
        logger.entering("EsdcRecordSource","getGranuleIterator");
        // https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery
        //   &LANG=ADQL&FORMAT=json
        //   &QUERY=SELECT+filename,+filepath+FROM+v_sc_data_item
        //    +WHERE+((instrument=%27MAG%27)+OR+(instrument=%27EPD%27))
        //    +AND+begin_time%3E%272020-08-29+00:00:28%27
        // https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=json&QUERY=SELECT+filename,+filepath,file_format+FROM+v_sc_data_item&begin_time%3E%272020-08-29+00:00:28%27&end_time%3E%272020-09-06+00:00:28%27&file_format=%27CDF%27
        String begin= TimeUtil.formatIso8601Time(start);
        String end=  TimeUtil.formatIso8601Time(stop);
        String url= "https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=csv&QUERY=SELECT+begin_time,end_time,filename,+filepath+FROM+v_sc_data_item+WHERE+end_time%3E%27"+begin+"%27+AND+begin_time%3C%27"+end+"%27+AND+data_item_id+LIKE+%27"+id+"%25%27+ORDER+BY+begin_time+ASC";
        
        try {
            Iterator<String> iter= org.hapiserver.source.SourceUtil.getFileLines(new URL(url));
            EsdcGranuleIterator granuleIter = new EsdcGranuleIterator(iter);
            logger.exiting("EsdcRecordSource","getGranuleIterator");
            return granuleIter;
            
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

    /**
     * return the File used as the sample file for the dataset.  The EsdcRecordSource can be called
     * with null used for the JSONObject, and then this can be called:
     * <code>
     * EsdcRecordSource recsrc= new EsdcRecordSource("solo_L2_epd-ept-south-rates",null);
     * System.err.println( recsrc.getSampleCdfFile() );
     * </code>
     * @return
     * @throws IOException 
     */
    public File getSampleCdfFile( ) throws IOException {
        try {
            String[] ss= EsdcAvailabilityInfoSource.getExtent(id);
            int[] start= TimeUtil.parseISO8601Time(ss[2]);
            int[] stop= TimeUtil.parseISO8601Time(ss[3]);
            Iterator<int[]> it= this.getGranuleIterator( start, stop );
            while ( it.hasNext() ) {
                it.next();
            }
            String starts= String.format( "%4d-%02d-%02dT%02d:%02d:%02d.%01d", start[0], start[1], start[2], start[3], start[4], start[5], start[6] );
            String stops= String.format( "%4d-%02d-%02dT%02d:%02d:%02d.%01d", stop[0], stop[1], stop[2], stop[3], stop[4], stop[5], stop[6]);
            String key= starts + "/" + stops;
            String filename= files.get(key);
            if ( filename==null ) {
                throw new IllegalStateException("this shouldn't happen");
            }
            return getCdfFile(filename);
        } catch (ParseException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Override
    public Iterator<HapiRecord> getIterator(int[] start, int[] stop, String[] params) {
        try {
            logger.entering("EsdcRecordSource","getIterator");
            String starts= String.format( "%4d-%02d-%02dT%02d:%02d:%02d.%01d", start[0], start[1], start[2], start[3], start[4], start[5], start[6] );
            String stops= String.format( "%4d-%02d-%02dT%02d:%02d:%02d.%01d", stop[0], stop[1], stop[2], stop[3], stop[4], stop[5], stop[6]);
            String key= starts + "/" + stops;
            String filename= files.get(key);
            if ( filename==null ) {
                throw new IllegalStateException("this shouldn't happen");
            }
            
            logger.log(Level.FINE, "getting CDF file {0}...", filename);
            File cdfFile= getCdfFile(filename);
            logger.log(Level.FINE, "got CDF file {0}.", cdfFile);
            
            CdfFileRecordIterator result= new CdfFileRecordIterator( info, start, stop, params, cdfFile );
            logger.exiting("EsdcRecordSource","getIterator");
            return result;
        
        } catch ( IOException | CDFException.ReaderError ex ) {
            throw new RuntimeException(ex);
        }
        
    }
    
    public static void main( String[] args ) throws ParseException, IOException, JSONException, CDFException.ReaderError {
        //String id= "solo_L2_rpw-lfr-surv-asm";
        //String id= "solo_L2_mag-srf";
        //String id= "solo_L2_swa-eas-pad-psd";
        String id= "solo_L2_mag-rtn-normal";
        String info= EsdcInfoSource.getInfo(id);
        EsdcRecordSource rs= new EsdcRecordSource(id,new JSONObject(info) );
        System.err.println( rs.hasGranuleIterator() );
        Iterator<int[]> iter= rs.getGranuleIterator( TimeUtil.parseISO8601Time("2023-08-29T00:00:00"), TimeUtil.parseISO8601Time("2023-09-05T00:00:00") );
        String t=null;
        int c=0;
        while ( iter.hasNext() ) {
            int[] i= iter.next();
            System.err.println(TimeUtil.formatIso8601TimeRange(i) );
            //rs.getIterator( i, i, args)
            if ( c==1 ) {
                t= TimeUtil.formatIso8601TimeRange(i);
            }
            c++;
        }
        if ( t==null ) {
            throw new IllegalStateException("Didn't find time");
        }
        int[] timeRange= TimeUtil.parseISO8601TimeRange(t);
        Iterator<HapiRecord> iter2= rs.getIterator( TimeUtil.getStartTime(timeRange), TimeUtil.getStopTime(timeRange), new String[] { "EPOCH","B_RTN" } );
        //Iterator<HapiRecord> iter2= rs.getIterator( TimeUtil.getStartTime(timeRange), TimeUtil.getStopTime(timeRange), new String[] { "Epoch","SWA_EAS_PAD_PSD_Data" } );
        
        while ( iter2.hasNext() ) {
            HapiRecord rec= iter2.next();
            System.err.println( rec.getIsoTime(0) );
        }
    }
    
    
}
