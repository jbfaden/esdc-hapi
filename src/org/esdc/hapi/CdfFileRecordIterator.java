
package org.esdc.hapi;

import gov.nasa.gsfc.spdf.cdfj.CDFException;
import gov.nasa.gsfc.spdf.cdfj.CDFReader;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.hapiserver.HapiRecord;
import org.hapiserver.TimeUtil;

/**
 *
 * @author jbf
 */
public class CdfFileRecordIterator  implements Iterator<HapiRecord> {
        
    private static final Logger logger= Logger.getLogger("hapi.esdc");
    
    static class TimerFormatter extends Formatter {
        long t0= System.currentTimeMillis();
        String resetMessage= "ENTRY";
        
        @Override
        public String format(LogRecord record) {
            if ( record.getMessage().equals(resetMessage) ) {
                t0= record.getMillis();
            }
            String message= MessageFormat.format( record.getMessage(), record.getParameters() );
            if ( message.equals("ENTRY") || message.equals("RETURN")) {
                message= message + " " + record.getSourceClassName() + " " + record.getSourceMethodName();
            }
            return String.format( "%06d: %s\n", record.getMillis()-t0, message );
        }
        
    }
    static {
        //logger.setLevel(Level.FINER);
        //ConsoleHandler h= new ConsoleHandler();
        //h.setFormatter( new TimerFormatter() );
        //h.setLevel(Level.ALL);
        //logger.addHandler(h);
    }
    
    HapiRecord nextRecord;
    Adapter[] adapters;
    
    int index;
    int nindex;

    /**
     * one of these methods will be implemented by the adapter.
     */
    private static abstract class Adapter {
        public String adaptString( int index ) {
            return null;
        }
        public double adaptDouble( int index ) {
            return Double.NEGATIVE_INFINITY;
        }
        public int adaptInteger( int index ) {
            return Integer.MIN_VALUE;
        }
        public double[] adaptDoubleArray( int index ) {
            return null;
        }
        public int[] adaptIntegerArray( int index ) {
            return null;
        }        
        public String[] adaptStringArray( int index ) {
            return null;
        }
    }
    
    private static class IsotimeEpochAdapter extends Adapter {
        int julianDay;
        long cdfTT2000= Long.MAX_VALUE;
        
        /**
         * the time in milliseconds since year 1 for cdfEpoch.
         */
        double baseTime;
        
        /**
         * 1000000 for epoch, which is a milliseconds offset.
         */
        double baseUnitsFactor;
        String baseYYYYmmddTHH;
        
        double[] array;
        
        String format= ":%02d:%02d.%09d";
        int formatFactor= 1; // number by which to round
        
        private IsotimeEpochAdapter( double[] array, int length ) {
            this.array= array;
            double d= array[0];
            double us2000= ( d - 6.3113904E13 ) * 1000; // ms -> microseconds
            double day2000= Math.floor( us2000 / 86400000000. ); // days since 2000-01-01.
            double usDay= us2000 - day2000 * 86400000000.; // microseconds within this day.
            double ms1970= day2000 * 86400000. + 946684800000.;
            String baseDay= TimeUtil.fromMillisecondsSince1970((long)ms1970);
            baseYYYYmmddTHH= baseDay.substring(0,10)+"T00";
            baseTime= (long)(d-usDay/1000);
            switch ( length ) { // YYYY4hh7mm0HH3MM6SS9NNNNNNNNNZ
                case 24:
                    format=":%02d:%02d.%03dZ";
                    formatFactor= 1000000;
                    break;
                case 27:
                    format=":%02d:%02d.%06dZ";
                    formatFactor= 1000000;
                    break;
                case 30:
                    format=":%02d:%02d.%09dZ";
                    break;
                default:
                    throw new IllegalArgumentException("not supported");
            }
        }
        
        private String formatTime( double t ) {
            double offset= t-baseTime;  // milliseconds
            while ( offset>=3600000. ) {
                double hours= offset / 3600000.;
                baseTime = baseTime + hours * 3600000.;
                int hour= Integer.parseInt(baseYYYYmmddTHH.substring(11,13));
                baseYYYYmmddTHH= baseYYYYmmddTHH.substring(0,11)+String.format("%02d",(int)(hour+hours));
                baseYYYYmmddTHH= TimeUtil.normalizeTimeString(baseYYYYmmddTHH).substring(0,13);
                offset= t-baseTime;
            }
            int nanos= (int)( (offset*1000000) % 1000000000. );
            offset= (int)( offset / 1000 ); // now it's in seconds.  Note offset must be positive for this to work.
            int seconds= (int)(offset % 60);
            int minutes= (int)(offset / 60); // now it's in minutes
            return baseYYYYmmddTHH + String.format( format, minutes, seconds, nanos/formatFactor );
        }        
        
        @Override
        public String adaptString( int index) {
            return formatTime( array[index] );
        }
        
    }
    
    private static class DoubleDoubleAdapter extends Adapter {
        double[] array;
        
        private DoubleDoubleAdapter( double[] array ) {
            this.array= array;
        }
        
        @Override
        public double adaptDouble(int index) {
            if ( index>=this.array.length ) {
                throw new ArrayIndexOutOfBoundsException("can't find the double at position "+index);
            }
            return this.array[index];
        }
    }
    
    private static class DoubleArrayDoubleAdapter extends Adapter {
        double[][] array;
        int n; // there's a weird bit of code where the Java library is giving me double arrays containing ints.
        
        private DoubleArrayDoubleAdapter( double[][] array ) {
            this.array= array;
            if ( array.length>0 ) {
                this.n= array[0].length;
            }
        }
        
        @Override
        public double[] adaptDoubleArray(int index) {
            return this.array[index];
        }

        @Override
        public int[] adaptIntegerArray(int index) {
            int[] adapt= new int[n];
            double[] rec= this.array[index];
            for ( int i=0; i<n; i++ ) {
                adapt[i]= (int)rec[i];
            }
            return adapt;
        }
        
    }
    
    private static class DoubleFloatAdapter extends Adapter {
        float[] array;
        
        private DoubleFloatAdapter( float[] array ) {
            this.array= array;
        }
        
        @Override
        public double adaptDouble(int index) {
            return this.array[index];
        }
    }
    
    private static class IntegerLongAdapter extends Adapter {
        long[] array;
        
        private IntegerLongAdapter( long[] array ) {
            this.array= array;
        }
        
        @Override
        public int adaptInteger( int index ) {
            return (int)this.array[index];
        }
    }
    
    private static class IntegerIntegerAdapter extends Adapter {
        int[] array;
        
        private IntegerIntegerAdapter( int[] array ) {
            this.array= array;
        }
        
        @Override
        public int adaptInteger( int index ) {
            return this.array[index];
        }
    }
    
    private static class IntegerShortAdapter extends Adapter {
        short[] array;
        
        private IntegerShortAdapter( short[] array ) {
            this.array= array;
        }
        
        @Override
        public int adaptInteger( int index ) {
            return this.array[index];
        }
    }
    private static class IntegerByteAdapter extends Adapter {
        byte[] array;
        
        private IntegerByteAdapter( byte[] array ) {
            this.array= array;
        }
        
        @Override
        public int adaptInteger( int index ) {
            return this.array[index];
        }
    }
    
    private static class IntegerArrayIntegerAdapter extends Adapter {
        int[][] array;
        
        private IntegerArrayIntegerAdapter( int[][] array ) {
            this.array= array;
        }

        @Override
        public int[] adaptIntegerArray(int index) {
            return this.array[index];
        }
        
        
    }
    
    private static class IsotimeTT2000Adapter extends Adapter {
        int julianDay;
        long cdfTT2000= Long.MAX_VALUE;
        /**
         * the time in milliseconds since year 1 for cdfEpoch, or nanoseconds for tt2000.
         */
        double baseTime;
        /**
         * 1 for tt2000, 1000000 for epoch.
         */
        double baseUnitsFactor;
        String baseYYYYmmddTHH;
        
        long[] array;
        
        private IsotimeTT2000Adapter( long[] array, int width ) {
            this.array= array;
            double d= Array.getDouble(array,0);
            double us2000= new LeapSecondsConverter(false).convert(d);
            double day2000= Math.floor( us2000 / 86400000000. ); // days since 2000-01-01.
            double usDay= us2000 - day2000 * 86400000000.; // seconds within this day.
            double ms1970= day2000 * 86400000. + 946684800000.;
            String baseDay= TimeUtil.fromMillisecondsSince1970((long)ms1970);
            baseYYYYmmddTHH= baseDay.substring(0,10)+"T00";
            baseTime= (long)(d-usDay*1000);
        }
        
        private String formatTime( double t ) {
            long offset= (long)((t-baseTime));  // This must not cross a leap second, will always be in nanos
            while ( offset>=3600000000000L ) {
                double hours= offset / 3600000000000L;
                baseTime = baseTime + hours * 3600000000000L;
                int hour= Integer.parseInt(baseYYYYmmddTHH.substring(11,13));
                baseYYYYmmddTHH= baseYYYYmmddTHH.substring(0,11)+String.format("%02d",(int)(hour+hours));
                baseYYYYmmddTHH= TimeUtil.normalizeTimeString(baseYYYYmmddTHH).substring(0,13);
                offset= (long)((t-baseTime));
            }
            int nanos= (int)( (offset) % 1000000000. );
            offset= offset / 1000000000; // now it's in seconds
            int seconds= (int)(offset % 60);
            int minutes= (int)(offset / 60); // now it's in minutes
            return baseYYYYmmddTHH + String.format( ":%02d:%02d.%09dZ", minutes, seconds, nanos );        
        }        
        
        @Override
        public String adaptString(int index) {
            return formatTime( array[index] );
        }
    }
    
    /**
     * Returns the name of the integer data type, for example, 8 is type
     * 8-byte integer (a.k.a. Java long), and 33 is CDF_TT2000.
     * @param type the code for data type
     * @return a name identifying the type.
     * @see https://spdf.gsfc.nasa.gov/pub/software/cdf/doc/cdf380/cdf38ifd.pdf page 33.
     */
    public static String nameForType(int type) {
        switch (type) {
            case 1:
                return "CDF_INT1";
            case 41:
                return "CDF_BYTE";  // 1-byte signed integer
            case 2:
                return "CDF_INT2"; 
            case 4:
                return "CDF_INT4";
            case 8:
                return "CDF_INT8";
            case 11:
                return "CDF_UINT1";
            case 12:
                return "CDF_UINT2";
            case 14:
                return "CDF_UINT4";
            case 21:
                return "CDF_REAL4";
            case 44:
                return "CDF_FLOAT"; 
            case 22:
                return "CDF_REAL8";
            case 45:
                return "CDF_DOUBLE"; 
            case 31:
                return "CDF_EPOCH";
            case 32:
                return "CDF_EPOCH16";  // make of two CDF_REAL8,
            case 33:
                return "CDF_TT2000";
            case 51:
                return "CDF_CHAR";
            case 52:
                return "CDF_UCHAR";
            default:
                return "???";
        }
    }
            
    /**
     * return the processID (pid), or the fallback if the pid cannot be found.
     * @param fallback the string (null is okay) to return when the pid cannot be found.
     * @return the process id or the fallback provided by the caller.
     * //TODO: Java9 has method for accessing process ID.
     */
    public static String getProcessId(final String fallback) {
        // Note: may fail in some JVM implementations
        // therefore fallback has to be provided

        // something like '<pid>@<hostname>', at least in SUN / Oracle JVMs
        final String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        final int index = jvmName.indexOf('@');

        if (index < 1) {
            // part before '@' empty (index = 0) / '@' not found (index = -1)
            return fallback;
        }

        try {
            return Long.toString(Long.parseLong(jvmName.substring(0, index)));
        } catch (NumberFormatException e) {
            // ignore
        }
        return fallback;
    }

    /**
     * flatten 3-D array into 2-D.  Thanks, Bard!
     * @param array
     * @return 
     */
    public static double[][] flatten(double[][][] array) {
        int len1= array[0].length * array[0][0].length;
        double[][] flattenedArray = new double[array.length][len1];
        int index;
        for (int i = 0; i < array.length; i++) {
            index=0;
            for (int j = 0; j < array[i].length; j++) {
                System.arraycopy(array[i][j], 0, flattenedArray[i], index, array[i][j].length);
                index+=array[i][j].length;
            }
        }

        return flattenedArray;
    }
    
    /**
     * flatten 3-D array into 2-D.  Thanks, Bard!
     * @param array
     * @return 
     */
    public static double[][] flatten(double[][][][] array) {
        int len1= array[0].length * array[0][0].length * array[0][0][0].length;
        double[][] flattenedArray = new double[array.length][len1];
        int index;
        for (int i = 0; i < array.length; i++) {
            index=0;
            for (int j = 0; j < array[i].length; j++) {
                for (int k = 0; k < array[i][j].length; k++) {
                    System.arraycopy(array[i][j][k], 0, flattenedArray[i], index, array[i][j][k].length);
                    index+=array[i][j][k].length;
                }
            }
        }

        return flattenedArray;
    }
    
    private double[][] flattenDoubleArray( Object array ) {
        int numDimensions = 1;
        Class<?> componentType = array.getClass().getComponentType();
        while (componentType != double.class) {
            numDimensions++;
            componentType = componentType.getComponentType();
        }
        switch (numDimensions) {
            case 2:
                return (double[][])array;
            case 3:
                return flatten((double[][][])array);
            case 4:
                return flatten((double[][][][])array);
            default:
                throw new IllegalArgumentException("Not supported: rank>4");
        }
    }
    
    public CdfFileRecordIterator( JSONObject info, int[] start, int[] stop, String[] params, File tmpFile ) throws CDFException.ReaderError {
         
        try {
            adapters= new Adapter[params.length];
            
            int nrec=-1;
            
            CDFReader reader= new CDFReader(tmpFile.toString());
            for ( int i=0; i<params.length; i++ ) {
                if ( i==0 ) {
                    int length= 24;
                    try {
                        if ( info!=null ) {
                            JSONArray pp = info.getJSONArray("parameters");
                            length= pp.getJSONObject(0).getInt("length");
                        }
                    } catch ( JSONException ex ) {
                        logger.warning("There should always be a length on parameters[0]");
                    }
                    String dep0=params[0]; //TODO: Huh??? Rewrite this so that it's clear.  Need definition on "params"
                    int type= reader.getType(dep0); // 31=Epoch
                    Object o= reader.get(dep0);
                    nrec= Array.getLength(o);
                    if ( nrec>0 ) {
                        switch (type) {
                            case 31:                                
                                adapters[i]= new IsotimeEpochAdapter( (double[])o, length );
                                break;
                            case 33:
                                adapters[i]= new IsotimeTT2000Adapter( (long[])o, length );
                                break;
                            default:
                                //TODO: epoch16.
                                throw new IllegalArgumentException("type not supported for column 0 time (cdf_epoch16");
                        }
                        nindex= Array.getLength(o);
                    } else {
                        nindex=0;
                    }
                    
                } else {
                    String param= params[i];
                    int type= reader.getType(param);
                    Object o= reader.get(param);
                    if ( Array.getLength(o)!=nrec ) {
                        if ( Array.getLength(o)==1 ) {
                            // let's assume they meant for this to non-time varying.
                            Object newO= Array.newInstance( o.getClass().getComponentType(), nrec );
                            Object v1= Array.get( o, 0 );
                            for ( int irec=0; irec<nrec; irec++ ) {
                                Array.set( newO, irec, v1 );
                            }
                            o= newO;
                        } else {
                            throw new IllegalArgumentException("nrec is inconsistent!  This internal error must be fixed.");
                        }
                    }
                    String stype= nameForType(type);
                    Class c= o.getClass().getComponentType();
                    if ( !c.isArray() ) {
                        if ( c==double.class ) {
                            adapters[i]= new DoubleDoubleAdapter( (double[])o );
                        } else if ( c==float.class ) {
                            adapters[i]= new DoubleFloatAdapter( (float[])o );
                        } else if ( c==int.class ) {
                            adapters[i]= new IntegerIntegerAdapter( (int[])o );
                        } else if ( c==short.class ) {
                            adapters[i]= new IntegerShortAdapter( (short[])o );
                        } else if ( c==byte.class ) {
                            adapters[i]= new IntegerByteAdapter( (byte[])o );
                        } else if ( c==long.class ) {
                            adapters[i]= new IntegerLongAdapter( (long[])o );
                        } else if ( stype.equals("CDF_UINT2") ) {
                            adapters[i]= new IntegerIntegerAdapter( (int[])o );
                        } else if ( stype.equals("CDF_UINT1") ) {
                            adapters[i]= new IntegerShortAdapter( (short[])o );                        
                        } else {
                            throw new IllegalArgumentException("unsupported type");
                        }
                    } else {
                        c= c.getComponentType();
                        if ( c==double.class ) {
                            adapters[i]= new DoubleArrayDoubleAdapter( (double[][])o );
                        } else if ( c==int.class ) {
                            adapters[i]= new IntegerArrayIntegerAdapter( (int[][])o );
                        } else if ( c.isArray()  ) {
                            o= flattenDoubleArray(o);
                            adapters[i]= new DoubleArrayDoubleAdapter( (double[][])o );
                        } else {
                            throw new IllegalArgumentException("unsupported type");
                        }
                    }
                }
            }
            
            logger.log(Level.FINER, "calculated adapters" );
            
            index= 0;
            logger.exiting( CdfFileRecordIterator.class.getCanonicalName(), "constructor" );
            
        } catch ( CDFException.ReaderError ex ) {
            throw new RuntimeException(ex);
        }

    }

    @Override
    public boolean hasNext() {
        return index<nindex;
    }

    @Override
    public HapiRecord next() {
        final int j= index;
        String thisTime= adapters[0].adaptString(j);
        index++;
        // there are some repeated records, and HAPI does not allow this.
        while ( index<nindex && adapters[0].adaptString(index).equals(thisTime) ) {
            index++;
        }
        return new HapiRecord() {
            @Override
            public String getIsoTime(int i) {
                return adapters[i].adaptString(j);
            }

            @Override
            public String[] getIsoTimeArray(int i) {
                return null;
            }

            @Override
            public String getString(int i) {
                return adapters[i].adaptString(j);
            }

            @Override
            public String[] getStringArray(int i) {
                return null;
            }

            @Override
            public double getDouble(int i) {
                return adapters[i].adaptDouble(j);
            }

            @Override
            public double[] getDoubleArray(int i) {
                return adapters[i].adaptDoubleArray(j);
            }

            @Override
            public int getInteger(int i) {
                return adapters[i].adaptInteger(i);
            }

            @Override
            public int[] getIntegerArray(int i) {
                return adapters[i].adaptIntegerArray(j);
            }

            @Override
            public String getAsString(int i) {
                return null;
            }

            @Override
            public int length() {
                return adapters.length;
            }
        };
    }
    
}
