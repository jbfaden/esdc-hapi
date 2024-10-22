package org.hapiserver.source.tap;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.hapiserver.AbstractHapiRecord;
import org.hapiserver.HapiRecord;
import org.hapiserver.source.SourceUtil;

/**
 * make CEF reader which provides records as the CEF is read in.
 *
 * @author jbf
 */
public class CefFileIterator implements Iterator<HapiRecord> {

    public Cef getCef() {
        return cef;
    }

    public CefFileIterator(ReadableByteChannel lun) throws IOException {

        for (int i = 0; i < doParse.length; i++) {
            doParse[i] = true;
        }

        this.lun = lun;

        CefReaderHeader readerh = new CefReaderHeader();
        cef = readerh.read(lun);

        readNextRecord();

    }

    private static final Logger logger = Logger.getLogger("hapi.cef");

    // *** Define the delimeters used in the CEF file
    byte eor;
    byte comma = (byte) ',';
    static final Charset CHARSET = java.nio.charset.Charset.forName("US-ASCII");

    public static final int MAX_FIELDS = 40000;
    boolean[] doParse = new boolean[MAX_FIELDS];

    private ReadableByteChannel lun;

    protected static class GlobalStruct {

        //String name;
        //List<String> entries;
        String valueType;
    }

    protected static class ParamStruct {

        String name;
        int[] sizes;
        int recType;
        int[] cefFieldPos;  // start, end inclusive
        Map<String, Object> entries = new LinkedHashMap<>();
    }

    /**
     * class representing the CEF file and data needed to parse it.
     *
     * @author jbf
     */
    private static class Cef {

        //int error = 0;
        int nglobal = 0;
        int nparam = 0;
        //int nrec = 0;
        byte eor = 10;
        //String dataUntil;
        //String fileName;
        //String fileFormatVersion;
        Map<String, ParamStruct> parameters = new LinkedHashMap<>();
        Map<String, GlobalStruct> globals = new LinkedHashMap<>();
    }

    /**
     * Reads the CEF Header, which contains metadata and information about how to parse the stream.
     *
     * @author jbf
     */
    public static class CefReaderHeader {

        private static final Logger logger = Logger.getLogger("hapi.cef");

        private enum State {

            TOP, END, DATA_READ, GLOBAL, PARAM
        }

        protected static class Record {

            String data;
        }

        protected static class KeyValue {

            String key;
            String[] val;
        }

        private static final byte EOL = 10;

        private boolean cefReadHeadRec(ReadableByteChannel c, Record record) throws IOException {

            boolean status = false;     // *** Status flag, set to 1 if complete record found
            boolean readFlag = true;     // *** used to flag multi-line records
            StringBuilder recordBuf = new StringBuilder();

            boolean eofReached = false;

            byte[] buf = new byte[1];
            ByteBuffer b1 = ByteBuffer.wrap(buf);

            //*** Keep reading unit until got complete entry or end of file ***
            while (readFlag && !eofReached) {

                StringBuilder sbuf = new StringBuilder();

                //*** read next record ***
                while (true) {
                    b1.rewind();
                    if (c.read(b1) == -1) {
                        eofReached = true;
                        break;
                    }
                    //c.read(b1);
                    sbuf.append((char) buf[0]);
                    if (buf[0] == EOL) {
                        break;
                    }
                }
                String tempRecord = sbuf.toString();

                tempRecord = tempRecord.trim();

                //*** skip comment lines ***
                if (tempRecord.length() > 0 && tempRecord.charAt(0) == '!') {
                    //; PRINT, tempRecord
                } else if (tempRecord.length() > 0 && tempRecord.charAt(tempRecord.length() - 1) == '\\') {
                    recordBuf.append(tempRecord.substring(0, tempRecord.length() - 1));
                } else {
                    recordBuf.append(tempRecord);
                    // *** if not blank then finish read  of this record ***
                    if (recordBuf.length() > 0) {
                        readFlag = false;
                        status = true;
                        record.data = recordBuf.toString();
                    } else {
                        record.data = "";
                    }
                }

            } // WHILE

            return status;
        }

        private boolean cefSplitRec(String record, KeyValue kv) {

            boolean status = false;     //*** Set default status

            // *** look for comment
            int pos = record.lastIndexOf('!');
            if (pos > -1) {
                record = record.substring(0, pos);
            }

            // *** look for key/value delimiter ***
            pos = record.indexOf('=');
            if (pos > -1) {
                status = true;

                //*** Extract the key ***
                kv.key = record.substring(0, pos).trim().toUpperCase();

                //*** Extract the value ***
                String val = record.substring(pos + 1).trim();

                //*** Split value into separate array elements
                //*** Handle quoted string elements
                if (val.charAt(0) == '"') {
                    //STRSPLIT(STRMID(val,1,STRLEN(val)-2), $
                    //    '"[ '+STRING(9B)+']*,[ '+STRING(9B)+']*"',/REGEX, /EXTRACT)
                    String tab = new String(new byte[]{9});
                    kv.val = val.substring(1, val.length() - 1).split("\"[ " + tab + "]*,[ " + tab + "]*\"");
                } else {
                    kv.val = val.split(",");
                    for (int i = 0; i < kv.val.length; i++) {
                        kv.val[i] = kv.val[i].trim();
                    }
                }
            }

            return status;
        }

        private Cef read(ReadableByteChannel c) throws IOException {

            Cef cef = new Cef();

            State state = State.TOP;

            int pdata = 0; // data index

            int eCount = 0;
            //List<String> elements = null; // for debugging
            int[] data_idx;

            GlobalStruct gStru = null;
            String gName = null;

            ParamStruct pStru = null;
            String pName = null;

            Record record = new Record();
            KeyValue kv = new KeyValue();

            //int recordNumber = 0;
            // *** Keep reading until end of header information or no more records ***
            while (state != State.DATA_READ && state != State.END) {

                //*** Try to read header record
                if (!cefReadHeadRec(c, record)) {
                    break;
                }
                //recordNumber++;

                if (record.data.length() > 2 && (record.data.startsWith("19") || record.data.startsWith("20"))) { //CFA has a bug that they don't output the "DATA_UNTIL" delimiter.
                    // C1_CP_WHI_ACTIVE__20020221_000000_20020221_050000_V120201.cef doesn't have delimiter, so trigger on a date.
                    break;
                }

                //*** Get the keyword/value(s) for this record            
                if (cefSplitRec(record.data, kv)) {

                    String key = kv.key.intern();
                    String[] value = kv.val;

                    //*** Use the parser state to check what we are looking for
                    switch (state) {

                        case TOP: {

                            //*** Use the keyword to determine the action
                            switch (key) {
                                case "START_META":
                                    //*** New global metadata item ***
                                    state = State.GLOBAL;
                                    gStru = new GlobalStruct();
                                    gName = value[0];
                                    //gStru.name = value[0];
                                    gStru.valueType = "CHAR";
                                    eCount = 0;
                                    break;
                                case "START_VARIABLE":
                                    //*** New parameter ***
                                    state = State.PARAM;
                                    pName = value[0];
                                    pStru = new ParamStruct();
                                    pStru.name = value[0];
                                    pStru.recType = 1;
                                    break;
                                case "INCLUDE":
                                    throw new IllegalArgumentException("not yet supported");
                                //if ( !value[0].equals(readFile) ) {
                                //   param = cef_read( findinclude(value[0]), cef );
                                // }
                                //*** Special CEF defined items at the top level ***
                                case "DATA_UNTIL":
                                    //*** Start of data ***
                                    state = State.DATA_READ;
                                    //cef.dataUntil = value[0];
                                    break;
                                //cef.fileName = value[0];
                                case "FILE_NAME":
                                    break;
                                //cef.fileFormatVersion = value[0];
                                case "FILE_FORMAT_VERSION":
                                    break;
                                case "END_OF_RECORD_MARKER":
                                    cef.eor = (byte) value[0].charAt(0);
                                    break;
                                default:
                                    throw new IllegalArgumentException("Unsupported key " + key);
                            }

                        }
                        break;

                        case GLOBAL: {        //*** Global metadata handling

                            if (value.length > 1) {
                                throw new IllegalArgumentException("Global entry not allowed multiple values per entry : " + gName);
                            }

                            switch (key) {
                                case "END_META":
                                    state = State.TOP;
                                    if (!kv.val[0].equals(gName)) {
                                        throw new IllegalArgumentException("END_VARIABLE expected " + gName + "  got " + kv.val[0]);
                                    }
                                    //gStru.entries = elements;
                                    cef.nglobal = cef.nglobal + 1;
                                    if (gStru.valueType.equals("CHAR")) {
                                        cef.globals.put(gName, gStru);
                                    } else {
                                        cef.globals.put(gName, gStru);
                                    }
                                    break;
                                case "VALUE_TYPE":
                                    //*** WARNING: In theory CEF allows a different VALUE_TYPE for each entry
                                    //*** this is a 'feature' from CDF but I can't think of a situation where
                                    //*** it is useful. This feature is not currently supported by this
                                    //*** software and so we just assign a type based on the last specification
                                    //*** of the VALUE_TYPE.

                                    gStru.valueType = value[0];
                                    break;
                                case "ENTRY":
                                    //*** if this is the second entry then must be multi entry global ***
                                    if (eCount == 0) {
                                        //elements = new ArrayList();
                                    }
                                    //elements.add(value[0]);
                                    eCount = eCount + 1;
                                    break;
                                default:
                                    throw new IllegalArgumentException("Unsupported global key " + key);
                            }
                        }
                        break;

                        case PARAM: //*** Parameter description handling
                        {
                            if (key.equals("END_VARIABLE")) {
                                //*** Set some defaults if not provided in the file
                                int[] sizes = pStru.sizes;
                                if (sizes == null) {
                                    pStru.sizes = new int[]{1};
                                }

                                if (pStru.recType == 0) {
                                    data_idx = new int[]{-1, -1};
                                } else {
                                    sizes = pStru.sizes;
                                    int n = pStru.sizes[0];
                                    for (int i = 1; i < sizes.length; i++) {
                                        n = n * sizes[i];
                                    }
                                    data_idx = new int[]{pdata, pdata + n - 1};
                                    pdata = pdata + n;
                                }
                                pStru.cefFieldPos = data_idx;
                                //*** Change parser state
                                state = State.TOP;
                                //*** Check this is the end of the correct parameter!
                                if (!value[0].equals(pStru.name)) {
                                    throw new IllegalArgumentException("END_VARIABLE expected " + pName + "  got " + value[0]);
                                }

                                //*** Update the number of parameters
                                cef.nparam = cef.nparam + 1;

                                cef.parameters.put(pName, pStru);

                            } else {

                                switch (key) {
                                    case "DATA":
                                        pStru.entries.put(key, value);
                                        pStru.recType = 0;   //*** Flag non-record varying data
                                        //********************************
                                        //;At the moment we just add non-record varying data as string array
                                        //;we should really check SIZES and VALUE_TYPE and reform and retype
                                        //;data as we do for the real data fields. Something for the next release?
                                        //********************************
                                        break;
                                    case "SIZES":
                                        logger.log(Level.FINER, "{0}={1}", new Object[]{key, Arrays.toString(value)});
                                        if (value.length > 1) {
                                            String[] rev = new String[value.length];
                                            for (int k = 0; k < value.length; k++) {
                                                rev[k] = value[value.length - k - 1];
                                            }
                                            value = rev;
                                        }
                                        pStru.entries.put(key, value);
                                        int[] isizes = new int[value.length];
                                        for (int i = 0; i < value.length; i++) {
                                            isizes[i] = Integer.parseInt(value[i]);
                                        }
                                        pStru.sizes = isizes;
                                        break;
                                    default:
                                        pStru.entries.put(key, value[0]);
                                        break;
                                }

                            }
                        } // case PARAM
                        break;
                        default: {
                            logger.warning("bad state, check code");
                        }
                        break;
                    } // switch
                } else {
                    throw new IllegalArgumentException("Bad record?  " + record.data);
                }
            } // while

            //*** Return the result
            return cef;
        }
    }

    public void skipParse(int i) {
        doParse[i] = false;
    }

    public void doParse(int i) {
        doParse[i] = true;
    }

    private int countFields(ByteBuffer work_buffer) {
        int fields = 1;
        for (int k = 0;; k++) {

            if (work_buffer.get(k) == comma) {
                fields++;
            } else if (work_buffer.get(k) == eor) {
                break;
            }
        }
        return fields;
    }

    /**
     * returns the position of the last end-of-record, or -1 if one is not found.
     *
     * @param work_buffer
     * @return the position of the last end-of-record, or -1 if one is not found.
     */
    private int getLastEor(ByteBuffer work_buffer) {
        ByteBuffer scanBuffer = work_buffer.slice();
        int pos_eor;
        for (pos_eor = scanBuffer.limit() - 1; pos_eor >= 0; pos_eor--) {
            if (scanBuffer.get(pos_eor) == eor) {
                break;
            }
        }
        return pos_eor;
    }

    private HapiRecord parseRecord(ByteBuffer record) {
        String s = StandardCharsets.UTF_8.decode(record).toString();
        logger.log(Level.FINER, "Record String: {0}", s);

        // Because of non-varying fields, this may be shorter than the number of fields.
        String[] f1s = SourceUtil.stringSplit(s);
        String[] fields;
        
        final List<List<Integer>> columnIndices = new ArrayList<>();
        final List<String> ffields= new ArrayList<>();
        final Map<Integer,String[]> vfields= new HashMap<>();
        
        Iterator<String> params = cef.parameters.keySet().iterator();
        int if1s = 0; // index into string array which contains physical records split up.
        for (int i = 0; i < cef.nparam; i++) {
            String key = params.next();
            ParamStruct p = cef.parameters.get(key);
            int index= p.cefFieldPos[0];
            if (index==-1 ) { // Non-record-varying
                columnIndices.add(Collections.singletonList(-i));
                String[] nonRecordVaryingValues= (String[])p.entries.get("DATA");
                vfields.put( -i, nonRecordVaryingValues );
            } else {
                int vectorLength;
                vectorLength = p.sizes[0];
                for ( int j=1; j<p.sizes.length; j++ ) {
                    vectorLength*= p.sizes[j];
                }
            
                List<Integer> componentIndices = new ArrayList<>();
                for (int iComponent = 0; iComponent < vectorLength; iComponent++) {
                    componentIndices.add(index+iComponent);
                    ffields.add(f1s[if1s++]);
                }
                columnIndices.add(componentIndices);
            }
        }
        
        fields= ffields.toArray( new String[ffields.size()] );

        // columnIndices maps from parameter number to CEF file column(s).  If it is
        // -1, it means the data is constant, and idx will refer to the column in the CEF file.
        return new AbstractHapiRecord() {
            @Override
            public int length() {
                return columnIndices.size();
            }

            @Override
            public String getIsoTime(int i) {
                int idx= columnIndices.get(i).get(0);
                String field = fields[idx].trim();
                if (field.length() > 45) { //TODO: kludge for time ranges.  See https://github.com/hapi-server/server-java/issues/22
                    int is1 = field.indexOf("/");
                    if (is1 > 0) {
                        field = field.substring(0, 24);
                        if ( !field.endsWith("Z") ) field=field+"Z";
                    }
                }
                return field;
            }

            @Override
            public String getString(int i) {
                return getAsString(i);
            }

            @Override
            public int getInteger(int i) {
                return Integer.parseInt(getAsString(i));
            }

            @Override
            public int[] getIntegerArray(int i) {
                String[] stringArray = getStringArray(i);
                int[] intArray = new int[stringArray.length];
                for (int iField = 0; iField < stringArray.length; iField++) {
                    intArray[iField] = Integer.parseInt(stringArray[iField]);
                }
                return intArray;
            }

            @Override
            public double[] getDoubleArray(int i) {
                String[] stringArray = getStringArray(i);
                double[] doubleArray = new double[stringArray.length];
                for (int iField = 0; iField < stringArray.length; iField++) {
                    doubleArray[iField] = Double.parseDouble(stringArray[iField]);
                }
                return doubleArray;

            }

            @Override
            public String[] getStringArray(int i) {
                List<Integer> indices = columnIndices.get(i);
                int firstIndex= indices.get(0);
                if ( firstIndex<0 ) {
                    return vfields.get(firstIndex);
                } else {
                    String[] vector = new String[indices.size()];
                    int lastIndex= firstIndex+indices.size();
                    for (int iField = firstIndex; iField < lastIndex; iField++) {
                        vector[iField-firstIndex] = fields[iField].trim();
                    }
                    return vector;
                }
            }

            @Override
            public double getDouble(int i) {
                return Double.parseDouble(getAsString(i));
            }

            @Override
            public String getAsString(int i) {
                int idx= columnIndices.get(i).get(0);
                if ( idx<0 ) {
                    return vfields.get(idx)[0];
                } else {
                    return fields[idx].trim();
                }
            }

            @Override
            public String toString() {
                return getAsString(0) + " " + length() + " fields";
            }

        };
    }

    private void removeComments(ByteBuffer work_buffer, int work_size) {
        // remove comments by replacing them with whitespace.  When the 
        // record delimiter is not EOL, replace EOLs with whitespace.

        if (work_size != work_buffer.limit()) {
            throw new IllegalArgumentException("work_size must be the same as the limit");
        }
        byte comment = (byte) '!';
        byte eol = (byte) 10;

        int pos_comment;
        for (pos_comment = 0; pos_comment < work_size; pos_comment++) {
            byte ch = work_buffer.get(pos_comment);
            if (ch == comment) {
                int j = pos_comment;
                while (j < work_size && work_buffer.get(j) != eol) {
                    work_buffer.put(j, (byte) 32);
                    j = j + 1;
                }
                work_buffer.put(j, (byte) 32);
                pos_comment = j;
            } else if (ch == eol && eor != eol) {
                work_buffer.put(pos_comment, (byte) 32);
            } else if (ch == eor) {
                //work_buffer.put( pos_comment, comma ); //leave them in in this parser
            }
        }
    }

    private int findDelimeterPosition(ByteBuffer work_buffer) {
        ByteBuffer scanBuffer = work_buffer.slice();
        while (scanBuffer.hasRemaining()) {
            if (scanBuffer.get() == eor) {
                return scanBuffer.position() - 1;
            }
        }
        return -1;

    }

    Cef cef;
    int buffer_size = 600000;

    /**
     * usable limit in work_buffer. This is the position of the end of the last complete record
     */
//    int work_size = 0;
    byte[] work_buf = new byte[2 * buffer_size];
    ByteBuffer read_buffer = ByteBuffer.wrap(new byte[buffer_size]);
    ByteBuffer work_buffer = ByteBuffer.wrap(work_buf);
    boolean eof = false;

    // *** Set the processing state flag (1=first record, 2=subsequent records, 0 = end of file )
    int n_fields = -1;     //*** number of fields per record, -1 means we haven't counted.

    int irec = 0;

    private HapiRecord nextRecord;

    private void readNextRecord() throws IOException {

        eor = cef.eor;

        // *** Keep reading until we reach the end of the file.
        if (eof) {
            this.nextRecord = null;
            this.lun.close();

        } else {

            //    removeComments(work_buffer, work_buffer.limit() );
            int last_eor = getLastEor(work_buffer); //*** look for delimeters, EOR, comments, EOL etc
            int loop = 0;
            while (last_eor < 0) { // reload the work_buffer
                //*** read the next chunk of the file
                //*** catch errors to avoid warning message if we read past end of file
                read_buffer.rewind();
                read_buffer.limit(read_buffer.capacity());

                int read_size = lun.read(read_buffer);
                logger.log(Level.FINER, "Read {0} bytes", read_size);

                if (read_size == -1) {
                    eof = true;
                    this.nextRecord = null;
                    return;
                }

                //*** transfer this onto the end of the work buffer and update size of work buffer
                if (read_size > 0) {
                    read_buffer.flip();
                    if (work_buffer.position() > 0 && loop == 0) {
                        //There's unused data in the work buffer, so preserve it
                        work_buffer.compact();
                    }
                    work_buffer.put(read_buffer);
                    work_buffer.flip();
                }

                //removeComments(work_buffer, work_buffer.limit() );                
                last_eor = getLastEor(work_buffer);
                if (last_eor < 0) {
                    //No full record available, so prepare the work buffer to read more
                    work_buffer.position(work_buffer.limit());
                    work_buffer.limit(work_buffer.capacity());
                }
                loop++;
            }

            int delimeterPos = findDelimeterPosition(work_buffer);
            int stringLength = delimeterPos;
            ByteBuffer record = work_buffer.slice();
            record.limit(stringLength);
            nextRecord = parseRecord(record);
            logger.log(Level.FINER, "Read: {0}", nextRecord);
            if ( nextRecord.getIsoTime(0).compareTo("2100")>0 ) { // If it is appearently fill
                logger.warning("TODO: Need to deal with fill value in time");
            }

            //advance the position
            work_buffer.position(work_buffer.position() + delimeterPos + 1);

            irec = irec + 1;

        }

    }

    @Override
    public boolean hasNext() {
        return nextRecord != null;
    }

    @Override
    public HapiRecord next() {
        final HapiRecord rec = nextRecord;
        try {
            readNextRecord();
        } catch (IOException ex) {
            try {
                this.lun.close();
            } catch (IOException ex1) {
                // do nothing, the first exception will be visible.
            }
            throw new RuntimeException(ex);
        }

        return rec;

    }

    public static void main(String[] args) throws MalformedURLException, IOException {
        //URL uu= new URL( "file:/home/jbf/ct/hapi/u/larry/20220503/CEF/FGM_SPIN.cef");
//        String infileString = "C:/Users/brownle1/cef/test.cef";
//		URL uu= new URL( "file:/"+infileString );

        String dataSet;
        String startDate;
        String endDate;
        String urlString;

        if ( false ) {
            dataSet= "C1_CP_FGM_20030303";
            startDate = "2003-03-03T00:00:00Z";
            endDate = "2003-03-03T02:00:00Z";
        //                             https://csa.esac.esa.int/csa-sl-tap/data?RETRIEVAL_TYPE=product&RETRIEVAL_ACCESS=streamed&DATASET_ID=C1_CP_FGM_SPIN&START_DATE=2003-03-03T00:00Z&END_DATE=2003-03-03T02:00Z
            urlString = String.format("https://csa.esac.esa.int/csa-sl-tap/data?RETRIEVAL_TYPE=product&RETRIEVAL_ACCESS=streamed&DATASET_ID=C1_CP_FGM_SPIN&START_DATE=%s&END_DATE=%s",
                startDate, endDate);
        }
        
        if ( true ) {
            dataSet="CL_SP_WHI";
            urlString = "https://csa.esac.esa.int/csa-sl-tap/data?RETRIEVAL_TYPE=product&RETRIEVAL_ACCESS=streamed&DATASET_ID=CL_SP_WHI&START_DATE=2012-12-25T00:00:00Z&END_DATE=2012-12-26T00:00:00Z";
        }
        
        URL uu = new URL(urlString);

        logger.log(Level.FINE, "Opening connection to: {0}", uu);

        InputStream in = uu.openStream();
        ReadableByteChannel lun = Channels.newChannel(in);

//		String dataSet = "C1_CP_FGM_test";
        String outfileString = "/home/jbf/tmp/20230314/" + dataSet + "csv.gz";

        FileOutputStream fos = new FileOutputStream(outfileString);
        GZIPOutputStream gzos = new GZIPOutputStream(fos);
        PrintWriter pw = new PrintWriter(gzos);

        System.err.println("begin reading " + uu);
        long t0 = System.currentTimeMillis();

        CefFileIterator iter = new CefFileIterator(lun);
        Cef cefSample = iter.getCef();
        List<String> headerList = new ArrayList<>();
        headerList.add("Epoch");
        for (String key : cefSample.parameters.keySet()) {
            if (key.contains("time")) {
                continue;
            }
            if (key.contains("xyz")) {
                headerList.add(key.replace("xyz", "x"));
                headerList.add(key.replace("xyz", "y"));
                headerList.add(key.replace("xyz", "z"));
            } else {
                headerList.add(key);
            }
        }
        pw.println(String.join(",", headerList));
        //Iterator<HapiRecord> iter= new CefFileIterator(lun); 
        int i = 0;

        try {
            while (iter.hasNext()) {
                HapiRecord rec;
                rec = iter.next();
                i++;
                List<String> lineList = new ArrayList<>();
                lineList.add(rec.getIsoTime(0));
                for (int iField = 1; iField < headerList.size() - 1; iField++) {
                    try {
                        String s = rec.getString(iField);
                        lineList.add(s);
                    } catch (Exception e) {
                        break;
                    }
                }
                pw.println(String.join(",", lineList));
            }
//      	System.err.println("records read: "+i);
//      	System.err.println("time to read: "+ (System.currentTimeMillis()-t0) + "ms" );
        } finally {
            pw.close();
            System.err.println("Got " + i + " records");
            System.err.println("Wrote " + outfileString);
        }
    }

}
