package modulemain;

import java.io.PrintStream;

import java.sql.ResultSet;

import java.util.ArrayList;

import javax.swing.*;

import connectors.MySqlConnector;
import dataset.Options;
import general.PrintLogger;
import linksmanager.ManagerGui;


/**
 * KNAW IISH - International Institute of Social History
 * @author Fons Laan
 *
 * FL-24-Nov-2014 Created
 * FL-13-May-2015
 */
public class LinksClean extends Thread
{
    private Options opts;
    private PrintLogger plog;
    private String sourceIdsGui;
    private int[] sourceList;                // available sources in db

    private String ref_url  = "";            // reference db access
    private String ref_user = "";
    private String ref_pass = "";
    private String ref_db = "";

    private String url  = "";                // links db's access
    private String user = "";
    private String pass = "";

    private JTextField outputLine;
    private JTextArea  outputArea;

    private ManagerGui mg;

    private MySqlConnector dbconOriginal;   // links_original


    /**
     * Constructor
     *
     * @param outputLine
     * @param outputArea
     * @param opts
     * @param mg
     */
    public LinksClean
    (
            Options opts,
            JTextField outputLine,
            JTextArea outputArea,
            ManagerGui mg
    )
    {
        this.opts = opts;

        this.plog = opts.getLogger();
        this.sourceIdsGui = opts.getSourceIds();

        this.ref_url  = opts.getDb_ref_url();
        this.ref_user = opts.getDb_ref_user();
        this.ref_pass = opts.getDb_ref_pass();
        this.ref_db   = opts.getDb_ref_db();

        this.url  = opts.getDb_url();
        this.user = opts.getDb_user();
        this.pass = opts.getDb_pass();

        this.outputLine = outputLine;
        this.outputArea = outputArea;

        this.mg = mg;

        String timestamp = LinksSpecific.getTimeStamp2( "HH:mm:ss" );
        System.out.println( timestamp + "  LinksClean()" );

        System.out.println( "mysql_hsnref_hosturl:\t"  + ref_url );
        System.out.println( "mysql_hsnref_username:\t" + ref_user );
        System.out.println( "mysql_hsnref_password:\t" + ref_pass );
        System.out.println( "mysql_hsnref_dbname:\t"   + ref_db );
    }


    @Override
    /**
     * run
     */
    public void run()
    {
        long threadId = Thread.currentThread().getId();

        try
        {
            String msg = String.format( "Thread id %02d; Links Data Manager 2.0", threadId );
            plog.show( msg );

            msg = String.format( "Thread id %02d; LinksClean/run()", threadId );
            plog.show( msg );

            int ncores = Runtime.getRuntime().availableProcessors();
            msg = String.format( "Thread id %02d; Available cores: %d", threadId, ncores );
            plog.show( msg );

            msg = String.format( "Thread id %02d; sourceIds from GUI: %s", threadId, sourceIdsGui );
            plog.show( msg );
        }
        catch( Exception ex ) { System.out.println( ex.getMessage() ); }

        try { dbconOriginal = new MySqlConnector( url, "links_original", user, pass ); }
        catch( Exception ex ) { System.out.println( ex.getMessage() ); }

        int[] sourceListAvail = getOrigSourceIds();               // get source ids from links_original.registration_o
        sourceList = createSourceList( sourceIdsGui, sourceListAvail );

        String s = "LinksClean: Available source Ids: ";
        for( int i : sourceListAvail ) { s = s + i + " "; }

        String msg = String.format( "Thread id %02d; %s", threadId, s );
        showMessage( msg, false, true );

        try { plog.show( "" ); } catch( Exception ex ) { System.out.println( ex.getMessage() ); }

        LinksCleanMain linksCleanedThread = new LinksCleanMain(
             opts,
             outputLine,
             outputArea,
             mg
        );

        linksCleanedThread.start();
    }


    /**
     * Read distinct source ids from links_original.registration_o
     * @return
     */
    private int[] getOrigSourceIds()
    {
        ArrayList< String > ids = new ArrayList();
        String query = "SELECT DISTINCT id_source FROM registration_o ORDER BY id_source;";
        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( query );
            int count = 0;
            while( rs.next() ) {
                count += 1;
                String id = rs.getString( "id_source" );
                if( id == null || id.isEmpty() ) { break; }
                else {
                    //System.out.printf( "id: %s\n", id );
                    ids.add(id);
                }
            }
            if( count == 0 ) { showMessage( "Empty links_original ?", false , true); }
        }
        catch( Exception ex ) {
            if( ex.getMessage() != "After end of result set" ) {
                System.out.printf("'%s'\n", ex.getMessage());
                ex.printStackTrace( new PrintStream( System.out ) );
            }
        }
        //System.out.println( ids );

        int[] idsInt = new int[ ids.size() ];
        int i = 0;
        for( String id : ids ) {
            //System.out.println( id );
            idsInt[ i ] = Integer.parseInt(id);
            i += 1;
        }

        return idsInt;
    } // getOrigSourceIds


    /**
     * Get source ids from GUI or links_original.registration_o
     * @return
     */
    private int[] createSourceList( String sourceIdsGui, int[] sourceListAvail )
    {
        int[] idsInt;

        if( sourceIdsGui.isEmpty() )
        { idsInt = sourceListAvail; }           // use all Ids from links_original.registration_o
        else
        {
            String idsStr[] = sourceIdsGui.split( " " );

            if( idsStr.length == 0 )            // nothing from GUI
            { idsInt = sourceListAvail; }       // use all Ids from links_original.registration_o
            else                                // use GUI supplied Ids
            {
                idsInt = new int[ idsStr.length ];
                for( int i = 0; i < idsStr.length; i++ )
                { idsInt[ i ] = Integer.parseInt( idsStr[ i ] ); }
            }
        }

        return idsInt;
    } // createSourceList


    /**
     * @param logText
     * @param isMinOnly
     * @param newLine
     */
    private void showMessage( String logText, boolean isMinOnly, boolean newLine )
    {
        outputLine.setText( logText );

        if( !isMinOnly ) {
            String newLineToken = "";
            if( newLine ) {
                newLineToken = "\r\n";
            }

            // prefix string with timestamp
            String ts = LinksSpecific.getTimeStamp2( "HH:mm:ss" );
            outputArea.append( ts + " " );
            // System.out.printf( "%s ", ts );
            //logger.info( logText );
            try { plog.show( logText ); }
            catch( Exception ex ) {
                System.out.println( ex.getMessage() );
                ex.printStackTrace( new PrintStream( System.out ) );
            }

            outputArea.append( logText + newLineToken );
            //System.out.printf( "%s%s", logText, newLineToken );
            try { plog.show( logText ); }
            catch( Exception ex ) {
                System.out.println( ex.getMessage() );
                ex.printStackTrace( new PrintStream( System.out ) );
            }
        }
    } // showMessage

}

// [eof]
