package modulemain;

import java.io.PrintStream;

import java.sql.Connection;
import java.sql.DriverManager;
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
 * FL-16-Oct-2014 Created
 */
public class LinksCleaned extends Thread
{
    private Options opts;
    private PrintLogger plog;
    private int sourceIdGui;
    private int[] sourceList;               // available sources in db

    private String ref_url = "";            // reference db access
    private String ref_user = "";
    private String ref_pass = "";
    private String ref_db = "";

    private String url = "";                // links db's access
    private String user = "";
    private String pass = "";

    private JTextField outputLine;
    private JTextArea  outputArea;

    private ManagerGui mg;

    private MySqlConnector dbconOriginal;       // original data from A2A


    /**
     * Constructor
     *
     * @param outputLine
     * @param outputArea
     * @param opts
     * @param mg
     */
    public LinksCleaned
    (
            Options opts,
            JTextField outputLine,
            JTextArea outputArea,
            ManagerGui mg
    )
    {
        this.opts = opts;

        this.plog = opts.getLogger();
        this.sourceIdGui = opts.getSourceId();

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
        System.out.println( timestamp + "  LinksCleaned()" );

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
        try {
            plog.show( "Links Match Manager 2.0" );
            plog.show( "LinksCleaned/run()" );
            int ncores = Runtime.getRuntime().availableProcessors();
            plog.show( "Available cores: " + ncores );
            plog.show( "source_id from GUI: " + sourceIdGui );
        }
        catch( Exception ex ) { System.out.println( ex.getMessage() ); }

        try { dbconOriginal = new MySqlConnector( url, "links_original", user, pass ); }
        catch( Exception ex ) { System.out.println( ex.getMessage() ); }

        int[] sourceListAvail = getOrigSourceIds();               // get source ids from links_original.registration_o
        sourceList = createSourceList( sourceIdGui, sourceListAvail );

        String s = "LinksCleaned: Available source Ids: ";
        for( int i : sourceListAvail ) { s = s + i + " "; }
        showMessage( s, false, true );

        LinksCleanedThread linksCleanedThread = new LinksCleanedThread(
             opts,
             outputLine,
             outputArea,
             mg );

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
     * Read distinct source ids from links_original.registration_o
     * @return
     */
    private int[] createSourceList( int sourceIdGui, int[] sourceListAvail )
    {
        int[] idsInt;

        if( sourceIdGui == 0 ) { idsInt = sourceListAvail; }
        else {
            idsInt = new int[ 1 ];
            idsInt[ 0 ] = sourceIdGui;
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
