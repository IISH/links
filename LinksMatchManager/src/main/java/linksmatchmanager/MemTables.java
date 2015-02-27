package linksmatchmanager;

import java.sql.Connection;


/**
 * @author Fons Laan
 *
 * FL-27-Feb-2015
 * FL-27-Feb-2015 Latest change
 *
 * Functions to copy the Levenshtein tables into MySQL MEMORY Engine tables.
 * This inproves the speed considerably.
 */

public class MemTables
{
    boolean debug;
    Connection dbconPrematch;


    void memtables_create( long threadId, long max_heap_table_size, String table_firstname_src, String table_familyname_src, String name_postfix )
    {
        System.out.println( "Thread id "+ threadId + "; memtables_create()" );

        try
        {
        String query = "SET max_heap_table_size = " + max_heap_table_size;
        dbconPrematch.createStatement().execute( query );

        String table_firstname_dst  = "`" + table_firstname_src  + name_postfix + "`";
        String table_familyname_dst = "`" + table_familyname_src + name_postfix + "`";

        memtable_ls_name( threadId, table_firstname_src, table_firstname_dst );

        memtable_ls_name( threadId, table_familyname_src, table_familyname_dst );
        }
        catch( Exception ex ) { System.out.println( "Exception in memtables_create(): " + ex.getMessage() ); }
    } // memtables_create


    void memtables_drop( long threadId, String table_firstname_src, String table_familyname_src, String name_postfix )
    {
        System.out.println( "Thread id "+ threadId + "; memtables_drop()" );

        try
        {
        String table_firstname_dst  = "`" + table_firstname_src  + name_postfix + "`";
        String table_familyname_dst = "`" + table_familyname_src + name_postfix + "`";

        String query = "DROP TABLE " + table_firstname_dst;
        dbconPrematch.createStatement().execute( query );

        query = "DROP TABLE " + table_familyname_dst;
        dbconPrematch.createStatement().execute( query );
        }
        catch( Exception ex ) { System.out.println( "Exception in memtables_drop(): " + ex.getMessage() ); }
    } // memtables_drop


    private void memtable_ls_name( long threadId, String src_table, String dst_table )
    {
        System.out.println( "Thread id "+ threadId + "; memtable_ls_name() copying " + src_table + " -> " + dst_table );

        try
        {
        String[] name_queries =
        {
        "DROP TABLE IF EXISTS " + dst_table,

        "CREATE TABLE " + dst_table
        + " ( "
        + " `id` int(10) unsigned NOT NULL AUTO_INCREMENT,"
        + "  `name_str_1` varchar(100) COLLATE utf8_bin DEFAULT NULL,"
        + "  `name_str_2` varchar(100) COLLATE utf8_bin DEFAULT NULL,"
        + "  `length_1` mediumint(8) unsigned DEFAULT NULL,"
        + "  `length_2` mediumint(8) unsigned DEFAULT NULL,"
        + "  `name_int_1` int(11) DEFAULT NULL,"
        + "  `name_int_2` int(11) DEFAULT NULL,"
        + "  `value` tinyint(3) unsigned DEFAULT NULL,"
        + "  PRIMARY KEY (`id`),"
        + "  KEY `value` (`value`),"
        + "  KEY `length_1` (`length_1`),"
        + "  KEY `length_2` (`length_2`),"
        + "  KEY `name_1` (`name_str_1`),"
        + "  KEY `name_2` (`name_str_2`),"
        + "  KEY `n_int_1` (`name_int_1`)"
        + " )"
        + " ENGINE = MEMORY DEFAULT CHARSET = utf8 COLLATE = utf8_bin",

        "ALTER TABLE " + dst_table + " DISABLE KEYS",

        "INSERT INTO " + dst_table + " SELECT * FROM " + src_table,

        "ALTER TABLE " + dst_table + " ENABLE KEYS"
        };

        for( String query : name_queries ) { dbconPrematch.createStatement().execute( query ); }
        }
        catch( Exception ex ) { System.out.println( "Exception in memtable_ls_name(): " + ex.getMessage() ); }
    } // memtable_ls_name
}

// [eof]
