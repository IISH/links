package dataset;



/**
 * @author Fons Laan
 *
 * FL-25-Mar-2015 Created
 * FL-15-Jan-2021 Changed
 *
 * Class to hold a record from the table links_general.scan_remarks
 */
public class Remarks
{
    private int id_scan;
    private int maintype;
    private int role;

    private String string_1;
    private String string_2;
    private String string_3;
    private String not_string1;
    private String not_string2;
    private String name_table;
    private String name_field;
    private String value;


    public void setIdScan( int id_scan ) { this.id_scan = id_scan; }

    public int getIdScan() { return id_scan; }


    public void setMaintype( int maintype ) { this.maintype = maintype; }

    public int getMaintype() { return maintype; }


    public void setRole( int role ) { this.role = role; }

    public int getRole() { return role; }


    public void setString_1( String string_1 ) { this.string_1 = string_1; }

    public String getString_1() { return string_1; }


    public void setString_2( String string_2 ) { this.string_2 = string_2; }

    public String getString_2() { return string_2; }


    public void setString_3( String string_3 ) { this.string_3 = string_3; }

    public String getString_3() { return string_3; }


    public void setNotString1( String not_string1 ) { this.not_string1 = not_string1; }

    public String getNotString1() { return not_string1; }

    public void setNotString2( String not_string2 ) { this.not_string2 = not_string2; }

    public String getNotString2() { return not_string2; }

    public void setNameTable( String name_table ) { this.name_table = name_table; }

    public String getNameTable() { return name_table; }


    public void setNameField( String name_field ) { this.name_field = name_field; }

    public String getNameField() { return name_field; }


    public void setValue( String value ) { this.value = value; }

    public String getValue() { return value; }
}
