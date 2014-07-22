package viewmatrix;

import java.sql.*;

/**
 *
 * @author oaz
 */
public class ViewMatrix {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        String bla = "";

        try {

            String driver = "org.gjt.mm.mysql.Driver";
            String longUrl = "jdbc:mysql://" + "127.0.0.1" + "/" + "links_match" + "?dontTrackOpenResources=true";

            Class.forName(driver);
            Connection con = DriverManager.getConnection(longUrl, "oaz", "abc123");

            // test
            String use_familyname_temp = "1001";
            String use_firstname_temp = "1001";
            int firstname = 2;


            //------------------//
            char[] use_familyname = use_familyname_temp.toCharArray();
            char[] use_firstname = use_firstname_temp.toCharArray();



            ResultSet rs = con.createStatement().executeQuery(""
                    + "SELECT "
                    + "ego_familyname_value, "
                    + "ego_firstname1_value, "
                    + "ego_firstname2_value, "
                    + "ego_firstname3_value, "
                    + "ego_firstname4_value, "
                    + "mother_familyname_value, "
                    + "mother_firstname1_value, "
                    + "mother_firstname2_value, "
                    + "mother_firstname3_value, "
                    + "mother_firstname4_value, "
                    + "father_familyname_value, "
                    + "father_firstname1_value, "
                    + "father_firstname2_value, "
                    + "father_firstname3_value, "
                    + "father_firstname4_value, "
                    + "partner_familyname_value, "
                    + "partner_firstname1_value, "
                    + "partner_firstname2_value, "
                    + "partner_firstname3_value, "
                    + "partner_firstname4_value "
                    + "FROM "
                    + "view_x");


            //vars
            int[] lv = new int[80];
            int[] lv_0 = new int[80];
            int[] lv_1 = new int[80];
            int[] lv_2 = new int[80];
            int[] lv_3 = new int[80];
            int[] lv_4 = new int[80];
            int[] lv_5 = new int[80];
            int[] lv_6 = new int[80];
            int[] lv_7 = new int[80];
            int[] lv_8 = new int[80];
            int[] lv_9 = new int[80];
            int[] lv_10 = new int[80];
            int[] lv_11 = new int[80];
            int[] lv_12 = new int[80];
            int[] lv_13 = new int[80];
            int[] lv_14 = new int[80];
            int[] lv_15 = new int[80];
            int[] lv_16 = new int[80];
            int[] lv_17 = new int[80];
            int[] lv_18 = new int[80];
            int[] lv_19 = new int[80];
            int[] lv_20 = new int[80];

            for (int i = 0; i < 80; i++) {
                lv[i] = 0;
                lv_0[i] = 0;
                lv_1[i] = 0;
                lv_2[i] = 0;
                lv_3[i] = 0;
                lv_4[i] = 0;
                lv_5[i] = 0;
                lv_6[i] = 0;
                lv_7[i] = 0;
                lv_8[i] = 0;
                lv_9[i] = 0;
                lv_10[i] = 0;
                lv_11[i] = 0;
                lv_12[i] = 0;
                lv_13[i] = 0;
                lv_14[i] = 0;
                lv_15[i] = 0;
                lv_16[i] = 0;
                lv_17[i] = 0;
                lv_18[i] = 0;
                lv_19[i] = 0;
                lv_20[i] = 0;
            }


            while (rs.next()) {

                // load into
                String[] sa = new String[20];

                // fill with empty
                for (int i = 0; i < 20; i++) {
                    sa[i] = "";
                }

                // familynames
                if (use_familyname[0] == '1') { //ego
                    sa[0] = rs.getString("ego_familyname_value") != null ? rs.getString("ego_familyname_value") : "";
                }
                if (use_familyname[1] == '1') { // moeder
                    sa[5] = rs.getString("mother_familyname_value") != null ? rs.getString("mother_familyname_value") : "";
                }
                if (use_familyname[2] == '1') { // vader
                    sa[10] = rs.getString("father_familyname_value") != null ? rs.getString("father_familyname_value") : "";
                }
                if (use_familyname[3] == '1') { // partner
                    sa[15] = rs.getString("partner_familyname_value") != null ? rs.getString("partner_familyname_value") : "";
                }

                if (use_firstname[0] == '1') { // ego
                    sa[1] = rs.getString("ego_firstname1_value") != null ? rs.getString("ego_firstname1_value") : "";

                    if (firstname == 1 || firstname > 2) {
                        sa[2] = rs.getString("ego_firstname2_value") != null ? rs.getString("ego_firstname2_value") : "";
                    }
                    if (firstname == 1 || firstname > 3) {
                        sa[3] = rs.getString("ego_firstname3_value") != null ? rs.getString("ego_firstname3_value") : "";
                    }
                    if (firstname == 1 || firstname > 4) {
                        sa[4] = rs.getString("ego_firstname4_value") != null ? rs.getString("ego_firstname4_value") : "";
                    }
                }
                if (use_firstname[1] == '1') { // mother
                    sa[6] = rs.getString("mother_firstname1_value") != null ? rs.getString("mother_firstname1_value") : "";

                    if (firstname == 1 || firstname > 2) {
                        sa[7] = rs.getString("mother_firstname2_value") != null ? rs.getString("mother_firstname2_value") : "";
                    }
                    if (firstname == 1 || firstname > 3) {
                        sa[8] = rs.getString("mother_firstname3_value") != null ? rs.getString("mother_firstname3_value") : "";
                    }
                    if (firstname == 1 || firstname > 4) {
                        sa[9] = rs.getString("mother_firstname4_value") != null ? rs.getString("mother_firstname4_value") : "";
                    }
                }
                if (use_firstname[2] == '1') { // father
                    sa[11] = rs.getString("father_firstname1_value") != null ? rs.getString("father_firstname1_value") : "";

                    if (firstname == 1 || firstname > 2) {
                        sa[12] = rs.getString("father_firstname2_value") != null ? rs.getString("father_firstname2_value") : "";
                    }
                    if (firstname == 1 || firstname > 3) {
                        sa[13] = rs.getString("father_firstname3_value") != null ? rs.getString("father_firstname3_value") : "";
                    }
                    if (firstname == 1 || firstname > 4) {
                        sa[14] = rs.getString("father_firstname4_value") != null ? rs.getString("father_firstname4_value") : "";
                    }
                }
                if (use_firstname[3] == '1') { // partner
                    sa[16] = rs.getString("partner_firstname1_value") != null ? rs.getString("partner_firstname1_value") : "";

                    if (firstname == 1 || firstname > 2) {
                        sa[17] = rs.getString("partner_firstname2_value") != null ? rs.getString("partner_firstname2_value") : "";
                    }
                    if (firstname == 1 || firstname > 3) {
                        sa[18] = rs.getString("partner_firstname3_value") != null ? rs.getString("partner_firstname3_value") : "";
                    }
                    if (firstname == 1 || firstname > 4) {
                        sa[19] = rs.getString("partner_firstname4_value") != null ? rs.getString("partner_firstname4_value") : "";
                    }
                }

                int lv_temp = 0;
                int exact = 0;

                for (String s : sa) {
                    if (!s.isEmpty()) {
                        lv_temp += Integer.parseInt(s);

                        if (Integer.parseInt(s) == 0) {
                            exact++;
                        }
                    }
                }

                lv[lv_temp]++;

                switch (exact) {
                    case 1:
                        lv_1[lv_temp]++;
                        break;
                    case 2:
                        lv_2[lv_temp]++;
                        break;
                    case 3:
                        lv_3[lv_temp]++;
                        break;
                    case 4:
                        lv_4[lv_temp]++;
                        break;
                    case 5:
                        lv_5[lv_temp]++;
                        break;
                    case 6:
                        lv_6[lv_temp]++;
                        break;
                    case 7:
                        lv_7[lv_temp]++;
                        break;
                    case 8:
                        lv_8[lv_temp]++;
                        break;
                    case 9:
                        lv_9[lv_temp]++;
                        break;
                    case 10:
                        lv_10[lv_temp]++;
                        break;
                    case 11:
                        lv_11[lv_temp]++;
                        break;
                    case 12:
                        lv_12[lv_temp]++;
                        break;
                    case 13:
                        lv_13[lv_temp]++;
                        break;
                    case 14:
                        lv_14[lv_temp]++;
                        break;
                    case 15:
                        lv_15[lv_temp]++;
                        break;
                    case 16:
                        lv_16[lv_temp]++;
                        break;
                    case 17:
                        lv_17[lv_temp]++;
                        break;
                    case 18:
                        lv_18[lv_temp]++;
                        break;
                    case 19:
                        lv_19[lv_temp]++;
                        break;
                    case 20:
                        lv_20[lv_temp]++;
                        break;
                    default:
                        lv_0[lv_temp]++;
                        break;
                }
            }

            // Do something with results


            // delete everything from table
            con.createStatement().execute("DELETE FROM matrix");

            for (int i = 0; i < 80; i++) {
                String query = "INSERT INTO matrix(lv,n,e0,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20)VALUES("
                        + i + ","
                        + lv[i] + ","
                        + lv_0[i] + ","
                        + lv_1[i] + ","
                        + lv_2[i] + ","
                        + lv_3[i] + ","
                        + lv_4[i] + ","
                        + lv_5[i] + ","
                        + lv_6[i] + ","
                        + lv_7[i] + ","
                        + lv_8[i] + ","
                        + lv_9[i] + ","
                        + lv_10[i] + ","
                        + lv_11[i] + ","
                        + lv_12[i] + ","
                        + lv_13[i] + ","
                        + lv_14[i] + ","
                        + lv_15[i] + ","
                        + lv_16[i] + ","
                        + lv_17[i] + ","
                        + lv_18[i] + ","
                        + lv_19[i] + ","
                        + lv_20[i] + ");";

                con.createStatement().execute(query);

            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
