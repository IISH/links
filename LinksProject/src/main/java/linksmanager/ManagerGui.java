package linksmanager;

import java.awt.Container;
import java.awt.Cursor;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;

import java.io.File;
import java.io.InputStream;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.ArrayList;
import java.util.Properties;

import javax.swing.*;
import javax.swing.AbstractListModel;
import javax.swing.ButtonGroup;
import javax.swing.GroupLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.LayoutStyle;
import javax.swing.UIManager;
import javax.swing.WindowConstants;

import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.jgoodies.forms.factories.*;
import com.jgoodies.forms.layout.*;

//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.apache.logging.log4j.status.StatusLogger;

import modulemain.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import connectors.CsvConnector;
import dataset.Options;
import enumdefinitions.IndexType;

import modulemain.LinksCleanedThread;

import general.Functions;
import general.PrintLogger;


/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-29-Jul-2014 Explicit imports
 * FL-05-Aug-2014 ref db also in GUI
 * FL-20-Aug-2014 Occupation added
 * FL-09-Feb-2015 Latest change
 */

public class ManagerGui extends javax.swing.JFrame
{
    //static final Logger logger = LogManager.getLogger( ManagerGui.class.getName() );
    //protected final static Logger logger = StatusLogger.getLogger();
    //static final Logger logger = LogManager.getLogger( "links" );        // "links" name specified in log4j.xml

    static PrintLogger plog = null;

    Document doc;

    private Options opts;

    // strings read from property file
    private String dbgRenewData        = "";
    private String dbgPrepieceSuffix   = "";
    private String dbgFirstnames       = "";
    private String dbgFamilynames      = "";
    private String dbgLocations        = "";
    private String dbgStatusSex        = "";
    private String dbgRegType          = "";
    private String dbgOccupation       = "";
    private String dbgAge              = "";
    private String dbgRole             = "";
    private String dbgDates            = "";
    private String dbgMinMaxMarriage   = "";
    private String dbgPartsToFullDate  = "";
    private String dbgDaysSinceBegin   = "";
    private String dbgPostTasks        = "";
    private String dbgRemoveDuplicates = "";
    private String dbgPrematch         = "";
    private String dbgMatch            = "";


    public ManagerGui()
    {
        //String timestamp = LinksSpecific.getTimeStamp2( "HH:mm:ss" );
        //System.out.println( timestamp + " ManagerGui/ManagerGui()" );

        setLookAndFeel();
        createDomProject();
        initComponents();

        // these 4 are not needed anymore
        tpMain.remove( pMain );
        tpMain.remove( pCsv );
        tpMain.remove( pCsvToDatabase );
        tpMain.remove( pBronInternToLinksOrigineel );

        getContentPane().revalidate();
        getContentPane().repaint();

        //tpMain.setSelectedIndex( 4 );       // select the Cleaned tab

        // disable the first 4 tabs because they are no longer used (Genlias only)
        //tpMain.setEnabledAt( 0, false );
        //tpMain.setEnabledAt( 1, false );
        //tpMain.setEnabledAt( 2, false );
        //tpMain.setEnabledAt( 3, false );

        Properties properties = Functions.getProperties();          // Read properties file
        loadProperties( plog, properties );                         // fill GUI fields
    }

    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    // Generated using JFormDesigner non-commercial license
    private void initComponents() {
        menuBar = new JMenuBar();
        fileMenu = new JMenu();
        openMenuItem = new JMenuItem();
        saveMenuItem = new JMenuItem();
        exitMenuItem = new JMenuItem();
        editMenu = new JMenu();
        cutMenuItem = new JMenuItem();
        copyMenuItem = new JMenuItem();
        pasteMenuItem = new JMenuItem();
        deleteMenuItem = new JMenuItem();
        helpMenu = new JMenu();
        contentsMenuItem = new JMenuItem();
        aboutMenuItem = new JMenuItem();
        tpMain = new JTabbedPane();
        pMain = new JPanel();
        bMAINAdd = new JButton();
        bMAINDelete = new JButton();
        tMAINProjectName = new JTextField();
        jLabel1 = new JLabel();
        jLabel19 = new JLabel();
        tMAINDbLocation = new JTextField();
        jLabel24 = new JLabel();
        tMAINPass = new JTextField();
        tMAINUser = new JTextField();
        jLabel26 = new JLabel();
        bMAINSave = new JButton();
        bMAINClear = new JButton();
        bMAINActivate = new JButton();
        jLabel30 = new JLabel();
        jScrollPane5 = new JScrollPane();
        taMAINOpmerkingen = new JTextArea();
        jScrollPane4 = new JScrollPane();
        lMAINProjects = new JList<>();
        jLabel37 = new JLabel();
        tMAINPass1 = new JTextField();
        tMAINPass2 = new JTextField();
        jButton2 = new JButton();
        jLabel38 = new JLabel();
        jLabel39 = new JLabel();
        tMAINPass3 = new JTextField();
        tMAINPass4 = new JTextField();
        cbMAINbebi = new JCheckBox();
        jLabel40 = new JLabel();
        cbMAINbilo = new JCheckBox();
        jLabel41 = new JLabel();
        cbMAINlolc = new JCheckBox();
        pCsv = new JPanel();
        jLabel7 = new JLabel();
        tfCSVCsvFile = new JTextField();
        bCSVPath = new JButton();
        bCSVGetHeader = new JButton();
        jLabel12 = new JLabel();
        tfCSVOutputFile = new JTextField();
        bCSVPathOutpuFile = new JButton();
        jLabel13 = new JLabel();
        tfCSVFilterText = new JTextField();
        jLabel9 = new JLabel();
        tfCSVLineData = new JTextField();
        jLabel2 = new JLabel();
        jScrollPane1 = new JScrollPane();
        taCSVOutput = new JTextArea();
        jLabel8 = new JLabel();
        tfCSVInfo = new JTextField();
        jLabel18 = new JLabel();
        jLabel20 = new JLabel();
        tfCSVFilterIndex = new JTextField();
        bCSVBeginFilter = new JButton();
        pCsvToDatabase = new JPanel();
        bSelectCsvToDbCsvFile = new JButton();
        tfCTBCsvFiles = new JTextField();
        jLabel3 = new JLabel();
        jLabel10 = new JLabel();
        jLabel14 = new JLabel();
        jLabel15 = new JLabel();
        jLabel16 = new JLabel();
        jLabel17 = new JLabel();
        tfCTDDbLocation = new JTextField();
        tfCTDPreFix = new JTextField();
        tfCTDDbName = new JTextField();
        tfCTDDbUser = new JTextField();
        tfCTDDbPass = new JTextField();
        jScrollPane2 = new JScrollPane();
        taCTDOutput = new JTextArea();
        bCsvToDbStart = new JButton();
        jLabel11 = new JLabel();
        tfCTDLatestOutput = new JTextField();
        jLabel21 = new JLabel();
        tfCTDLineNumber = new JTextField();
        pbDTCTotal = new JProgressBar();
        jLabel22 = new JLabel();
        jLabel23 = new JLabel();
        pbDTCFile = new JProgressBar();
        tfCTDdelay = new JTextField();
        jLabel32 = new JLabel();
        rbBebiIndexGeen = new JRadioButton();
        rbBebiIndexGenlias = new JRadioButton();
        pBronInternToLinksOrigineel = new JPanel();
        jLabel25 = new JLabel();
        tfBILODbLocation = new JTextField();
        jLabel27 = new JLabel();
        tfBILODbUser = new JTextField();
        tfBILODbPass = new JTextField();
        jLabel28 = new JLabel();
        jLabel29 = new JLabel();
        bBronInternToLinksOrigineel = new JButton();
        tfBILOProjectName = new JTextField();
        jLabel31 = new JLabel();
        jScrollPane3 = new JScrollPane();
        taBILOOutput = new JTextArea();
        tfBILOLatestOutput = new JTextField();
        jLabel46 = new JLabel();
        tfBILOdelay = new JTextField();
        tfBILODeleteSource = new JTextField();
        jLabel33 = new JLabel();
        cbOrunCleaned = new JCheckBox();
        pLOLC = new JPanel();
        tbLOLCurl = new JTextField();
        jLabel4 = new JLabel();
        tbLOLCuser = new JTextField();
        tbLOLCpass = new JTextField();
        jLabel42 = new JLabel();
        jLabel43 = new JLabel();
        tbLOLCSourceId = new JTextField();
        jLabel44 = new JLabel();
        btnStartProcessClean = new JButton();
        jScrollPane6 = new JScrollPane();
        taLOLCoutput = new JTextArea();
        jLabel45 = new JLabel();
        tbLOLClatestOutput = new JTextField();
        jLabel51 = new JLabel();
        cbCdoFamilynames = new JCheckBox();
        cbCdoStatusSex = new JCheckBox();
        cbCdoDates = new JCheckBox();
        cbCdoRefreshData = new JCheckBox();
        cbCdoRegType = new JCheckBox();
        cbCdoLocations = new JCheckBox();
        cbCdoMinMaxMarriage = new JCheckBox();
        cbCdoPartsToFullDate = new JCheckBox();
        cbCdoDaysSinceBegin = new JCheckBox();
        cbCdoPostTasks = new JCheckBox();
        tbLOLCrefurl = new JTextField();
        tbLOLCrefuser = new JTextField();
        tbLOLCrefpass = new JTextField();
        label1 = new JLabel();
        label2 = new JLabel();
        label3 = new JLabel();
        label4 = new JLabel();
        tbLOLCrefdb = new JTextField();
        cbCdoOccupation = new JCheckBox();
        cbCdoFirstnames = new JCheckBox();
        cbCdoPrepieceSuffix = new JCheckBox();
        cbCdoRemoveDuplicates = new JCheckBox();
        pPrematch = new JPanel();
        cbPdoFrequencyTables = new JCheckBox();
        cbPdoLevenshtein = new JCheckBox();
        cbPdoCreateBaseTable = new JCheckBox();
        cbPdoSplitFirstnames = new JCheckBox();
        btnStartProcessPrematch = new JButton();
        jScrollPane7 = new JScrollPane();
        taPresult = new JTextArea();
        cbPdoNamesToNos = new JCheckBox();
        taPinfo = new JTextField();
        cbPdoStandardization = new JCheckBox();
        pMatch = new JPanel();
        scrollPane1 = new JScrollPane();
        textArea1 = new JTextArea();
        btnStartProcessMatch = new JButton();

        //======== this ========
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setTitle("LINKS Data Manager 2.0");
        setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
        setName("this");
        Container contentPane = getContentPane();

        //======== menuBar ========
        {
            menuBar.setName("menuBar");

            //======== fileMenu ========
            {
                fileMenu.setText("File");
                fileMenu.setName("fileMenu");

                //---- openMenuItem ----
                openMenuItem.setText("Open");
                openMenuItem.setName("openMenuItem");
                openMenuItem.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        openMenuItemActionPerformed(e);
                    }
                });
                fileMenu.add(openMenuItem);

                //---- saveMenuItem ----
                saveMenuItem.setText("Save");
                saveMenuItem.setName("saveMenuItem");
                saveMenuItem.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        saveMenuItemActionPerformed(e);
                    }
                });
                fileMenu.add(saveMenuItem);

                //---- exitMenuItem ----
                exitMenuItem.setText("Exit");
                exitMenuItem.setName("exitMenuItem");
                exitMenuItem.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        exitMenuItemActionPerformed(e);
                    }
                });
                fileMenu.add(exitMenuItem);
            }
            menuBar.add(fileMenu);

            //======== editMenu ========
            {
                editMenu.setText("Edit");
                editMenu.setName("editMenu");

                //---- cutMenuItem ----
                cutMenuItem.setText("Cut");
                cutMenuItem.setName("cutMenuItem");
                editMenu.add(cutMenuItem);

                //---- copyMenuItem ----
                copyMenuItem.setText("Copy");
                copyMenuItem.setName("copyMenuItem");
                editMenu.add(copyMenuItem);

                //---- pasteMenuItem ----
                pasteMenuItem.setText("Paste");
                pasteMenuItem.setName("pasteMenuItem");
                editMenu.add(pasteMenuItem);

                //---- deleteMenuItem ----
                deleteMenuItem.setText("Delete");
                deleteMenuItem.setName("deleteMenuItem");
                editMenu.add(deleteMenuItem);
            }
            menuBar.add(editMenu);

            //======== helpMenu ========
            {
                helpMenu.setText("Help");
                helpMenu.setName("helpMenu");

                //---- contentsMenuItem ----
                contentsMenuItem.setText("Contents");
                contentsMenuItem.setName("contentsMenuItem");
                helpMenu.add(contentsMenuItem);

                //---- aboutMenuItem ----
                aboutMenuItem.setText("About");
                aboutMenuItem.setName("aboutMenuItem");
                helpMenu.add(aboutMenuItem);
            }
            menuBar.add(helpMenu);
        }
        setJMenuBar(menuBar);

        //======== tpMain ========
        {
            tpMain.setName("tpMain");

            //======== pMain ========
            {
                pMain.setName("pMain");

                //---- bMAINAdd ----
                bMAINAdd.setText("Add");
                bMAINAdd.setName("bMAINAdd");
                bMAINAdd.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        bMAINAddActionPerformed(e);
                    }
                });

                //---- bMAINDelete ----
                bMAINDelete.setText("Delete");
                bMAINDelete.setName("bMAINDelete");
                bMAINDelete.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        bMAINDeleteActionPerformed(e);
                    }
                });

                //---- tMAINProjectName ----
                tMAINProjectName.setName("tMAINProjectName");

                //---- jLabel1 ----
                jLabel1.setText("Project name");
                jLabel1.setName("jLabel1");

                //---- jLabel19 ----
                jLabel19.setText("User");
                jLabel19.setName("jLabel19");

                //---- tMAINDbLocation ----
                tMAINDbLocation.setName("tMAINDbLocation");

                //---- jLabel24 ----
                jLabel24.setText("Database location");
                jLabel24.setName("jLabel24");

                //---- tMAINPass ----
                tMAINPass.setName("tMAINPass");

                //---- tMAINUser ----
                tMAINUser.setName("tMAINUser");

                //---- jLabel26 ----
                jLabel26.setText("Pass");
                jLabel26.setName("jLabel26");

                //---- bMAINSave ----
                bMAINSave.setText("Save");
                bMAINSave.setName("bMAINSave");
                bMAINSave.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        bMAINSaveActionPerformed(e);
                    }
                });

                //---- bMAINClear ----
                bMAINClear.setText("Clear");
                bMAINClear.setName("bMAINClear");
                bMAINClear.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        bMAINClearActionPerformed(e);
                    }
                });

                //---- bMAINActivate ----
                bMAINActivate.setText("Activate");
                bMAINActivate.setName("bMAINActivate");
                bMAINActivate.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        bMAINActivateActionPerformed(e);
                    }
                });

                //---- jLabel30 ----
                jLabel30.setText("EXTRA");
                jLabel30.setName("jLabel30");

                //======== jScrollPane5 ========
                {
                    jScrollPane5.setName("jScrollPane5");

                    //---- taMAINOpmerkingen ----
                    taMAINOpmerkingen.setColumns(20);
                    taMAINOpmerkingen.setRows(5);
                    taMAINOpmerkingen.setName("taMAINOpmerkingen");
                    jScrollPane5.setViewportView(taMAINOpmerkingen);
                }

                //======== jScrollPane4 ========
                {
                    jScrollPane4.setName("jScrollPane4");

                    //---- lMAINProjects ----
                    lMAINProjects.setModel(new AbstractListModel<String>() {
                        String[] values = {

                        };
                        @Override
                        public int getSize() { return values.length; }
                        @Override
                        public String getElementAt(int i) { return values[i]; }
                    });
                    lMAINProjects.setName("lMAINProjects");
                    lMAINProjects.addListSelectionListener(new ListSelectionListener() {
                        @Override
                        public void valueChanged(ListSelectionEvent e) {
                            lMAINProjectsValueChanged(e);
                        }
                    });
                    jScrollPane4.setViewportView(lMAINProjects);
                }

                //---- jLabel37 ----
                jLabel37.setText("Logbestand");
                jLabel37.setName("jLabel37");

                //---- tMAINPass1 ----
                tMAINPass1.setName("tMAINPass1");

                //---- tMAINPass2 ----
                tMAINPass2.setName("tMAINPass2");

                //---- jButton2 ----
                jButton2.setText("...");
                jButton2.setName("jButton2");

                //---- jLabel38 ----
                jLabel38.setText("EXTRA");
                jLabel38.setName("jLabel38");

                //---- jLabel39 ----
                jLabel39.setText("EXTRA");
                jLabel39.setName("jLabel39");

                //---- tMAINPass3 ----
                tMAINPass3.setName("tMAINPass3");

                //---- tMAINPass4 ----
                tMAINPass4.setName("tMAINPass4");

                //---- cbMAINbebi ----
                cbMAINbebi.setText("Bron Extern To Bron Intern");
                cbMAINbebi.setName("cbMAINbebi");

                //---- jLabel40 ----
                jLabel40.setText("Activate");
                jLabel40.setName("jLabel40");

                //---- cbMAINbilo ----
                cbMAINbilo.setText("Bron Intern To Links Originee");
                cbMAINbilo.setName("cbMAINbilo");

                //---- jLabel41 ----
                jLabel41.setText("Opmerkingen");
                jLabel41.setName("jLabel41");

                //---- cbMAINlolc ----
                cbMAINlolc.setText("lLinks Origineel To Links Cleaned");
                cbMAINlolc.setName("cbMAINlolc");

                GroupLayout pMainLayout = new GroupLayout(pMain);
                pMain.setLayout(pMainLayout);
                pMainLayout.setHorizontalGroup(
                    pMainLayout.createParallelGroup()
                        .addGroup(GroupLayout.Alignment.TRAILING, pMainLayout.createSequentialGroup()
                            .addContainerGap()
                            .addGroup(pMainLayout.createParallelGroup(GroupLayout.Alignment.LEADING, false)
                                .addGroup(pMainLayout.createSequentialGroup()
                                    .addComponent(bMAINAdd)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                    .addComponent(bMAINDelete))
                                .addComponent(jScrollPane4, GroupLayout.PREFERRED_SIZE, 153, GroupLayout.PREFERRED_SIZE))
                            .addGap(18, 18, 18)
                            .addGroup(pMainLayout.createParallelGroup()
                                .addComponent(jLabel24)
                                .addComponent(jLabel1)
                                .addComponent(jLabel19)
                                .addComponent(jLabel26)
                                .addComponent(jLabel37)
                                .addComponent(jLabel30)
                                .addComponent(jLabel38)
                                .addComponent(jLabel39)
                                .addComponent(jLabel40)
                                .addComponent(jLabel41))
                            .addGap(36, 36, 36)
                            .addGroup(pMainLayout.createParallelGroup()
                                .addGroup(GroupLayout.Alignment.TRAILING, pMainLayout.createParallelGroup()
                                    .addComponent(tMAINPass4, GroupLayout.Alignment.TRAILING)
                                    .addComponent(tMAINPass3, GroupLayout.Alignment.TRAILING)
                                    .addComponent(tMAINUser)
                                    .addComponent(tMAINPass)
                                    .addGroup(GroupLayout.Alignment.TRAILING, pMainLayout.createSequentialGroup()
                                        .addComponent(bMAINClear)
                                        .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                        .addComponent(bMAINActivate)
                                        .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                        .addComponent(bMAINSave))
                                    .addGroup(GroupLayout.Alignment.TRAILING, pMainLayout.createSequentialGroup()
                                        .addComponent(tMAINPass1)
                                        .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                        .addComponent(jButton2))
                                    .addComponent(jScrollPane5)
                                    .addComponent(tMAINDbLocation)
                                    .addComponent(tMAINPass2)
                                    .addComponent(tMAINProjectName))
                                .addGroup(pMainLayout.createSequentialGroup()
                                    .addComponent(cbMAINbebi)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addComponent(cbMAINbilo)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addComponent(cbMAINlolc)))
                            .addGap(1666, 1666, 1666))
                );
                pMainLayout.setVerticalGroup(
                    pMainLayout.createParallelGroup()
                        .addGroup(pMainLayout.createSequentialGroup()
                            .addContainerGap()
                            .addGroup(pMainLayout.createParallelGroup()
                                .addComponent(jScrollPane4, GroupLayout.DEFAULT_SIZE, 583, Short.MAX_VALUE)
                                .addGroup(pMainLayout.createSequentialGroup()
                                    .addGroup(pMainLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(tMAINProjectName, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jLabel1))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addGroup(pMainLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(tMAINDbLocation, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jLabel24))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addGroup(pMainLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(tMAINUser, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jLabel19))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addGroup(pMainLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(tMAINPass, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jLabel26))
                                    .addGroup(pMainLayout.createParallelGroup()
                                        .addGroup(pMainLayout.createSequentialGroup()
                                            .addGap(9, 9, 9)
                                            .addComponent(jLabel37))
                                        .addGroup(pMainLayout.createSequentialGroup()
                                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                            .addGroup(pMainLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                                .addComponent(jButton2)
                                                .addComponent(tMAINPass1, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addGroup(pMainLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(tMAINPass2, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jLabel30))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addGroup(pMainLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(tMAINPass3, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jLabel38))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addGroup(pMainLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(tMAINPass4, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jLabel39))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addGroup(pMainLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(cbMAINbebi)
                                        .addComponent(jLabel40)
                                        .addComponent(cbMAINbilo)
                                        .addComponent(cbMAINlolc))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addGroup(pMainLayout.createParallelGroup()
                                        .addComponent(jScrollPane5, GroupLayout.DEFAULT_SIZE, 350, Short.MAX_VALUE)
                                        .addGroup(pMainLayout.createSequentialGroup()
                                            .addComponent(jLabel41)
                                            .addGap(0, 335, Short.MAX_VALUE)))))
                            .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                            .addGroup(pMainLayout.createParallelGroup()
                                .addGroup(pMainLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                    .addComponent(bMAINActivate)
                                    .addComponent(bMAINClear)
                                    .addComponent(bMAINSave))
                                .addGroup(pMainLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                    .addComponent(bMAINAdd)
                                    .addComponent(bMAINDelete)))
                            .addContainerGap())
                );
            }
            tpMain.addTab("MAIN", pMain);

            //======== pCsv ========
            {
                pCsv.setName("pCsv");

                //---- jLabel7 ----
                jLabel7.setText("CSV File");
                jLabel7.setName("jLabel7");

                //---- tfCSVCsvFile ----
                tfCSVCsvFile.setName("tfCSVCsvFile");

                //---- bCSVPath ----
                bCSVPath.setText("...");
                bCSVPath.setName("bCSVPath");
                bCSVPath.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        bCSVPathActionPerformed(e);
                    }
                });

                //---- bCSVGetHeader ----
                bCSVGetHeader.setText("Get Header");
                bCSVGetHeader.setName("bCSVGetHeader");
                bCSVGetHeader.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        bCSVGetHeaderActionPerformed(e);
                    }
                });

                //---- jLabel12 ----
                jLabel12.setText("Destination");
                jLabel12.setName("jLabel12");

                //---- tfCSVOutputFile ----
                tfCSVOutputFile.setName("tfCSVOutputFile");

                //---- bCSVPathOutpuFile ----
                bCSVPathOutpuFile.setText("...");
                bCSVPathOutpuFile.setName("bCSVPathOutpuFile");
                bCSVPathOutpuFile.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        bCSVPathOutpuFileActionPerformed(e);
                    }
                });

                //---- jLabel13 ----
                jLabel13.setText("FILTER OPTIONS");
                jLabel13.setName("jLabel13");

                //---- tfCSVFilterText ----
                tfCSVFilterText.setName("tfCSVFilterText");

                //---- jLabel9 ----
                jLabel9.setText("LINE");
                jLabel9.setName("jLabel9");

                //---- tfCSVLineData ----
                tfCSVLineData.setName("tfCSVLineData");

                //---- jLabel2 ----
                jLabel2.setText("Output");
                jLabel2.setName("jLabel2");

                //======== jScrollPane1 ========
                {
                    jScrollPane1.setName("jScrollPane1");

                    //---- taCSVOutput ----
                    taCSVOutput.setColumns(20);
                    taCSVOutput.setRows(5);
                    taCSVOutput.setName("jtaOutput");
                    jScrollPane1.setViewportView(taCSVOutput);
                }

                //---- jLabel8 ----
                jLabel8.setText("Info");
                jLabel8.setName("jLabel8");

                //---- tfCSVInfo ----
                tfCSVInfo.setName("tfCSVInfo");

                //---- jLabel18 ----
                jLabel18.setText("Header index. index starts with 0");
                jLabel18.setName("jLabel18");

                //---- jLabel20 ----
                jLabel20.setText("Filter text");
                jLabel20.setName("jLabel20");

                //---- tfCSVFilterIndex ----
                tfCSVFilterIndex.setName("tfCSVFilterIndex");

                //---- bCSVBeginFilter ----
                bCSVBeginFilter.setText("begin Filtering");
                bCSVBeginFilter.setName("bCSVBeginFilter");
                bCSVBeginFilter.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        bCSVBeginFilterActionPerformed(e);
                    }
                });

                GroupLayout pCsvLayout = new GroupLayout(pCsv);
                pCsv.setLayout(pCsvLayout);
                pCsvLayout.setHorizontalGroup(
                    pCsvLayout.createParallelGroup()
                        .addGroup(GroupLayout.Alignment.TRAILING, pCsvLayout.createSequentialGroup()
                            .addContainerGap()
                            .addGroup(pCsvLayout.createParallelGroup(GroupLayout.Alignment.TRAILING)
                                .addComponent(jScrollPane1, GroupLayout.Alignment.LEADING, GroupLayout.DEFAULT_SIZE, 1097, Short.MAX_VALUE)
                                .addGroup(pCsvLayout.createSequentialGroup()
                                    .addGroup(pCsvLayout.createParallelGroup(GroupLayout.Alignment.LEADING, false)
                                        .addComponent(jLabel13, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                        .addComponent(jLabel8)
                                        .addComponent(jLabel20)
                                        .addComponent(jLabel9)
                                        .addComponent(jLabel7)
                                        .addComponent(jLabel12, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addGroup(pCsvLayout.createParallelGroup()
                                        .addComponent(tfCSVInfo, GroupLayout.DEFAULT_SIZE, 985, Short.MAX_VALUE)
                                        .addComponent(tfCSVLineData, GroupLayout.DEFAULT_SIZE, 985, Short.MAX_VALUE)
                                        .addGroup(GroupLayout.Alignment.TRAILING, pCsvLayout.createSequentialGroup()
                                            .addGroup(pCsvLayout.createParallelGroup(GroupLayout.Alignment.TRAILING)
                                                .addComponent(tfCSVFilterText, GroupLayout.DEFAULT_SIZE, 863, Short.MAX_VALUE)
                                                .addGroup(GroupLayout.Alignment.LEADING, pCsvLayout.createSequentialGroup()
                                                    .addComponent(jLabel18)
                                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                                    .addComponent(tfCSVFilterIndex, GroupLayout.DEFAULT_SIZE, 645, Short.MAX_VALUE))
                                                .addComponent(tfCSVOutputFile, GroupLayout.Alignment.LEADING, GroupLayout.DEFAULT_SIZE, 863, Short.MAX_VALUE)
                                                .addComponent(tfCSVCsvFile, GroupLayout.DEFAULT_SIZE, 863, Short.MAX_VALUE))
                                            .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                            .addGroup(pCsvLayout.createParallelGroup()
                                                .addComponent(bCSVPathOutpuFile, GroupLayout.PREFERRED_SIZE, 28, GroupLayout.PREFERRED_SIZE)
                                                .addGroup(pCsvLayout.createParallelGroup()
                                                    .addComponent(bCSVBeginFilter, GroupLayout.Alignment.TRAILING, GroupLayout.PREFERRED_SIZE, 110, GroupLayout.PREFERRED_SIZE)
                                                    .addComponent(bCSVGetHeader, GroupLayout.Alignment.TRAILING, GroupLayout.PREFERRED_SIZE, 110, GroupLayout.PREFERRED_SIZE)
                                                    .addComponent(bCSVPath, GroupLayout.PREFERRED_SIZE, 28, GroupLayout.PREFERRED_SIZE))))))
                                .addComponent(jLabel2, GroupLayout.Alignment.LEADING))
                            .addContainerGap())
                );
                pCsvLayout.setVerticalGroup(
                    pCsvLayout.createParallelGroup()
                        .addGroup(pCsvLayout.createSequentialGroup()
                            .addContainerGap()
                            .addGroup(pCsvLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                .addComponent(jLabel7)
                                .addComponent(tfCSVCsvFile, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                .addComponent(bCSVPath))
                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                            .addGroup(pCsvLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                .addComponent(jLabel12)
                                .addComponent(tfCSVOutputFile, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                .addComponent(bCSVPathOutpuFile))
                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                            .addGroup(pCsvLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                .addComponent(bCSVGetHeader)
                                .addComponent(jLabel13)
                                .addComponent(jLabel18)
                                .addComponent(tfCSVFilterIndex, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                            .addGroup(pCsvLayout.createParallelGroup()
                                .addComponent(bCSVBeginFilter)
                                .addGroup(pCsvLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                    .addComponent(tfCSVFilterText, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                    .addComponent(jLabel20)))
                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                            .addGroup(pCsvLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                .addComponent(tfCSVInfo, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                .addComponent(jLabel8))
                            .addGroup(pCsvLayout.createParallelGroup()
                                .addGroup(pCsvLayout.createSequentialGroup()
                                    .addGap(9, 9, 9)
                                    .addComponent(jLabel9))
                                .addGroup(pCsvLayout.createSequentialGroup()
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addComponent(tfCSVLineData, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)))
                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                            .addComponent(jLabel2)
                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                            .addComponent(jScrollPane1, GroupLayout.DEFAULT_SIZE, 425, Short.MAX_VALUE)
                            .addContainerGap())
                );
            }
            tpMain.addTab("PREP: CSV Filter", pCsv);

            //======== pCsvToDatabase ========
            {
                pCsvToDatabase.setName("pCsvToDatabase");

                //---- bSelectCsvToDbCsvFile ----
                bSelectCsvToDbCsvFile.setText("...");
                bSelectCsvToDbCsvFile.setName("bSelectCsvToDbCsvFile");
                bSelectCsvToDbCsvFile.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        bSelectCsvToDbCsvFileActionPerformed(e);
                    }
                });

                //---- tfCTBCsvFiles ----
                tfCTBCsvFiles.setName("tfCTBCsvFiles");

                //---- jLabel3 ----
                jLabel3.setText("Select CSV File(S)");
                jLabel3.setName("jLabel3");

                //---- jLabel10 ----
                jLabel10.setText("Database Location");
                jLabel10.setName("jLabel10");

                //---- jLabel14 ----
                jLabel14.setText("Project");
                jLabel14.setName("jLabel14");

                //---- jLabel15 ----
                jLabel15.setText("DB Name");
                jLabel15.setName("jLabel15");

                //---- jLabel16 ----
                jLabel16.setText("Password");
                jLabel16.setName("jLabel16");

                //---- jLabel17 ----
                jLabel17.setText("User");
                jLabel17.setName("jLabel17");

                //---- tfCTDDbLocation ----
                tfCTDDbLocation.setName("tfCTDDbLocation");

                //---- tfCTDPreFix ----
                tfCTDPreFix.setName("tfCTDPreFix");

                //---- tfCTDDbName ----
                tfCTDDbName.setName("tfCTDDbName");

                //---- tfCTDDbUser ----
                tfCTDDbUser.setName("tfCTDDbUser");

                //---- tfCTDDbPass ----
                tfCTDDbPass.setName("tfCTDDbPass");
                tfCTDDbPass.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        tfCTDDbPassActionPerformed(e);
                    }
                });

                //======== jScrollPane2 ========
                {
                    jScrollPane2.setName("jScrollPane2");

                    //---- taCTDOutput ----
                    taCTDOutput.setColumns(20);
                    taCTDOutput.setRows(5);
                    taCTDOutput.setName("taCTDOutput");
                    jScrollPane2.setViewportView(taCTDOutput);
                }

                //---- bCsvToDbStart ----
                bCsvToDbStart.setText("Start Process");
                bCsvToDbStart.setName("bCsvToDbStart");
                bCsvToDbStart.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        bCsvToDbStartActionPerformed(e);
                    }
                });

                //---- jLabel11 ----
                jLabel11.setFont(new Font("Tahoma", Font.BOLD, 11));
                jLabel11.setText(" Latest information and Log (below)");
                jLabel11.setName("jLabel11");

                //---- tfCTDLatestOutput ----
                tfCTDLatestOutput.setName("tfCTDLatestOutput");

                //---- jLabel21 ----
                jLabel21.setText("Line number");
                jLabel21.setName("jLabel21");

                //---- tfCTDLineNumber ----
                tfCTDLineNumber.setName("tfCTDLineNumber");

                //---- pbDTCTotal ----
                pbDTCTotal.setName("pbDTCTotal");

                //---- jLabel22 ----
                jLabel22.setText("Total ");
                jLabel22.setName("jLabel22");

                //---- jLabel23 ----
                jLabel23.setText("Current File");
                jLabel23.setName("jLabel23");

                //---- pbDTCFile ----
                pbDTCFile.setName("pbDTCFile");

                //---- tfCTDdelay ----
                tfCTDdelay.setText("0");
                tfCTDdelay.setName("tfCTDdelay");

                //---- jLabel32 ----
                jLabel32.setText("Delay minutes");
                jLabel32.setName("jLabel32");

                //---- rbBebiIndexGeen ----
                rbBebiIndexGeen.setText("Geen Indexering");
                rbBebiIndexGeen.setName("rbBebiIndexGeen");
                rbBebiIndexGeen.setSelected(true);

                //---- rbBebiIndexGenlias ----
                rbBebiIndexGenlias.setText("Genlias 7-tabellen indexering");
                rbBebiIndexGenlias.setName("rbBebiIndexGenlias");
                rbBebiIndexGenlias.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        rbBebiIndexGenliasActionPerformed(e);
                    }
                });

                GroupLayout pCsvToDatabaseLayout = new GroupLayout(pCsvToDatabase);
                pCsvToDatabase.setLayout(pCsvToDatabaseLayout);
                pCsvToDatabaseLayout.setHorizontalGroup(
                    pCsvToDatabaseLayout.createParallelGroup()
                        .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                            .addContainerGap()
                            .addGroup(pCsvToDatabaseLayout.createParallelGroup()
                                .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                                    .addComponent(jLabel3)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addComponent(tfCTBCsvFiles, GroupLayout.DEFAULT_SIZE, 940, Short.MAX_VALUE)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addComponent(bSelectCsvToDbCsvFile, GroupLayout.PREFERRED_SIZE, 28, GroupLayout.PREFERRED_SIZE))
                                .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                                    .addGroup(pCsvToDatabaseLayout.createParallelGroup(GroupLayout.Alignment.TRAILING)
                                        .addComponent(bCsvToDbStart)
                                        .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                                            .addGroup(pCsvToDatabaseLayout.createParallelGroup()
                                                .addComponent(jLabel15)
                                                .addComponent(jLabel10)
                                                .addComponent(jLabel17)
                                                .addComponent(jLabel16)
                                                .addComponent(jLabel14)
                                                .addComponent(jLabel32))
                                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                            .addGroup(pCsvToDatabaseLayout.createParallelGroup()
                                                .addComponent(tfCTDDbPass, GroupLayout.DEFAULT_SIZE, 281, Short.MAX_VALUE)
                                                .addComponent(tfCTDDbUser, GroupLayout.DEFAULT_SIZE, 281, Short.MAX_VALUE)
                                                .addComponent(tfCTDDbName, GroupLayout.DEFAULT_SIZE, 281, Short.MAX_VALUE)
                                                .addComponent(tfCTDDbLocation, GroupLayout.DEFAULT_SIZE, 281, Short.MAX_VALUE)
                                                .addComponent(tfCTDPreFix, GroupLayout.DEFAULT_SIZE, 281, Short.MAX_VALUE)
                                                .addComponent(tfCTDdelay, GroupLayout.DEFAULT_SIZE, 281, Short.MAX_VALUE)))
                                        .addComponent(rbBebiIndexGeen, GroupLayout.Alignment.LEADING)
                                        .addComponent(rbBebiIndexGenlias, GroupLayout.Alignment.LEADING))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addGroup(pCsvToDatabaseLayout.createParallelGroup()
                                        .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                                            .addGap(58, 58, 58)
                                            .addComponent(jLabel11))
                                        .addComponent(jScrollPane2, GroupLayout.DEFAULT_SIZE, 671, Short.MAX_VALUE)
                                        .addComponent(tfCTDLatestOutput, GroupLayout.DEFAULT_SIZE, 671, Short.MAX_VALUE)
                                        .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                                            .addGroup(pCsvToDatabaseLayout.createParallelGroup()
                                                .addComponent(jLabel21)
                                                .addComponent(jLabel23)
                                                .addComponent(jLabel22))
                                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                            .addGroup(pCsvToDatabaseLayout.createParallelGroup()
                                                .addComponent(pbDTCFile, GroupLayout.DEFAULT_SIZE, 581, Short.MAX_VALUE)
                                                .addComponent(pbDTCTotal, GroupLayout.DEFAULT_SIZE, 581, Short.MAX_VALUE)
                                                .addComponent(tfCTDLineNumber, GroupLayout.DEFAULT_SIZE, 581, Short.MAX_VALUE))))))
                            .addContainerGap())
                );
                pCsvToDatabaseLayout.setVerticalGroup(
                    pCsvToDatabaseLayout.createParallelGroup()
                        .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                            .addGroup(pCsvToDatabaseLayout.createParallelGroup()
                                .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                                    .addGap(45, 45, 45)
                                    .addGroup(pCsvToDatabaseLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(tfCTDDbLocation, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jLabel10)
                                        .addComponent(jLabel11)))
                                .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                                    .addContainerGap()
                                    .addGroup(pCsvToDatabaseLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(jLabel3)
                                        .addComponent(tfCTBCsvFiles, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(bSelectCsvToDbCsvFile))))
                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                            .addGroup(pCsvToDatabaseLayout.createParallelGroup(GroupLayout.Alignment.TRAILING)
                                .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                                    .addGroup(pCsvToDatabaseLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(tfCTDDbName, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jLabel15))
                                    .addGap(7, 7, 7))
                                .addComponent(jLabel23)
                                .addComponent(pbDTCFile, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                            .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                            .addGroup(pCsvToDatabaseLayout.createParallelGroup(GroupLayout.Alignment.TRAILING)
                                .addComponent(pbDTCTotal, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                .addGroup(pCsvToDatabaseLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                    .addComponent(tfCTDDbUser, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                    .addComponent(jLabel17))
                                .addComponent(jLabel22))
                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                            .addGroup(pCsvToDatabaseLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                .addComponent(tfCTDDbPass, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                .addComponent(jLabel16)
                                .addComponent(jLabel21)
                                .addComponent(tfCTDLineNumber, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                            .addGroup(pCsvToDatabaseLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                .addComponent(tfCTDPreFix, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                .addComponent(jLabel14)
                                .addComponent(tfCTDLatestOutput, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                            .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                            .addGroup(pCsvToDatabaseLayout.createParallelGroup(GroupLayout.Alignment.TRAILING)
                                .addComponent(jScrollPane2, GroupLayout.Alignment.LEADING, GroupLayout.DEFAULT_SIZE, 443, Short.MAX_VALUE)
                                .addGroup(GroupLayout.Alignment.LEADING, pCsvToDatabaseLayout.createSequentialGroup()
                                    .addGroup(pCsvToDatabaseLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(tfCTDdelay, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jLabel32))
                                    .addGap(18, 18, 18)
                                    .addComponent(rbBebiIndexGeen)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addComponent(rbBebiIndexGenlias))
                                .addComponent(bCsvToDbStart))
                            .addContainerGap())
                );
            }
            tpMain.addTab("INTERNAL", pCsvToDatabase);

            //======== pBronInternToLinksOrigineel ========
            {
                pBronInternToLinksOrigineel.setName("pBronInternToLinksOrigineel");

                //---- jLabel25 ----
                jLabel25.setText("Database Location");
                jLabel25.setName("jLabel25");

                //---- tfBILODbLocation ----
                tfBILODbLocation.setName("tfBILODbLocation");

                //---- jLabel27 ----
                jLabel27.setText("User");
                jLabel27.setName("jLabel27");

                //---- tfBILODbUser ----
                tfBILODbUser.setName("tfBILODbUser");

                //---- tfBILODbPass ----
                tfBILODbPass.setName("tfBILODbPass");

                //---- jLabel28 ----
                jLabel28.setText("Pass");
                jLabel28.setName("jLabel28");

                //---- jLabel29 ----
                jLabel29.setText("Project name");
                jLabel29.setName("jLabel29");

                //---- bBronInternToLinksOrigineel ----
                bBronInternToLinksOrigineel.setText("Start Process");
                bBronInternToLinksOrigineel.setName("bBronInternToLinksOrigineel");
                bBronInternToLinksOrigineel.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        bBronInternToLinksOrigineelActionPerformed(e);
                    }
                });

                //---- tfBILOProjectName ----
                tfBILOProjectName.setName("tfBILOProjectName");
                tfBILOProjectName.addKeyListener(new KeyAdapter() {
                    @Override
                    public void keyReleased(KeyEvent e) {
                        tfBILOProjectNameKeyReleased(e);
                    }
                });

                //---- jLabel31 ----
                jLabel31.setFont(new Font("Tahoma", Font.BOLD, 11));
                jLabel31.setText(" Latest information and Log (below)");
                jLabel31.setName("jLabel31");

                //======== jScrollPane3 ========
                {
                    jScrollPane3.setName("jScrollPane3");

                    //---- taBILOOutput ----
                    taBILOOutput.setColumns(20);
                    taBILOOutput.setRows(5);
                    taBILOOutput.setName("taBILOOutput");
                    jScrollPane3.setViewportView(taBILOOutput);
                }

                //---- tfBILOLatestOutput ----
                tfBILOLatestOutput.setName("tfBILOLatestOutput");

                //---- jLabel46 ----
                jLabel46.setText("Delay minutes");
                jLabel46.setName("jLabel46");

                //---- tfBILOdelay ----
                tfBILOdelay.setText("0");
                tfBILOdelay.setName("tfBILOdelay");

                //---- tfBILODeleteSource ----
                tfBILODeleteSource.setName("tfBILODeleteSource");
                tfBILODeleteSource.addKeyListener(new KeyAdapter() {
                    @Override
                    public void keyReleased(KeyEvent e) {
                        tfBILODeleteSourceKeyReleased(e);
                    }
                });

                //---- jLabel33 ----
                jLabel33.setText("Source to delete");
                jLabel33.setName("jLabel33");

                //---- cbOrunCleaned ----
                cbOrunCleaned.setText("RUN CLEANED");
                cbOrunCleaned.setName("cbOrunCleaned");

                GroupLayout pBronInternToLinksOrigineelLayout = new GroupLayout(pBronInternToLinksOrigineel);
                pBronInternToLinksOrigineel.setLayout(pBronInternToLinksOrigineelLayout);
                pBronInternToLinksOrigineelLayout.setHorizontalGroup(
                    pBronInternToLinksOrigineelLayout.createParallelGroup()
                        .addGroup(pBronInternToLinksOrigineelLayout.createSequentialGroup()
                            .addContainerGap()
                            .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup()
                                .addComponent(jLabel25)
                                .addComponent(jLabel27)
                                .addComponent(jLabel28)
                                .addComponent(jLabel29)
                                .addComponent(jLabel33)
                                .addComponent(jLabel46))
                            .addGap(18, 18, 18)
                            .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup()
                                .addGroup(pBronInternToLinksOrigineelLayout.createSequentialGroup()
                                    .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup()
                                        .addComponent(tfBILODbPass, GroupLayout.DEFAULT_SIZE, 400, Short.MAX_VALUE)
                                        .addComponent(tfBILODbUser, GroupLayout.DEFAULT_SIZE, 400, Short.MAX_VALUE)
                                        .addComponent(tfBILODbLocation, GroupLayout.DEFAULT_SIZE, 400, Short.MAX_VALUE)
                                        .addComponent(tfBILOProjectName, GroupLayout.DEFAULT_SIZE, 400, Short.MAX_VALUE)
                                        .addComponent(tfBILODeleteSource, GroupLayout.Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, 400, Short.MAX_VALUE)
                                        .addGroup(GroupLayout.Alignment.TRAILING, pBronInternToLinksOrigineelLayout.createSequentialGroup()
                                            .addComponent(tfBILOdelay, GroupLayout.DEFAULT_SIZE, 280, Short.MAX_VALUE)
                                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                            .addComponent(bBronInternToLinksOrigineel)))
                                    .addGap(14, 14, 14))
                                .addGroup(pBronInternToLinksOrigineelLayout.createSequentialGroup()
                                    .addComponent(cbOrunCleaned, GroupLayout.PREFERRED_SIZE, 137, GroupLayout.PREFERRED_SIZE)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)))
                            .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup()
                                .addComponent(jScrollPane3, GroupLayout.Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, 550, Short.MAX_VALUE)
                                .addComponent(tfBILOLatestOutput, GroupLayout.DEFAULT_SIZE, 550, Short.MAX_VALUE)
                                .addComponent(jLabel31))
                            .addContainerGap())
                );
                pBronInternToLinksOrigineelLayout.setVerticalGroup(
                    pBronInternToLinksOrigineelLayout.createParallelGroup()
                        .addGroup(pBronInternToLinksOrigineelLayout.createSequentialGroup()
                            .addContainerGap()
                            .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                .addComponent(jLabel25)
                                .addComponent(tfBILODbLocation, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                .addComponent(jLabel31))
                            .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                            .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                .addComponent(jLabel27)
                                .addComponent(tfBILODbUser, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                .addComponent(tfBILOLatestOutput, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                            .addGap(11, 11, 11)
                            .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(GroupLayout.Alignment.TRAILING)
                                .addGroup(GroupLayout.Alignment.LEADING, pBronInternToLinksOrigineelLayout.createSequentialGroup()
                                    .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(jLabel28)
                                        .addComponent(tfBILODbPass, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(tfBILOProjectName, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jLabel29))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(tfBILODeleteSource, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jLabel33))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(bBronInternToLinksOrigineel)
                                        .addComponent(tfBILOdelay, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jLabel46))
                                    .addGap(18, 18, 18)
                                    .addComponent(cbOrunCleaned))
                                .addComponent(jScrollPane3, GroupLayout.DEFAULT_SIZE, 559, Short.MAX_VALUE))
                            .addContainerGap())
                );
            }
            tpMain.addTab("ORIGINAL", pBronInternToLinksOrigineel);

            //======== pLOLC ========
            {
                pLOLC.setName("pLOLC");

                //---- tbLOLCurl ----
                tbLOLCurl.setName("tbLOLCurl");

                //---- jLabel4 ----
                jLabel4.setText("Links Databases URL:");
                jLabel4.setName("jLabel4");

                //---- tbLOLCuser ----
                tbLOLCuser.setName("tbLOLCuser");

                //---- tbLOLCpass ----
                tbLOLCpass.setName("tbLOLCpass");

                //---- jLabel42 ----
                jLabel42.setText("Links DBs Username:");
                jLabel42.setName("jLabel42");

                //---- jLabel43 ----
                jLabel43.setText("Links DBs Password:");
                jLabel43.setName("jLabel43");

                //---- tbLOLCSourceId ----
                tbLOLCSourceId.setName("tbLOLCBronNr");

                //---- jLabel44 ----
                jLabel44.setText("Source Id:");
                jLabel44.setName("jLabel44");

                //---- btnStartProcessClean ----
                btnStartProcessClean.setText("Start Cleaning");
                btnStartProcessClean.setName("bnLOLCstartProcess");
                btnStartProcessClean.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        bnLOLCstartProcessActionPerformed(e);
                    }
                });

                //======== jScrollPane6 ========
                {
                    jScrollPane6.setName("jScrollPane6");

                    //---- taLOLCoutput ----
                    taLOLCoutput.setColumns(20);
                    taLOLCoutput.setRows(5);
                    taLOLCoutput.setName("taLOLCoutput");
                    jScrollPane6.setViewportView(taLOLCoutput);
                }

                //---- jLabel45 ----
                jLabel45.setFont(new Font("Tahoma", Font.BOLD, 11));
                jLabel45.setText(" Latest information and Log (below)");
                jLabel45.setName("jLabel45");

                //---- tbLOLClatestOutput ----
                tbLOLClatestOutput.setName("tbLOLClatestOutput");

                //---- jLabel51 ----
                jLabel51.setText("Cleaning Tasks:");
                jLabel51.setName("jLabel51");

                //---- cbCdoFamilynames ----
                cbCdoFamilynames.setText("Familynames");
                cbCdoFamilynames.setName("cbCdoNames");

                //---- cbCdoStatusSex ----
                cbCdoStatusSex.setText("Civil Status and Sex");
                cbCdoStatusSex.setName("cbCdoStaatSex");

                //---- cbCdoDates ----
                cbCdoDates.setText("Age, Role, Dates");
                cbCdoDates.setName("cbCdoDates");

                //---- cbCdoRefreshData ----
                cbCdoRefreshData.setSelected(true);
                cbCdoRefreshData.setText("Remove Cleaned Data");
                cbCdoRefreshData.setName("cbCdoVernieuwen");

                //---- cbCdoRegType ----
                cbCdoRegType.setText("Registration Type");
                cbCdoRegType.setName("cbCdoType");

                //---- cbCdoLocations ----
                cbCdoLocations.setText("Locations");
                cbCdoLocations.setName("cbCdoLocations");

                //---- cbCdoMinMaxMarriage ----
                cbCdoMinMaxMarriage.setText("Min Max Marriage");
                cbCdoMinMaxMarriage.setName("cbLOLCdoMinMaxMarriage");

                //---- cbCdoPartsToFullDate ----
                cbCdoPartsToFullDate.setText("Parts to Full Date");
                cbCdoPartsToFullDate.setName("cbLOLCdoPartsToFullDate");

                //---- cbCdoDaysSinceBegin ----
                cbCdoDaysSinceBegin.setText("Days Since Begin");
                cbCdoDaysSinceBegin.setName("cbCdoDaysSinceBegin");

                //---- cbCdoPostTasks ----
                cbCdoPostTasks.setText("Post Tasks");
                cbCdoPostTasks.setName("cbLOLCdoPostTasks");

                //---- tbLOLCrefurl ----
                tbLOLCrefurl.setName("tbLOLCrefurl");

                //---- tbLOLCrefuser ----
                tbLOLCrefuser.setName("tbLOLCrefuser");

                //---- tbLOLCrefpass ----
                tbLOLCrefpass.setName("tbLOLCrefpass");

                //---- label1 ----
                label1.setText("Reference Database URL:");
                label1.setName("label1");

                //---- label2 ----
                label2.setText("Reference DB Username:");
                label2.setName("label2");

                //---- label3 ----
                label3.setText("Reference DB Password:");
                label3.setName("label3");

                //---- label4 ----
                label4.setText("Reference DB Name");
                label4.setName("label4");

                //---- tbLOLCrefdb ----
                tbLOLCrefdb.setName("tbLOLCrefdb");

                //---- cbCdoOccupation ----
                cbCdoOccupation.setText("Occupation");
                cbCdoOccupation.setName("cbCdoOccupation");

                //---- cbCdoFirstnames ----
                cbCdoFirstnames.setText("Firstnames");
                cbCdoFirstnames.setName("cbCdoFirstnames");

                //---- cbCdoPrepieceSuffix ----
                cbCdoPrepieceSuffix.setText("Prepiece, Suffix");
                cbCdoPrepieceSuffix.setName("cbCdoPrepieceSuffix");

                //---- cbCdoRemoveDuplicates ----
                cbCdoRemoveDuplicates.setText("Remove Duplicates");
                cbCdoRemoveDuplicates.setName("cbCdoRemoveDuplicates");

                GroupLayout pLOLCLayout = new GroupLayout(pLOLC);
                pLOLC.setLayout(pLOLCLayout);
                pLOLCLayout.setHorizontalGroup(
                    pLOLCLayout.createParallelGroup()
                        .addGroup(pLOLCLayout.createSequentialGroup()
                            .addContainerGap()
                            .addGroup(pLOLCLayout.createParallelGroup()
                                .addComponent(btnStartProcessClean, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                .addGroup(GroupLayout.Alignment.TRAILING, pLOLCLayout.createSequentialGroup()
                                    .addGroup(pLOLCLayout.createParallelGroup(GroupLayout.Alignment.TRAILING)
                                        .addGroup(pLOLCLayout.createSequentialGroup()
                                            .addGroup(pLOLCLayout.createParallelGroup()
                                                .addGroup(pLOLCLayout.createParallelGroup(GroupLayout.Alignment.TRAILING, false)
                                                    .addComponent(label3, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                                    .addComponent(label1, GroupLayout.Alignment.LEADING, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                                    .addComponent(label2, GroupLayout.Alignment.LEADING, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                                                .addComponent(label4, GroupLayout.PREFERRED_SIZE, 185, GroupLayout.PREFERRED_SIZE)
                                                .addGroup(pLOLCLayout.createParallelGroup(GroupLayout.Alignment.TRAILING)
                                                    .addComponent(jLabel42, GroupLayout.Alignment.LEADING, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                                    .addComponent(jLabel43, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)))
                                            .addGap(42, 42, 42)
                                            .addGroup(pLOLCLayout.createParallelGroup()
                                                .addComponent(tbLOLCrefpass, GroupLayout.Alignment.TRAILING, GroupLayout.PREFERRED_SIZE, 270, GroupLayout.PREFERRED_SIZE)
                                                .addComponent(tbLOLCrefuser, GroupLayout.Alignment.TRAILING, GroupLayout.PREFERRED_SIZE, 270, GroupLayout.PREFERRED_SIZE)
                                                .addComponent(tbLOLCrefurl, GroupLayout.PREFERRED_SIZE, 270, GroupLayout.PREFERRED_SIZE)
                                                .addComponent(tbLOLCrefdb, GroupLayout.PREFERRED_SIZE, 270, GroupLayout.PREFERRED_SIZE)
                                                .addComponent(tbLOLCpass, GroupLayout.Alignment.TRAILING, GroupLayout.PREFERRED_SIZE, 270, GroupLayout.PREFERRED_SIZE)))
                                        .addGroup(pLOLCLayout.createSequentialGroup()
                                            .addGap(0, 0, Short.MAX_VALUE)
                                            .addComponent(tbLOLCuser, GroupLayout.PREFERRED_SIZE, 270, GroupLayout.PREFERRED_SIZE))
                                        .addGroup(pLOLCLayout.createSequentialGroup()
                                            .addGroup(pLOLCLayout.createParallelGroup()
                                                .addGroup(pLOLCLayout.createSequentialGroup()
                                                    .addComponent(jLabel44, GroupLayout.PREFERRED_SIZE, 185, GroupLayout.PREFERRED_SIZE)
                                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                                                .addGroup(pLOLCLayout.createSequentialGroup()
                                                    .addComponent(jLabel4, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                                    .addGap(77, 77, 77)))
                                            .addGroup(pLOLCLayout.createParallelGroup(GroupLayout.Alignment.LEADING, false)
                                                .addComponent(tbLOLCurl, GroupLayout.PREFERRED_SIZE, 270, GroupLayout.PREFERRED_SIZE)
                                                .addComponent(tbLOLCSourceId, GroupLayout.PREFERRED_SIZE, 270, GroupLayout.PREFERRED_SIZE))))
                                    .addGap(20, 20, 20))
                                .addComponent(jLabel51, GroupLayout.Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                .addGroup(pLOLCLayout.createSequentialGroup()
                                    .addGroup(pLOLCLayout.createParallelGroup(GroupLayout.Alignment.LEADING, false)
                                        .addComponent(cbCdoPostTasks, GroupLayout.PREFERRED_SIZE, 160, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(cbCdoDaysSinceBegin, GroupLayout.DEFAULT_SIZE, 200, Short.MAX_VALUE)
                                        .addComponent(cbCdoFamilynames, GroupLayout.Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                        .addComponent(cbCdoLocations, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                        .addComponent(cbCdoStatusSex, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                        .addComponent(cbCdoRegType, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                        .addComponent(cbCdoDates, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                        .addComponent(cbCdoMinMaxMarriage, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                        .addComponent(cbCdoOccupation, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                        .addComponent(cbCdoPartsToFullDate, GroupLayout.DEFAULT_SIZE, 200, Short.MAX_VALUE)
                                        .addComponent(cbCdoPrepieceSuffix)
                                        .addComponent(cbCdoRefreshData, GroupLayout.DEFAULT_SIZE, 200, Short.MAX_VALUE)
                                        .addComponent(cbCdoFirstnames, GroupLayout.DEFAULT_SIZE, 200, Short.MAX_VALUE))
                                    .addGap(49, 49, 49)
                                    .addComponent(cbCdoRemoveDuplicates, GroupLayout.PREFERRED_SIZE, 162, GroupLayout.PREFERRED_SIZE)
                                    .addGap(0, 0, Short.MAX_VALUE)))
                            .addGroup(pLOLCLayout.createParallelGroup()
                                .addGroup(pLOLCLayout.createSequentialGroup()
                                    .addGap(6, 6, 6)
                                    .addComponent(jLabel45))
                                .addGroup(pLOLCLayout.createSequentialGroup()
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addGroup(pLOLCLayout.createParallelGroup(GroupLayout.Alignment.LEADING, false)
                                        .addComponent(jScrollPane6, GroupLayout.DEFAULT_SIZE, 578, Short.MAX_VALUE)
                                        .addComponent(tbLOLClatestOutput, GroupLayout.DEFAULT_SIZE, 578, Short.MAX_VALUE))))
                            .addGap(15, 15, 15))
                );
                pLOLCLayout.setVerticalGroup(
                    pLOLCLayout.createParallelGroup()
                        .addGroup(pLOLCLayout.createSequentialGroup()
                            .addContainerGap()
                            .addGroup(pLOLCLayout.createParallelGroup()
                                .addGroup(pLOLCLayout.createSequentialGroup()
                                    .addGap(1, 1, 1)
                                    .addComponent(jLabel45))
                                .addComponent(tbLOLCSourceId, GroupLayout.PREFERRED_SIZE, 19, GroupLayout.PREFERRED_SIZE)
                                .addComponent(jLabel44))
                            .addGroup(pLOLCLayout.createParallelGroup(GroupLayout.Alignment.LEADING, false)
                                .addGroup(GroupLayout.Alignment.TRAILING, pLOLCLayout.createSequentialGroup()
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addComponent(tbLOLClatestOutput, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                    .addComponent(jScrollPane6, GroupLayout.PREFERRED_SIZE, 568, GroupLayout.PREFERRED_SIZE))
                                .addGroup(pLOLCLayout.createSequentialGroup()
                                    .addGroup(pLOLCLayout.createParallelGroup()
                                        .addGroup(pLOLCLayout.createSequentialGroup()
                                            .addGap(14, 14, 14)
                                            .addComponent(jLabel4)
                                            .addGap(8, 8, 8))
                                        .addGroup(GroupLayout.Alignment.TRAILING, pLOLCLayout.createSequentialGroup()
                                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                            .addComponent(tbLOLCurl, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)))
                                    .addGroup(pLOLCLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(jLabel42)
                                        .addComponent(tbLOLCuser, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addGroup(pLOLCLayout.createParallelGroup()
                                        .addComponent(jLabel43, GroupLayout.PREFERRED_SIZE, 17, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(tbLOLCpass, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addGroup(pLOLCLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(label1)
                                        .addComponent(tbLOLCrefurl, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addGroup(pLOLCLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(label2)
                                        .addComponent(tbLOLCrefuser, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addGroup(pLOLCLayout.createParallelGroup()
                                        .addComponent(label3)
                                        .addComponent(tbLOLCrefpass, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                    .addGroup(pLOLCLayout.createParallelGroup()
                                        .addComponent(label4)
                                        .addComponent(tbLOLCrefdb, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                                    .addGap(18, 18, 18)
                                    .addComponent(jLabel51)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addComponent(cbCdoRefreshData)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addComponent(cbCdoPrepieceSuffix)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addComponent(cbCdoFirstnames)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addComponent(cbCdoFamilynames)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addComponent(cbCdoLocations)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addComponent(cbCdoStatusSex)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addComponent(cbCdoRegType)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addComponent(cbCdoOccupation)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addComponent(cbCdoDates)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addComponent(cbCdoMinMaxMarriage)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addGroup(pLOLCLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(cbCdoPartsToFullDate)
                                        .addComponent(cbCdoRemoveDuplicates))
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addComponent(cbCdoDaysSinceBegin)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                                    .addComponent(cbCdoPostTasks)
                                    .addGap(8, 8, 8)
                                    .addComponent(btnStartProcessClean)))
                            .addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                );
            }
            tpMain.addTab("CLEAN", pLOLC);

            //======== pPrematch ========
            {
                pPrematch.setName("jPanel1");

                //---- cbPdoFrequencyTables ----
                cbPdoFrequencyTables.setText("Frequency Tables");
                cbPdoFrequencyTables.setName("cbPdoUniqueNameTables");

                //---- cbPdoLevenshtein ----
                cbPdoLevenshtein.setText("Levenshtein");
                cbPdoLevenshtein.setName("cbPdoLevenshtein");

                //---- cbPdoCreateBaseTable ----
                cbPdoCreateBaseTable.setText("Base Table");
                cbPdoCreateBaseTable.setName("cbPdoCreateBaseTable");
                cbPdoCreateBaseTable.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        cbPdoCreateBaseTableActionPerformed(e);
                    }
                });

                //---- cbPdoSplitFirstnames ----
                cbPdoSplitFirstnames.setText("Split Firstnames");
                cbPdoSplitFirstnames.setName("cbPdoSplitName");

                //---- btnStartProcessPrematch ----
                btnStartProcessPrematch.setText("Start PreMatching");
                btnStartProcessPrematch.setName("bnPstartProcess");
                btnStartProcessPrematch.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        bnPstartProcessActionPerformed(e);
                    }
                });

                //======== jScrollPane7 ========
                {
                    jScrollPane7.setName("jScrollPane7");

                    //---- taPresult ----
                    taPresult.setColumns(20);
                    taPresult.setRows(5);
                    taPresult.setName("taPresult");
                    jScrollPane7.setViewportView(taPresult);
                }

                //---- cbPdoNamesToNos ----
                cbPdoNamesToNos.setText("Names to Numbers");
                cbPdoNamesToNos.setName("cbPdoNameToNo");

                //---- taPinfo ----
                taPinfo.setName("taPinfo");

                //---- cbPdoStandardization ----
                cbPdoStandardization.setText("Automatic Standardization");
                cbPdoStandardization.setName("cbPdoStandardization");

                GroupLayout pPrematchLayout = new GroupLayout(pPrematch);
                pPrematch.setLayout(pPrematchLayout);
                pPrematchLayout.setHorizontalGroup(
                    pPrematchLayout.createParallelGroup()
                        .addGroup(pPrematchLayout.createSequentialGroup()
                            .addContainerGap()
                            .addGroup(pPrematchLayout.createParallelGroup()
                                .addComponent(jScrollPane7, GroupLayout.DEFAULT_SIZE, 1097, Short.MAX_VALUE)
                                .addComponent(taPinfo, GroupLayout.Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, 1097, Short.MAX_VALUE)
                                .addGroup(pPrematchLayout.createSequentialGroup()
                                    .addGroup(pPrematchLayout.createParallelGroup()
                                        .addComponent(cbPdoLevenshtein, GroupLayout.PREFERRED_SIZE, 261, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(cbPdoNamesToNos, GroupLayout.PREFERRED_SIZE, 261, GroupLayout.PREFERRED_SIZE)
                                        .addGroup(pPrematchLayout.createParallelGroup(GroupLayout.Alignment.LEADING, false)
                                            .addComponent(cbPdoSplitFirstnames, GroupLayout.DEFAULT_SIZE, 261, Short.MAX_VALUE)
                                            .addComponent(cbPdoFrequencyTables, GroupLayout.DEFAULT_SIZE, 261, Short.MAX_VALUE)
                                            .addComponent(cbPdoStandardization, GroupLayout.Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, 261, Short.MAX_VALUE)))
                                    .addGap(0, 0, Short.MAX_VALUE))
                                .addGroup(pPrematchLayout.createSequentialGroup()
                                    .addComponent(cbPdoCreateBaseTable, GroupLayout.PREFERRED_SIZE, 261, GroupLayout.PREFERRED_SIZE)
                                    .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED, 641, Short.MAX_VALUE)
                                    .addComponent(btnStartProcessPrematch, GroupLayout.PREFERRED_SIZE, 195, GroupLayout.PREFERRED_SIZE)))
                            .addContainerGap())
                );
                pPrematchLayout.setVerticalGroup(
                    pPrematchLayout.createParallelGroup()
                        .addGroup(pPrematchLayout.createSequentialGroup()
                            .addContainerGap()
                            .addComponent(cbPdoSplitFirstnames)
                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                            .addComponent(cbPdoFrequencyTables)
                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                            .addComponent(cbPdoStandardization)
                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                            .addComponent(cbPdoLevenshtein)
                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                            .addComponent(cbPdoNamesToNos)
                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                            .addGroup(pPrematchLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                .addComponent(cbPdoCreateBaseTable)
                                .addComponent(btnStartProcessPrematch))
                            .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED, 14, Short.MAX_VALUE)
                            .addComponent(taPinfo, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                            .addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED)
                            .addComponent(jScrollPane7, GroupLayout.PREFERRED_SIZE, 437, GroupLayout.PREFERRED_SIZE)
                            .addContainerGap())
                );
            }
            tpMain.addTab("PREMATCH", pPrematch);

            //======== pMatch ========
            {
                pMatch.setName("pMatch");

                //======== scrollPane1 ========
                {
                    scrollPane1.setName("scrollPane1");

                    //---- textArea1 ----
                    textArea1.setName("textArea1");
                    scrollPane1.setViewportView(textArea1);
                }

                //---- btnStartProcessMatch ----
                btnStartProcessMatch.setText("Start Matching");
                btnStartProcessMatch.setName("btnStartProcessMatch");
                btnStartProcessMatch.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        btnStartProcessMatchActionPerformed(e);
                    }
                });

                GroupLayout pMatchLayout = new GroupLayout(pMatch);
                pMatch.setLayout(pMatchLayout);
                pMatchLayout.setHorizontalGroup(
                    pMatchLayout.createParallelGroup()
                        .addGroup(pMatchLayout.createSequentialGroup()
                            .addGap(34, 34, 34)
                            .addGroup(pMatchLayout.createParallelGroup(GroupLayout.Alignment.LEADING, false)
                                .addComponent(scrollPane1, GroupLayout.DEFAULT_SIZE, 1057, Short.MAX_VALUE)
                                .addComponent(btnStartProcessMatch, GroupLayout.DEFAULT_SIZE, 1057, Short.MAX_VALUE))
                            .addContainerGap(30, Short.MAX_VALUE))
                );
                pMatchLayout.setVerticalGroup(
                    pMatchLayout.createParallelGroup()
                        .addGroup(pMatchLayout.createSequentialGroup()
                            .addGap(26, 26, 26)
                            .addComponent(scrollPane1, GroupLayout.PREFERRED_SIZE, 545, GroupLayout.PREFERRED_SIZE)
                            .addGap(18, 18, 18)
                            .addComponent(btnStartProcessMatch)
                            .addContainerGap(30, Short.MAX_VALUE))
                );
            }
            tpMain.addTab("MATCH", pMatch);
        }

        GroupLayout contentPaneLayout = new GroupLayout(contentPane);
        contentPane.setLayout(contentPaneLayout);
        contentPaneLayout.setHorizontalGroup(
            contentPaneLayout.createParallelGroup()
                .addGroup(contentPaneLayout.createSequentialGroup()
                    .addContainerGap()
                    .addComponent(tpMain, GroupLayout.PREFERRED_SIZE, 1126, GroupLayout.PREFERRED_SIZE)
                    .addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        contentPaneLayout.setVerticalGroup(
            contentPaneLayout.createParallelGroup()
                .addGroup(contentPaneLayout.createSequentialGroup()
                    .addComponent(tpMain, GroupLayout.PREFERRED_SIZE, 671, GroupLayout.PREFERRED_SIZE)
                    .addGap(0, 0, Short.MAX_VALUE))
        );
        pack();
        setLocationRelativeTo(getOwner());

        //---- buttonGroup1 ----
        ButtonGroup buttonGroup1 = new ButtonGroup();
        buttonGroup1.add(rbBebiIndexGeen);
        buttonGroup1.add(rbBebiIndexGenlias);
    }// </editor-fold>//GEN-END:initComponents

    private void createDomProject() {
        doc = modulemain.LinksSpecific.createDomDocument();
        Element element = doc.createElement("projects");
        doc.appendChild(element);
    }

    /**
     * Set Look and feel
     */
    private void setLookAndFeel() {
        // Look and Feel Property String
        String lafString = "com.sun.java.swing.plaf.windows.WindowsLookAndFeel";
        if (LinksSpecific.isWindows()) {
            try {
                UIManager.setLookAndFeel(lafString);
            } catch (Exception e) {
                // TODO: Write to system log
            }
        }
    }


    /**
     * Fill GUI with the read properties
     */
    public void loadProperties( PrintLogger plog, Properties properties )
    {
        String sourceId = properties.getProperty( "tbLOLCSourceId" );
        if( sourceId == null ) { sourceId = ""; }
        if( sourceId.isEmpty() ) { System.out.println( "sourceId not specified" ); }
        else { System.out.println( "sourceId: " + sourceId ); }
        tbLOLCSourceId.setText( sourceId );

        String ref_url  = properties.getProperty( "mysql_hsnref_hosturl" );
        String ref_user = properties.getProperty( "mysql_hsnref_username" );
        String ref_pass = properties.getProperty( "mysql_hsnref_password" );
        String ref_db   = properties.getProperty( "mysql_hsnref_dbname" );

        // copy values into form, tab Cleaned
        tbLOLCrefurl.setText(  ref_url );
        tbLOLCrefuser.setText( ref_user );
        tbLOLCrefpass.setText( ref_pass );
        tbLOLCrefdb.setText(   ref_db );

        System.out.println( "mysql_hsnref_hosturl:\t"  + ref_url );
        System.out.println( "mysql_hsnref_username:\t" + ref_user );
        System.out.println( "mysql_hsnref_password:\t" + ref_pass );
        System.out.println( "mysql_hsnref_dbname:\t"   + ref_db );

        String url  = properties.getProperty( "mysql_links_hosturl" );
        String user = properties.getProperty( "mysql_links_username" );
        String pass = properties.getProperty( "mysql_links_password" );

        // copy values into form, tab Cleaned
        tbLOLCurl.setText(  url );
        tbLOLCuser.setText( user );
        tbLOLCpass.setText( pass );

        System.out.println( "mysql_links_hosturl:\t"  + url );
        System.out.println( "mysql_links_username:\t" + user );
        System.out.println( "mysql_links_password:\t" + pass );

        // Remove previous data ?
        String doRefreshData = properties.getProperty( "doRefreshData" );
        if( doRefreshData != null ) {
            if( doRefreshData.equals( "true" ) ) { cbCdoRefreshData.setSelected( true ); }
            else { cbCdoRefreshData.setSelected( false ); }
        }

        // PrepieceSuffix
        String doPrepieceSuffix = properties.getProperty( "doPrepieceSuffix" );
        if( doPrepieceSuffix != null ) {
            if( doPrepieceSuffix.equals( "true" ) ) { cbCdoPrepieceSuffix.setSelected( true ); }
            else { cbCdoPrepieceSuffix.setSelected( false ); }
        }

        // Firstnames
        String doFirstnames = properties.getProperty( "doFirstnames" );
        if( doFirstnames != null ) {
            if( doFirstnames.equals( "true" ) ) { cbCdoFirstnames.setSelected( true ); }
            else { cbCdoFirstnames.setSelected( false ); }
        }

        // Familynames
        String doFamilynames = properties.getProperty( "doFamilynames" );
        if( doFamilynames != null ) {
            if( doFamilynames.equals( "true" ) ) { cbCdoFamilynames.setSelected( true ); }
            else { cbCdoFamilynames.setSelected( false ); }
        }

        // Locations
        String doLocations = properties.getProperty( "doLocations" );
        if( doLocations != null ) {
            if( doLocations.equals( "true" ) ) { cbCdoLocations.setSelected( true ); }
            else { cbCdoLocations.setSelected( false ); }
        }

        // Status and sex
        String doStatusSex = properties.getProperty( "doStatusSex" );
        if( doStatusSex != null ) {
            if( doStatusSex.equals( "true" ) ) { cbCdoStatusSex.setSelected( true ); }
            else { cbCdoStatusSex.setSelected( false ); }
        }

        // Type
        String doRegType = properties.getProperty( "doRegType" );
        if( doRegType != null ) {
            if( doRegType.equals( "true" ) ) { cbCdoRegType.setSelected( true ); }
            else { cbCdoRegType.setSelected( false ); }
        }

        // Relation
        /*
        String doRelation = properties.getProperty( "doRelation" );
        if( doRelation != null ) {
            if( doRelation.equals( "true" ) ) { cbCdoRelation.setSelected( true ); }
            else { cbCdoRelation.setSelected( false ); }
        }
        */

        // Age
        /*
        String doAge = properties.getProperty( "doAge" );
        if( doAge != null ) {
            if( doAge.equals( "true" ) ) { cbCdoAge.setSelected( true ); }
            else { cbCdoAge.setSelected( false ); }
        }
        */

        // Role
        /*
        String doRole = properties.getProperty( "doRole" );
        if( doRole != null ) {
            if( doRole.equals( "true" ) ) { cbLOLCdoRole.setSelected( true ); }
            else { cbLOLCdoRole.setSelected( false ); }
        }
        */

        // Occupation
        String doOccupation = properties.getProperty( "doOccupation" );
        if( doOccupation != null ) {
            if( doOccupation.equals( "true" ) ) { cbCdoOccupation.setSelected( true ); }
            else { cbCdoOccupation.setSelected( false ); }
        }

        // Dates
        String doDates = properties.getProperty( "doDates" );
        if( doDates != null ) {
            if( doDates.equals( "true" ) ) { cbCdoDates.setSelected( true ); }
            else { cbCdoDates.setSelected( false ); }
        }

        // Min Max Marriage
        String doMinMaxMarriage = properties.getProperty( "doMinMaxMarriage" );
        if( doMinMaxMarriage != null ) {
            if( doMinMaxMarriage.equals( "true" ) ) { cbCdoMinMaxMarriage.setSelected( true ); }
            else { cbCdoMinMaxMarriage.setSelected( false ); }
        }

        // Parts to Full Date
        String doPartsToFullDate = properties.getProperty( "doPartsToFullDate" );
        if( doPartsToFullDate != null ) {
            if( doPartsToFullDate.equals( "true" ) ) { cbCdoPartsToFullDate.setSelected( true ); }
            else { cbCdoPartsToFullDate.setSelected( false ); }
        }

        // Days since begin
        String doDaysSinceBegin = properties.getProperty( "doDaysSinceBegin" );
        if( doDaysSinceBegin != null ) {
            if( doDaysSinceBegin.equals( "true" ) ) { cbCdoDaysSinceBegin.setSelected( true ); }
            else { cbCdoDaysSinceBegin.setSelected( false ); }
        }

        // Post Tasks
        String doPostTasks = properties.getProperty( "doPostTasks" );
        if( doPostTasks != null ) {
            if( doPostTasks.equals( "true" ) ) { cbCdoPostTasks.setSelected( true ); }
            else { cbCdoPostTasks.setSelected( false ); }
        }

        // Remove Duplicates
        String doRemoveDuplicates = properties.getProperty( "doRemoveDuplicates" );
        if( doRemoveDuplicates != null ) {
            if( doRemoveDuplicates.equals( "true" ) ) { cbCdoRemoveDuplicates.setSelected( true ); }
            else { cbCdoRemoveDuplicates.setSelected( false ); }
        }

        // debug flags
        dbgRenewData        = properties.getProperty( "dbgRenewData" );
        dbgPrepieceSuffix   = properties.getProperty( "dbgPrepieceSuffix" );
        dbgFirstnames       = properties.getProperty( "dbgFirstnames" );
        dbgFamilynames      = properties.getProperty( "dbgFamilynames" );
        dbgLocations        = properties.getProperty( "dbgLocations" );
        dbgStatusSex        = properties.getProperty( "dbgStatusSex" );
        dbgRegType          = properties.getProperty( "dbgRegType" );
        dbgOccupation       = properties.getProperty( "dbgOccupation" );
        dbgAge              = properties.getProperty( "dbgAge" );
        dbgRole             = properties.getProperty( "dbgRole" );
        dbgDates            = properties.getProperty( "dbgDates" );
        dbgMinMaxMarriage   = properties.getProperty( "dbgMinMaxMarriage" );
        dbgPartsToFullDate  = properties.getProperty( "dbgPartsToFullDate" );
        dbgDaysSinceBegin   = properties.getProperty( "dbgDaysSinceBegin" );
        dbgPostTasks        = properties.getProperty( "dbgPostTasks" );
        dbgRemoveDuplicates = properties.getProperty( "dbgRemoveDuplicates" );

        dbgPrematch         = properties.getProperty( "dbgPrematch" );
        dbgMatch            = properties.getProperty( "dbgMatch" );


        String doSplitFirstnames = properties.getProperty( "doSplitFirstnames" );
        if( doSplitFirstnames != null ) {
            if( doSplitFirstnames.equals( "true" ) ) { cbPdoSplitFirstnames.setSelected( true ); }
            else { cbPdoSplitFirstnames.setSelected( false ); }
        }

        String doFrequencyTables = properties.getProperty( "doFrequencyTables" );
        if( doFrequencyTables != null ) {
            if( doFrequencyTables.equals( "true" ) ) { cbPdoFrequencyTables.setSelected( true ); }
            else { cbPdoFrequencyTables.setSelected( false ); }
        }

        String doStandardization = properties.getProperty( "doStandardization" );
        if( doStandardization != null ) {
            if( doStandardization.equals( "true" ) ) { cbPdoStandardization.setSelected( true ); }
            else { cbPdoStandardization.setSelected( false ); }
        }

        String doLevenshtein = properties.getProperty( "doLevenshtein" );
        if( doLevenshtein != null ) {
            if( doLevenshtein.equals( "true" ) ) { cbPdoLevenshtein.setSelected( true ); }
            else { cbPdoLevenshtein.setSelected( false ); }
        }

        String doNamesToNos = properties.getProperty( "doNamesToNos" );
       if( doNamesToNos != null ) {
            if( doNamesToNos.equals( "true" ) ) { cbPdoNamesToNos.setSelected( true ); }
            else { cbPdoNamesToNos.setSelected( false ); }
        }

        String doCreateBaseTable = properties.getProperty( "doCreateBaseTable" );
        if( doCreateBaseTable != null ) {
            if( doCreateBaseTable.equals( "true" ) ) { cbPdoCreateBaseTable.setSelected( true ); }
            else { cbPdoCreateBaseTable.setSelected( false ); }
        }

    } // loadProperties


    // all selectors
    private void openMenuItemActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_openMenuItemActionPerformed
        try {
            doc = LinksSpecific.LoadXmlFromfile(LinksSpecific.OpenFile("xml", false));
        } catch (Exception e) {
        }

        ArrayList<String> al = new ArrayList<String>();
        // Dit is ene herhaling
        for (int i = 0; i < doc.getDocumentElement().getElementsByTagName("project").getLength(); i++) {
            Element els = (Element) doc.getDocumentElement().getChildNodes().item(i);
            al.add(els.getAttribute("name"));
        }

        try {
            lMAINProjects.setListData((String[]) al.toArray());
        } catch (Exception e) {
        }
    }//GEN-LAST:event_openMenuItemActionPerformed


    private void exitMenuItemActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_exitMenuItemActionPerformed
        System.exit(0);
    }//GEN-LAST:event_exitMenuItemActionPerformed


    private void saveMenuItemActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_saveMenuItemActionPerformed
        String selectedPath = LinksSpecific.SelectFileWithCheck("xml");
        LinksSpecific.writeXmlToFile(doc, selectedPath);
    }//GEN-LAST:event_saveMenuItemActionPerformed


    private void tfBILOProjectNameKeyReleased(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_tfBILOProjectNameKeyReleased
    }//GEN-LAST:event_tfBILOProjectNameKeyReleased


    private void bBronInternToLinksOrigineelActionPerformed(java.awt.event.ActionEvent evt)
    {//GEN-FIRST:event_bBronInternToLinksOrigineelActionPerformed
        LinksOriginal bilo;
        boolean doCleaned = false;

        if (this.cbOrunCleaned.isSelected()) {
            doCleaned = true;
        }

        bilo = new LinksOriginal(tfBILODbLocation.getText(),
                tfBILODbUser.getText(),
                tfBILODbPass.getText(),
                tfBILOProjectName.getText(),
                tfBILOLatestOutput,
                taBILOOutput,
                Integer.parseInt(tfBILOdelay.getText()),
                tfBILODeleteSource.getText(),
                this,
                doCleaned);
        bilo.start();
    }//GEN-LAST:event_bBronInternToLinksOrigineelActionPerformed


    private void bCsvToDbStartActionPerformed(java.awt.event.ActionEvent evt)
    {//GEN-FIRST:event_bCsvToDbStartActionPerformed
        IndexType it;

        //Type indexen bepalen
        if (rbBebiIndexGenlias.isSelected()) {
            it = IndexType.GENLIAS;
        } else {
            it = IndexType.NONE;
        }

        LinksInternal ctdb;

        ctdb = new LinksInternal(tfCTBCsvFiles.getText().split(","),
                tfCTDDbLocation.getText(),
                tfCTDDbName.getText(),
                tfCTDDbUser.getText(),
                tfCTDDbPass.getText(),
                tfCTDPreFix.getText(),
                tfCTDLatestOutput,
                tfCTDLineNumber,
                taCTDOutput,
                pbDTCTotal,
                pbDTCFile,
                Integer.parseInt(tfCTDdelay.getText()),
                it);
        ctdb.start();
    }//GEN-LAST:event_bCsvToDbStartActionPerformed


    private void bSelectCsvToDbCsvFileActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bSelectCsvToDbCsvFileActionPerformed
        String files = LinksSpecific.OpenFile("csv", true);
        tfCTBCsvFiles.setText(files);
    }//GEN-LAST:event_bSelectCsvToDbCsvFileActionPerformed


    private void bCSVBeginFilterActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bCSVBeginFilterActionPerformed
        // controle, moet herbruikbare methode worden
        if (tfCSVCsvFile.getText().isEmpty()
                || tfCSVOutputFile.getText().isEmpty()
                || tfCSVFilterText.getText().isEmpty()
                || tfCSVFilterIndex.getText().isEmpty()) {
            javax.swing.JOptionPane.showMessageDialog(this, "One or more required fields are empty!");
        }

        CsvConnector tcr;
        tcr = new CsvConnector(tfCSVCsvFile.getText(),
                tfCSVOutputFile.getText(),
                tfCSVFilterText.getText(),
                Integer.valueOf(tfCSVFilterIndex.getText()),
                tfCSVInfo,
                tfCSVLineData,
                taCSVOutput);
        tcr.start();
    }//GEN-LAST:event_bCSVBeginFilterActionPerformed


    private void bCSVPathOutpuFileActionPerformed(java.awt.event.ActionEvent evt)
    {//GEN-FIRST:event_bCSVPathOutpuFileActionPerformed
        String selectedPath = LinksSpecific.SelectFileWithCheck("csv");
        tfCSVOutputFile.setText(selectedPath);
    }//GEN-LAST:event_bCSVPathOutpuFileActionPerformed


    private void bCSVGetHeaderActionPerformed(java.awt.event.ActionEvent evt)
    {//GEN-FIRST:event_bCSVGetHeaderActionPerformed

        CsvConnector ic = new CsvConnector();
        String[] headerVelden = ic.importHeader(tfCSVCsvFile.getText());

        headerVelden = ic.importHeader(tfCSVCsvFile.getText());

        for (int i = 0; i < headerVelden.length; i++) {
            taCSVOutput.append(headerVelden[i] + "   ");

        }
        taCSVOutput.append("\r\n");
    }//GEN-LAST:event_bCSVGetHeaderActionPerformed


    private void bCSVPathActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bCSVPathActionPerformed
        String path = LinksSpecific.OpenFile("csv", false);
        tfCSVCsvFile.setText(path);
    }//GEN-LAST:event_bCSVPathActionPerformed


    private void lMAINProjectsValueChanged(javax.swing.event.ListSelectionEvent evt)
    {//GEN-FIRST:event_lMAINProjectsValueChanged

        Element el = (Element) doc.getDocumentElement().getChildNodes().item(lMAINProjects.getSelectedIndex());

        tMAINProjectName.setText(el.getAttribute("name"));
        tMAINDbLocation.setText(el.getAttribute("url"));
        tMAINUser.setText(el.getAttribute("user"));
        tMAINPass.setText(el.getAttribute("pass"));
        taMAINOpmerkingen.setText(el.getAttribute("comment"));
    }//GEN-LAST:event_lMAINProjectsValueChanged


    private void bMAINSaveActionPerformed(java.awt.event.ActionEvent evt)
    {//GEN-FIRST:event_bMAINSaveActionPerformed

        //Node n = doc.getDocumentElement();
        Element el = doc.getDocumentElement();

        Element element = doc.createElement("project");

        element.setAttribute("name", tMAINProjectName.getText());
        element.setAttribute("url", tMAINDbLocation.getText());
        element.setAttribute("user", tMAINUser.getText());
        element.setAttribute("pass", tMAINPass.getText());
        element.setAttribute("comment", taMAINOpmerkingen.getText());

        el.removeChild(doc.getDocumentElement().getChildNodes().item(lMAINProjects.getSelectedIndex()));
        el.appendChild(element);

        // herhaling
        ArrayList<String> al = new ArrayList<String>();
        // Dit is ene herhaling
        for (int i = 0; i < doc.getDocumentElement().getElementsByTagName("project").getLength(); i++) {
            Element els = (Element) doc.getDocumentElement().getChildNodes().item(i);
            al.add(els.getAttribute("name"));
        }

        try {
            lMAINProjects.setListData((String[]) al.toArray());
        } catch (Exception e) {
        }
        lMAINProjects.setSelectedIndex(al.size() - 1);
    }//GEN-LAST:event_bMAINSaveActionPerformed


    private void bMAINDeleteActionPerformed(java.awt.event.ActionEvent evt)
    {//GEN-FIRST:event_bMAINDeleteActionPerformed

        Element el = doc.getDocumentElement();
        el.removeChild(doc.getDocumentElement().getChildNodes().item(lMAINProjects.getSelectedIndex()));

        // herhaling
        ArrayList<String> al = new ArrayList<String>();
        // Dit is ene herhaling
        for (int i = 0; i < doc.getDocumentElement().getElementsByTagName("project").getLength(); i++) {
            Element els = (Element) doc.getDocumentElement().getChildNodes().item(i);
            al.add(els.getAttribute("name"));
        }

        try {
            lMAINProjects.setListData((String[]) al.toArray());
        } catch (Exception e) {
        }
    }//GEN-LAST:event_bMAINDeleteActionPerformed


    private void bMAINAddActionPerformed(java.awt.event.ActionEvent evt)
    {//GEN-FIRST:event_bMAINAddActionPerformed
        Element projects = doc.getDocumentElement();

        Element element = doc.createElement("project");
        element.setAttribute("name", "New projects");
        element.setAttribute("url", "");
        element.setAttribute("user", "");
        element.setAttribute("pass", "");
        element.setAttribute("comment", "");
        projects.appendChild(element);

        ArrayList<String> al = new ArrayList<String>();

        for (int i = 0; i < doc.getDocumentElement().getElementsByTagName("project").getLength(); i++) {
            Element el = (Element) doc.getDocumentElement().getChildNodes().item(i);
            al.add(el.getAttribute("name"));
        }

        try {
            lMAINProjects.setListData((String[]) al.toArray());
        } catch (Exception e) {
        }

        lMAINProjects.setSelectedIndex(al.size() - 1);
    }//GEN-LAST:event_bMAINAddActionPerformed


    private void bMAINActivateActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bMAINActivateActionPerformed
        if (cbMAINbebi.isSelected()) {
            tfCTDPreFix.setText(tMAINProjectName.getText());
            tfCTDDbLocation.setText(tMAINDbLocation.getText());
            tfCTDDbUser.setText(tMAINUser.getText());
            tfCTDDbPass.setText(tMAINPass.getText());
        }
        if (cbMAINbilo.isSelected()) {
            tfBILOProjectName.setText(tMAINProjectName.getText());
            tfBILODbLocation.setText(tMAINDbLocation.getText());
            tfBILODbUser.setText(tMAINUser.getText());
            tfBILODbPass.setText(tMAINPass.getText());
        }
        if (cbMAINlolc.isSelected()) {
        }
    }//GEN-LAST:event_bMAINActivateActionPerformed


    private void bMAINClearActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bMAINClearActionPerformed
        cbMAINbebi.setSelected(false);
        cbMAINbilo.setSelected(false);
        tMAINProjectName.setText("");
        tMAINDbLocation.setText("");
        tMAINUser.setText("");
        tMAINPass.setText("");
        taMAINOpmerkingen.setText("");
    }//GEN-LAST:event_bMAINClearActionPerformed


    public void fireCleaned() {

        bnLOLCstartProcessActionPerformed( null );

    }

    private Options setOptions()
    {
        Options opts = new Options();


        if( cbCdoRefreshData.isSelected() ) { opts.setDoRenewData( true ); }
        else { opts.setDoRenewData( false ); }

        if( cbCdoPrepieceSuffix.isSelected() ) { opts.setDoPrepieceSuffix( true ); }
        else { opts.setDoPrepieceSuffix( false ); }

        if( cbCdoFirstnames.isSelected() ) { opts.setDoFirstnames( true ); }
        else { opts.setDoFirstnames( false ); }

        if( cbCdoFamilynames.isSelected() ) { opts.setDoFamilynames( true ); }
        else { opts.setDoFamilynames( false ); }

        if( cbCdoLocations.isSelected() ) { opts.setDoLocations( true ); }
        else { opts.setDoLocations( false ); }

        if( cbCdoStatusSex.isSelected() ) { opts.setDoStatusSex( true ); }
        else { opts.setDoStatusSex( false ); }

        if( cbCdoRegType.isSelected() ) { opts.setDoRegType( true ); }
        else { opts.setDoRegType( false ); }

        if( cbCdoOccupation.isSelected() ) { opts.setDoOccupation( true ); }
        else { opts.setDoOccupation( false ); }

        if( cbCdoDates.isSelected() ) { opts.setDoDates( true ); }
        else { opts.setDoDates( false ); }

        if( cbCdoMinMaxMarriage.isSelected() ) { opts.setDoMinMaxMarriage( true ); }
        else { opts.setDoMinMaxMarriage( false ); }

        if( cbCdoPartsToFullDate.isSelected() ) { opts.setDoPartsToFullDate( true ); }
        else { opts.setDoPartsToFullDate( false );  }

        if( cbCdoDaysSinceBegin.isSelected() ) { opts.setDoDaysSinceBegin( true ); }
        else { opts.setDoDaysSinceBegin( false ); }

        if( cbCdoPostTasks.isSelected() ) { opts.setDoPostTasks( true ); }
        else { opts.setDoPostTasks( false ); }

        if( cbCdoRemoveDuplicates.isSelected() ) { opts.setDoRemoveDuplicates( true ); }
        else { opts.setDoRemoveDuplicates( false ); }


        if( dbgRenewData != null ) {
            if( dbgRenewData.equals( "true" ) ) { opts.setDbgRenewData( true ); }
            else { opts.setDbgRenewData( false ); }
        }

        if( dbgPrepieceSuffix != null ) {
            if( dbgPrepieceSuffix.equals( "true" ) ) { opts.setDbgPrepieceSuffix( true ); }
            else { opts.setDbgPrepieceSuffix( false ); }
        }

        if( dbgFirstnames != null ) {
            if( dbgFirstnames.equals( "true" ) ) { opts.setDbgFirstnames( true ); }
            else { opts.setDbgFirstnames( false ); }
        }

        if( dbgFamilynames != null ) {
            if( dbgFamilynames.equals( "true" ) ) { opts.setDbgFamilynames( true ); }
            else { opts.setDbgFamilynames( false ); }
        }

        if( dbgLocations != null ) {
            if( dbgLocations.equals( "true" ) ) { opts.setDbgLocations( true ); }
            else { opts.setDbgLocations( false ); }
        }

        if( dbgStatusSex != null ) {
            if( dbgStatusSex.equals( "true" ) ) { opts.setDbgStatusSex( true ); }
            else { opts.setDbgStatusSex( false ); }
        }

        if( dbgRegType != null ) {
            if( dbgRegType.equals( "true" ) ) { opts.setDbgRegType( true ); }
            else { opts.setDbgRegType( false ); }
        }

        if( dbgOccupation != null ) {
            if( dbgOccupation.equals( "true" ) ) { opts.setDbgOccupation( true ); }
            else { opts.setDbgOccupation( false ); }
        }

        if( dbgAge != null ) {
            if( dbgAge.equals( "true" ) ) { opts.setDbgAge( true ); }
            else { opts.setDbgAge( false ); }
        }

        if( dbgRole != null ) {
            if( dbgRole.equals( "true" ) ) { opts.setDbgRole( true ); }
            else { opts.setDbgRole( false ); }
        }

        if( dbgDates != null ) {
            if( dbgDates.equals( "true" ) ) { opts.setDbgDates( true ); }
            else { opts.setDbgDates( false ); }
        }

        if( dbgMinMaxMarriage != null ) {
            if( dbgMinMaxMarriage.equals( "true" ) ) { opts.setDbgMinMaxMarriage( true ); }
            else { opts.setDbgMinMaxMarriage( false ); }
        }

        if( dbgPartsToFullDate != null ) {
            if( dbgPartsToFullDate.equals( "true" ) ) { opts.setDbgPartsToFullDate( true ); }
            else { opts.setDbgPartsToFullDate( false ); }
        }

        if( dbgDaysSinceBegin != null ) {
            if( dbgDaysSinceBegin.equals( "true" ) ) { opts.setDbgDaysSinceBegin( true ); }
            else { opts.setDbgDaysSinceBegin( false ); }
        }

        if( dbgPostTasks != null ) {
            if( dbgPostTasks.equals( "true" ) ) { opts.setDbgPostTasks(true); }
            else { opts.setDbgPostTasks(false); }
        }

        if( dbgRemoveDuplicates != null ) {
            if( dbgRemoveDuplicates.equals( "true" ) ) { opts.setDbgRemoveDuplicates(true); }
            else { opts.setDbgRemoveDuplicates(false); }
        }

        if( dbgPrematch != null ) {
            if( dbgPrematch.equals( "true" ) ) { opts.setDbgPrematch(true); }
            else { opts.setDbgPrematch(false); }
        }

        if( dbgMatch != null ) {
            if( dbgMatch.equals( "true" ) ) { opts.setDbgMatch(true); }
            else { opts.setDbgMatch(false); }
        }


        String sourceIds = tbLOLCSourceId.getText();

        String db_url  = tbLOLCurl.getText();      // from Tab Cleaned
        String db_user = tbLOLCuser.getText();     // from Tab Cleaned
        String db_pass = tbLOLCpass.getText();     // from Tab Cleaned

        String db_ref_url  = tbLOLCrefurl.getText();      // from Tab Cleaned
        String db_ref_user = tbLOLCrefuser.getText();     // from Tab Cleaned
        String db_ref_pass = tbLOLCrefpass.getText();     // from Tab Cleaned
        String db_ref_db   = tbLOLCrefdb.getText();       // from Tab Cleaned

        opts.setLogger( plog );
        opts.setSourceIds( sourceIds );

        opts.setDb_ref_url(  db_ref_url );
        opts.setDb_ref_user( db_ref_user );
        opts.setDb_ref_pass( db_ref_pass );
        opts.setDb_ref_db(   db_ref_db );

        opts.setDb_url(  db_url );
        opts.setDb_user( db_user );
        opts.setDb_pass( db_pass );

        return opts;
    }


    private void bnLOLCstartProcessActionPerformed( java.awt.event.ActionEvent evt )
    {
        // "Start Cleaning" entry
        opts = setOptions();

        tbLOLClatestOutput.setText( "" );
        taLOLCoutput.setText( "" );

        LinksCleaned linksCleaned = new LinksCleaned( opts, tbLOLClatestOutput, taLOLCoutput, this );

        linksCleaned.start();
    }//GEN-LAST:event_btnStartProcessClean


    private void tfBILODeleteSourceKeyReleased(java.awt.event.KeyEvent evt)
    {//GEN-FIRST:event_tfBILODeleteSourceKeyReleased
        // TODO add your handling code here:
    }//GEN-LAST:event_tfBILODeleteSourceKeyReleased


    private void btnStartProcessMatchActionPerformed( ActionEvent ev )
    {
        System.out.println( "ManagerGui/btnStartProcessMatchActionPerformed()" );

        //String jarDir  = "/home/fons/IdeaProjects/links/LinksMatchManager/target/";
        String jarName = "LinksMatchManager-2.0.jar";
        //String jarPath = jarDir + jarName;

        String cwd = System.getProperty( "user.dir" );
        System.out.println( cwd );
        String jarPath = new File( cwd, jarName ).toString();
        System.out.println( jarPath );

        String db_url  = tbLOLCurl.getText();      // from Tab Clean
        String db_user = tbLOLCuser.getText();     // from Tab Clean
        String db_pass = tbLOLCpass.getText();     // from Tab Clean

        String max_threads = "1";

        try
        {
            // java -jar LinksMatchManager-2.0.jar <db_url> <db_username> <db_password> <max_threads>
            String cmdline[] = new String[] { "java", "-jar", jarPath, db_url, db_user, db_pass, max_threads };

            // show what we do
            //System.out.println( Arrays.toString( cmdline ) );     // comma separated substrings
            String vizline = "";
            for( String s : cmdline ) {
                if( vizline.isEmpty() ) { vizline = s; }
                else{ vizline = vizline + " " + s; }
            }
            System.out.println( "Execute: " + vizline );

            Process ps = Runtime.getRuntime().exec( cmdline );
            ps.waitFor();

            InputStream inp = ps.getInputStream();
            InputStream err = ps.getErrorStream();

            byte b_inp[] = new byte[ inp.available() ];
            inp.read( b_inp, 0, b_inp.length );
            System.out.println( new String( b_inp ) );

            byte b_err[] = new byte[ err.available() ];
            err.read( b_err, 0, b_err.length );
            System.out.println( new String( b_err ) );
        }
        catch( Exception ex ) { System.out.println(ex.getMessage()); }
    }


    public void firePrematch() {
        bnPstartProcessActionPerformed( null );
    }


    private void bnPstartProcessActionPerformed( java.awt.event.ActionEvent evt )
    {
        // "Start PreMatching" entry

        opts = setOptions();

        boolean doSplitFirstnames = false;
        boolean doFrequencyTables = false;
        boolean doStandardization = false;
        boolean doLevenshtein     = false;
        boolean doNamesToNos      = false;
        boolean doBaseTable       = false;

        if( cbPdoSplitFirstnames.isSelected() ) { doSplitFirstnames = true; }

        if( cbPdoFrequencyTables.isSelected() ) { doFrequencyTables = true; }

        if( cbPdoStandardization.isSelected() ) { doStandardization = true; }

        if( cbPdoLevenshtein.isSelected() ) { doLevenshtein = true; }

        if( cbPdoNamesToNos.isSelected() ) { doNamesToNos = true; }

        if( cbPdoCreateBaseTable.isSelected() ) { doBaseTable = true; }


        try {
            LinksPrematch lpm = new LinksPrematch
            (
                opts,

                taPinfo,
                taPresult,

                doSplitFirstnames,
                doFrequencyTables,
                doStandardization,
                doLevenshtein,
                doNamesToNos,
                doBaseTable
            );

            lpm.start();
        }
        catch( Exception ex ) {
            taPinfo.setText( "ERROR: "  + ex.getMessage() ) ;
        }
    }//GEN-LAST:event_bnPstartProcessActionPerformed


    private void cbPdoCreateBaseTableActionPerformed(java.awt.event.ActionEvent evt)
    {//GEN-FIRST:event_cbPdoCreateBaseTableActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_cbPdoCreateBaseTableActionPerformed


    private void tfCTDDbPassActionPerformed(java.awt.event.ActionEvent evt)
    {//GEN-FIRST:event_tfCTDDbPassActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_tfCTDDbPassActionPerformed


    private void rbBebiIndexGenliasActionPerformed(java.awt.event.ActionEvent evt)
    {//GEN-FIRST:event_rbBebiIndexGenliasActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_rbBebiIndexGenliasActionPerformed


    /**
     * Main method, to start Gui manager
     * @param args
     */
    public static void main( String args[] )
    {
        //try { logger.info( "ManagerGui/main()" ); }
        //catch( Exception ex ) { System.out.println( ex.getMessage() ); }

        String hostname = "";
        try { hostname = InetAddress.getLocalHost().getHostName(); }
        catch( UnknownHostException ex ) { System.out.println( ex.getMessage() ); }

        try {
            plog = new general.PrintLogger();
            plog.show( "Running on host: " + hostname );
            plog.show( "ManagerGui/main()" );
        }
        catch( Exception ex ) { System.out.println( ex.getMessage() ); }

        java.awt.EventQueue.invokeLater( new Runnable()
        {
            public void run()
            {
                String timestamp1 = "09-Feb-2015 14:39";

                String timestamp2 = LinksSpecific.getTimeStamp2( "yyyy.MM.dd-HH:mm:ss" );

                try {
                    plog.show( "LINKS timestamp: " + timestamp1 );
                    plog.show( "Start at: " + timestamp2 );
                    plog.show( "ManagerGui/main/run()" );
                }
                catch( Exception ex ) { System.out.println( ex.getMessage() ); }

                new ManagerGui().setVisible( true );
            }
        });
    }
    // Variables declaration - do not modify//GEN-BEGIN:variables
    // Generated using JFormDesigner non-commercial license
    private JMenuBar menuBar;
    private JMenu fileMenu;
    private JMenuItem openMenuItem;
    private JMenuItem saveMenuItem;
    private JMenuItem exitMenuItem;
    private JMenu editMenu;
    private JMenuItem cutMenuItem;
    private JMenuItem copyMenuItem;
    private JMenuItem pasteMenuItem;
    private JMenuItem deleteMenuItem;
    private JMenu helpMenu;
    private JMenuItem contentsMenuItem;
    private JMenuItem aboutMenuItem;
    private JTabbedPane tpMain;
    private JPanel pMain;
    private JButton bMAINAdd;
    private JButton bMAINDelete;
    private JTextField tMAINProjectName;
    private JLabel jLabel1;
    private JLabel jLabel19;
    private JTextField tMAINDbLocation;
    private JLabel jLabel24;
    private JTextField tMAINPass;
    private JTextField tMAINUser;
    private JLabel jLabel26;
    private JButton bMAINSave;
    private JButton bMAINClear;
    private JButton bMAINActivate;
    private JLabel jLabel30;
    private JScrollPane jScrollPane5;
    private JTextArea taMAINOpmerkingen;
    private JScrollPane jScrollPane4;
    private JList<String> lMAINProjects;
    private JLabel jLabel37;
    private JTextField tMAINPass1;
    private JTextField tMAINPass2;
    private JButton jButton2;
    private JLabel jLabel38;
    private JLabel jLabel39;
    private JTextField tMAINPass3;
    private JTextField tMAINPass4;
    private JCheckBox cbMAINbebi;
    private JLabel jLabel40;
    private JCheckBox cbMAINbilo;
    private JLabel jLabel41;
    private JCheckBox cbMAINlolc;
    private JPanel pCsv;
    private JLabel jLabel7;
    private JTextField tfCSVCsvFile;
    private JButton bCSVPath;
    private JButton bCSVGetHeader;
    private JLabel jLabel12;
    private JTextField tfCSVOutputFile;
    private JButton bCSVPathOutpuFile;
    private JLabel jLabel13;
    private JTextField tfCSVFilterText;
    private JLabel jLabel9;
    private JTextField tfCSVLineData;
    private JLabel jLabel2;
    private JScrollPane jScrollPane1;
    private JTextArea taCSVOutput;
    private JLabel jLabel8;
    private JTextField tfCSVInfo;
    private JLabel jLabel18;
    private JLabel jLabel20;
    private JTextField tfCSVFilterIndex;
    private JButton bCSVBeginFilter;
    private JPanel pCsvToDatabase;
    private JButton bSelectCsvToDbCsvFile;
    private JTextField tfCTBCsvFiles;
    private JLabel jLabel3;
    private JLabel jLabel10;
    private JLabel jLabel14;
    private JLabel jLabel15;
    private JLabel jLabel16;
    private JLabel jLabel17;
    private JTextField tfCTDDbLocation;
    private JTextField tfCTDPreFix;
    private JTextField tfCTDDbName;
    private JTextField tfCTDDbUser;
    private JTextField tfCTDDbPass;
    private JScrollPane jScrollPane2;
    private JTextArea taCTDOutput;
    private JButton bCsvToDbStart;
    private JLabel jLabel11;
    private JTextField tfCTDLatestOutput;
    private JLabel jLabel21;
    private JTextField tfCTDLineNumber;
    private JProgressBar pbDTCTotal;
    private JLabel jLabel22;
    private JLabel jLabel23;
    private JProgressBar pbDTCFile;
    private JTextField tfCTDdelay;
    private JLabel jLabel32;
    private JRadioButton rbBebiIndexGeen;
    private JRadioButton rbBebiIndexGenlias;
    private JPanel pBronInternToLinksOrigineel;
    private JLabel jLabel25;
    private JTextField tfBILODbLocation;
    private JLabel jLabel27;
    private JTextField tfBILODbUser;
    private JTextField tfBILODbPass;
    private JLabel jLabel28;
    private JLabel jLabel29;
    private JButton bBronInternToLinksOrigineel;
    private JTextField tfBILOProjectName;
    private JLabel jLabel31;
    private JScrollPane jScrollPane3;
    private JTextArea taBILOOutput;
    private JTextField tfBILOLatestOutput;
    private JLabel jLabel46;
    private JTextField tfBILOdelay;
    private JTextField tfBILODeleteSource;
    private JLabel jLabel33;
    private JCheckBox cbOrunCleaned;
    private JPanel pLOLC;
    private JTextField tbLOLCurl;
    private JLabel jLabel4;
    private JTextField tbLOLCuser;
    private JTextField tbLOLCpass;
    private JLabel jLabel42;
    private JLabel jLabel43;
    private JTextField tbLOLCSourceId;
    private JLabel jLabel44;
    private JButton btnStartProcessClean;
    private JScrollPane jScrollPane6;
    private JTextArea taLOLCoutput;
    private JLabel jLabel45;
    private JTextField tbLOLClatestOutput;
    private JLabel jLabel51;
    private JCheckBox cbCdoFamilynames;
    private JCheckBox cbCdoStatusSex;
    private JCheckBox cbCdoDates;
    private JCheckBox cbCdoRefreshData;
    private JCheckBox cbCdoRegType;
    private JCheckBox cbCdoLocations;
    private JCheckBox cbCdoMinMaxMarriage;
    private JCheckBox cbCdoPartsToFullDate;
    private JCheckBox cbCdoDaysSinceBegin;
    private JCheckBox cbCdoPostTasks;
    private JTextField tbLOLCrefurl;
    private JTextField tbLOLCrefuser;
    private JTextField tbLOLCrefpass;
    private JLabel label1;
    private JLabel label2;
    private JLabel label3;
    private JLabel label4;
    private JTextField tbLOLCrefdb;
    private JCheckBox cbCdoOccupation;
    private JCheckBox cbCdoFirstnames;
    private JCheckBox cbCdoPrepieceSuffix;
    private JCheckBox cbCdoRemoveDuplicates;
    private JPanel pPrematch;
    private JCheckBox cbPdoFrequencyTables;
    private JCheckBox cbPdoLevenshtein;
    private JCheckBox cbPdoCreateBaseTable;
    private JCheckBox cbPdoSplitFirstnames;
    private JButton btnStartProcessPrematch;
    private JScrollPane jScrollPane7;
    private JTextArea taPresult;
    private JCheckBox cbPdoNamesToNos;
    private JTextField taPinfo;
    private JCheckBox cbPdoStandardization;
    private JPanel pMatch;
    private JScrollPane scrollPane1;
    private JTextArea textArea1;
    private JButton btnStartProcessMatch;
    // End of variables declaration//GEN-END:variables
}
