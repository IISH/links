package linksManager;

import moduleMain.*;
import javax.swing.*;
import java.util.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import enumDefinitions.*;
//import dataSet.*;
import dataSet.DoSet;

import connectors.CsvConnector;

//test
import java.util.regex.*;

public class ManagerGui extends javax.swing.JFrame {

    // Instances of used objects
    Document doc;

    public ManagerGui() {
        setLookAndFeel();
        createDomProject();
        initComponents();
    }

    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        buttonGroup1 = new javax.swing.ButtonGroup();
        buttonGroup2 = new javax.swing.ButtonGroup();
        tpMain = new javax.swing.JTabbedPane();
        pMain = new javax.swing.JPanel();
        bMAINAdd = new javax.swing.JButton();
        bMAINDelete = new javax.swing.JButton();
        tMAINProjectName = new javax.swing.JTextField();
        jLabel1 = new javax.swing.JLabel();
        jLabel19 = new javax.swing.JLabel();
        tMAINDbLocation = new javax.swing.JTextField();
        jLabel24 = new javax.swing.JLabel();
        tMAINPass = new javax.swing.JTextField();
        tMAINUser = new javax.swing.JTextField();
        jLabel26 = new javax.swing.JLabel();
        bMAINSave = new javax.swing.JButton();
        bMAINClear = new javax.swing.JButton();
        bMAINActivate = new javax.swing.JButton();
        jLabel30 = new javax.swing.JLabel();
        jScrollPane5 = new javax.swing.JScrollPane();
        taMAINOpmerkingen = new javax.swing.JTextArea();
        jScrollPane4 = new javax.swing.JScrollPane();
        lMAINProjects = new javax.swing.JList();
        jLabel37 = new javax.swing.JLabel();
        tMAINPass1 = new javax.swing.JTextField();
        tMAINPass2 = new javax.swing.JTextField();
        jButton2 = new javax.swing.JButton();
        jLabel38 = new javax.swing.JLabel();
        jLabel39 = new javax.swing.JLabel();
        tMAINPass3 = new javax.swing.JTextField();
        tMAINPass4 = new javax.swing.JTextField();
        cbMAINbebi = new javax.swing.JCheckBox();
        jLabel40 = new javax.swing.JLabel();
        cbMAINbilo = new javax.swing.JCheckBox();
        jLabel41 = new javax.swing.JLabel();
        cbMAINlolc = new javax.swing.JCheckBox();
        pCsv = new javax.swing.JPanel();
        jLabel7 = new javax.swing.JLabel();
        tfCSVCsvFile = new javax.swing.JTextField();
        bCSVPath = new javax.swing.JButton();
        bCSVGetHeader = new javax.swing.JButton();
        jLabel12 = new javax.swing.JLabel();
        tfCSVOutputFile = new javax.swing.JTextField();
        bCSVPathOutpuFile = new javax.swing.JButton();
        jLabel13 = new javax.swing.JLabel();
        tfCSVFilterText = new javax.swing.JTextField();
        jLabel9 = new javax.swing.JLabel();
        tfCSVLineData = new javax.swing.JTextField();
        jLabel2 = new javax.swing.JLabel();
        jScrollPane1 = new javax.swing.JScrollPane();
        taCSVOutput = new javax.swing.JTextArea();
        jLabel8 = new javax.swing.JLabel();
        tfCSVInfo = new javax.swing.JTextField();
        jLabel18 = new javax.swing.JLabel();
        jLabel20 = new javax.swing.JLabel();
        tfCSVFilterIndex = new javax.swing.JTextField();
        bCSVBeginFilter = new javax.swing.JButton();
        pCsvToDatabase = new javax.swing.JPanel();
        bSelectCsvToDbCsvFile = new javax.swing.JButton();
        tfCTBCsvFiles = new javax.swing.JTextField();
        jLabel3 = new javax.swing.JLabel();
        jLabel10 = new javax.swing.JLabel();
        jLabel14 = new javax.swing.JLabel();
        jLabel15 = new javax.swing.JLabel();
        jLabel16 = new javax.swing.JLabel();
        jLabel17 = new javax.swing.JLabel();
        tfCTDDbLocation = new javax.swing.JTextField();
        tfCTDPreFix = new javax.swing.JTextField();
        tfCTDDbName = new javax.swing.JTextField();
        tfCTDDbUser = new javax.swing.JTextField();
        tfCTDDbPass = new javax.swing.JTextField();
        jScrollPane2 = new javax.swing.JScrollPane();
        taCTDOutput = new javax.swing.JTextArea();
        bCsvToDbStart = new javax.swing.JButton();
        jLabel11 = new javax.swing.JLabel();
        tfCTDLatestOutput = new javax.swing.JTextField();
        jLabel21 = new javax.swing.JLabel();
        tfCTDLineNumber = new javax.swing.JTextField();
        pbDTCTotal = new javax.swing.JProgressBar();
        jLabel22 = new javax.swing.JLabel();
        jLabel23 = new javax.swing.JLabel();
        pbDTCFile = new javax.swing.JProgressBar();
        tfCTDdelay = new javax.swing.JTextField();
        jLabel32 = new javax.swing.JLabel();
        rbBebiIndexGeen = new javax.swing.JRadioButton();
        rbBebiIndexGenlias = new javax.swing.JRadioButton();
        pBronInternToLinksOrigineel = new javax.swing.JPanel();
        jLabel25 = new javax.swing.JLabel();
        tfBILODbLocation = new javax.swing.JTextField();
        jLabel27 = new javax.swing.JLabel();
        tfBILODbUser = new javax.swing.JTextField();
        tfBILODbPass = new javax.swing.JTextField();
        jLabel28 = new javax.swing.JLabel();
        jLabel29 = new javax.swing.JLabel();
        bBronInternToLinksOrigineel = new javax.swing.JButton();
        tfBILOProjectName = new javax.swing.JTextField();
        jLabel31 = new javax.swing.JLabel();
        jScrollPane3 = new javax.swing.JScrollPane();
        taBILOOutput = new javax.swing.JTextArea();
        tfBILOLatestOutput = new javax.swing.JTextField();
        jLabel46 = new javax.swing.JLabel();
        tfBILOdelay = new javax.swing.JTextField();
        tfBILODeleteSource = new javax.swing.JTextField();
        jLabel33 = new javax.swing.JLabel();
        cbOrunCleaned = new javax.swing.JCheckBox();
        pLOLC = new javax.swing.JPanel();
        tbLOLCurl = new javax.swing.JTextField();
        jLabel4 = new javax.swing.JLabel();
        tbLOLCuser = new javax.swing.JTextField();
        tbLOLCpass = new javax.swing.JTextField();
        jLabel42 = new javax.swing.JLabel();
        jLabel43 = new javax.swing.JLabel();
        tbLOLCBronNr = new javax.swing.JTextField();
        jLabel44 = new javax.swing.JLabel();
        bnLOLCstartProcess = new javax.swing.JButton();
        jScrollPane6 = new javax.swing.JScrollPane();
        taLOLCoutput = new javax.swing.JTextArea();
        jLabel45 = new javax.swing.JLabel();
        tbLOLClatestOutput = new javax.swing.JTextField();
        jLabel51 = new javax.swing.JLabel();
        cbCdoOpmerkingen = new javax.swing.JCheckBox();
        cbCdoNames = new javax.swing.JCheckBox();
        cbCdoStaatSex = new javax.swing.JCheckBox();
        cbCdoDates = new javax.swing.JCheckBox();
        cbCdoJaarLeeftijd = new javax.swing.JCheckBox();
        cbCdoVernieuwen = new javax.swing.JCheckBox();
        cbCdoType = new javax.swing.JCheckBox();
        cbLOLCdoSequentie = new javax.swing.JCheckBox();
        cbCdoLocations = new javax.swing.JCheckBox();
        cbCdoRelation = new javax.swing.JCheckBox();
        cbLOLCdoMinMaxDate = new javax.swing.JCheckBox();
        cbLOLCdoMinMaxMarriage = new javax.swing.JCheckBox();
        cbLOLCdoPartsToFullDate = new javax.swing.JCheckBox();
        cbCdoDaysSinceBegin = new javax.swing.JCheckBox();
        cbLOLCdoRole = new javax.swing.JCheckBox();
        cbLOLCdoPostTasks = new javax.swing.JCheckBox();
        cbCdoPrematch = new javax.swing.JCheckBox();
        cbCdoPreBasicNames = new javax.swing.JCheckBox();
        jPanel1 = new javax.swing.JPanel();
        cbPdoUniqueNameTables = new javax.swing.JCheckBox();
        cbPdoLevenshtein = new javax.swing.JCheckBox();
        cbPdoCreateBaseTable = new javax.swing.JCheckBox();
        cbPdoSplitName = new javax.swing.JCheckBox();
        bnPstartProcess = new javax.swing.JButton();
        jScrollPane7 = new javax.swing.JScrollPane();
        taPresult = new javax.swing.JTextArea();
        cbPdoNameToNo = new javax.swing.JCheckBox();
        taPinfo = new javax.swing.JTextField();
        menuBar = new javax.swing.JMenuBar();
        fileMenu = new javax.swing.JMenu();
        openMenuItem = new javax.swing.JMenuItem();
        saveMenuItem = new javax.swing.JMenuItem();
        exitMenuItem = new javax.swing.JMenuItem();
        editMenu = new javax.swing.JMenu();
        cutMenuItem = new javax.swing.JMenuItem();
        copyMenuItem = new javax.swing.JMenuItem();
        pasteMenuItem = new javax.swing.JMenuItem();
        deleteMenuItem = new javax.swing.JMenuItem();
        helpMenu = new javax.swing.JMenu();
        contentsMenuItem = new javax.swing.JMenuItem();
        aboutMenuItem = new javax.swing.JMenuItem();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
        setTitle("Links Data Manager v. 0.1");
        setCursor(new java.awt.Cursor(java.awt.Cursor.DEFAULT_CURSOR));

        tpMain.setName("tpMain"); // NOI18N

        pMain.setName("pMain"); // NOI18N

        bMAINAdd.setText("Add");
        bMAINAdd.setName("bMAINAdd"); // NOI18N
        bMAINAdd.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bMAINAddActionPerformed(evt);
            }
        });

        bMAINDelete.setText("Delete");
        bMAINDelete.setName("bMAINDelete"); // NOI18N
        bMAINDelete.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bMAINDeleteActionPerformed(evt);
            }
        });

        tMAINProjectName.setName("tMAINProjectName"); // NOI18N

        jLabel1.setText("Project name");
        jLabel1.setName("jLabel1"); // NOI18N

        jLabel19.setText("User");
        jLabel19.setName("jLabel19"); // NOI18N

        tMAINDbLocation.setName("tMAINDbLocation"); // NOI18N

        jLabel24.setText("Database location");
        jLabel24.setName("jLabel24"); // NOI18N

        tMAINPass.setName("tMAINPass"); // NOI18N

        tMAINUser.setName("tMAINUser"); // NOI18N

        jLabel26.setText("Pass");
        jLabel26.setName("jLabel26"); // NOI18N

        bMAINSave.setText("Save");
        bMAINSave.setName("bMAINSave"); // NOI18N
        bMAINSave.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bMAINSaveActionPerformed(evt);
            }
        });

        bMAINClear.setText("Clear");
        bMAINClear.setName("bMAINClear"); // NOI18N
        bMAINClear.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bMAINClearActionPerformed(evt);
            }
        });

        bMAINActivate.setText("Activate");
        bMAINActivate.setName("bMAINActivate"); // NOI18N
        bMAINActivate.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bMAINActivateActionPerformed(evt);
            }
        });

        jLabel30.setText("EXTRA");
        jLabel30.setName("jLabel30"); // NOI18N

        jScrollPane5.setName("jScrollPane5"); // NOI18N

        taMAINOpmerkingen.setColumns(20);
        taMAINOpmerkingen.setRows(5);
        taMAINOpmerkingen.setName("taMAINOpmerkingen"); // NOI18N
        jScrollPane5.setViewportView(taMAINOpmerkingen);

        jScrollPane4.setName("jScrollPane4"); // NOI18N

        lMAINProjects.setName("lMAINProjects"); // NOI18N
        lMAINProjects.addListSelectionListener(new javax.swing.event.ListSelectionListener() {
            public void valueChanged(javax.swing.event.ListSelectionEvent evt) {
                lMAINProjectsValueChanged(evt);
            }
        });
        jScrollPane4.setViewportView(lMAINProjects);

        jLabel37.setText("Logbestand");
        jLabel37.setName("jLabel37"); // NOI18N

        tMAINPass1.setName("tMAINPass1"); // NOI18N

        tMAINPass2.setName("tMAINPass2"); // NOI18N

        jButton2.setText("...");
        jButton2.setName("jButton2"); // NOI18N

        jLabel38.setText("EXTRA");
        jLabel38.setName("jLabel38"); // NOI18N

        jLabel39.setText("EXTRA");
        jLabel39.setName("jLabel39"); // NOI18N

        tMAINPass3.setName("tMAINPass3"); // NOI18N

        tMAINPass4.setName("tMAINPass4"); // NOI18N

        cbMAINbebi.setText("Bron Extern To Bron Intern");
        cbMAINbebi.setName("cbMAINbebi"); // NOI18N

        jLabel40.setText("Activate");
        jLabel40.setName("jLabel40"); // NOI18N

        cbMAINbilo.setText("Bron Intern To Links Originee");
        cbMAINbilo.setName("cbMAINbilo"); // NOI18N

        jLabel41.setText("Opmerkingen");
        jLabel41.setName("jLabel41"); // NOI18N

        cbMAINlolc.setText("lLinks Origineel To Links Cleaned");
        cbMAINlolc.setName("cbMAINlolc"); // NOI18N

        javax.swing.GroupLayout pMainLayout = new javax.swing.GroupLayout(pMain);
        pMain.setLayout(pMainLayout);
        pMainLayout.setHorizontalGroup(
            pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, pMainLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addGroup(pMainLayout.createSequentialGroup()
                        .addComponent(bMAINAdd)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(bMAINDelete))
                    .addComponent(jScrollPane4, javax.swing.GroupLayout.PREFERRED_SIZE, 153, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(18, 18, 18)
                .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
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
                .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                        .addComponent(tMAINPass4, javax.swing.GroupLayout.Alignment.TRAILING)
                        .addComponent(tMAINPass3, javax.swing.GroupLayout.Alignment.TRAILING)
                        .addComponent(tMAINUser)
                        .addComponent(tMAINPass)
                        .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, pMainLayout.createSequentialGroup()
                            .addComponent(bMAINClear)
                            .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                            .addComponent(bMAINActivate)
                            .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                            .addComponent(bMAINSave))
                        .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, pMainLayout.createSequentialGroup()
                            .addComponent(tMAINPass1)
                            .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                            .addComponent(jButton2))
                        .addComponent(jScrollPane5)
                        .addComponent(tMAINDbLocation)
                        .addComponent(tMAINPass2)
                        .addComponent(tMAINProjectName))
                    .addGroup(pMainLayout.createSequentialGroup()
                        .addComponent(cbMAINbebi)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(cbMAINbilo)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(cbMAINlolc)))
                .addGap(1666, 1666, 1666))
        );
        pMainLayout.setVerticalGroup(
            pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pMainLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jScrollPane4, javax.swing.GroupLayout.DEFAULT_SIZE, 591, Short.MAX_VALUE)
                    .addGroup(pMainLayout.createSequentialGroup()
                        .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(tMAINProjectName, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel1))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(tMAINDbLocation, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel24))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(tMAINUser, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel19))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(tMAINPass, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel26))
                        .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addGroup(pMainLayout.createSequentialGroup()
                                .addGap(9, 9, 9)
                                .addComponent(jLabel37))
                            .addGroup(pMainLayout.createSequentialGroup()
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                                    .addComponent(jButton2)
                                    .addComponent(tMAINPass1, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(tMAINPass2, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel30))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(tMAINPass3, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel38))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(tMAINPass4, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel39))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(cbMAINbebi)
                            .addComponent(jLabel40)
                            .addComponent(cbMAINbilo)
                            .addComponent(cbMAINlolc))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(jLabel41)
                            .addComponent(jScrollPane5, javax.swing.GroupLayout.DEFAULT_SIZE, 354, Short.MAX_VALUE))))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                        .addComponent(bMAINActivate)
                        .addComponent(bMAINClear)
                        .addComponent(bMAINSave))
                    .addGroup(pMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                        .addComponent(bMAINAdd)
                        .addComponent(bMAINDelete)))
                .addContainerGap())
        );

        tpMain.addTab("MAIN", pMain);

        pCsv.setName("pCsv"); // NOI18N

        jLabel7.setText("CSV File");
        jLabel7.setName("jLabel7"); // NOI18N

        tfCSVCsvFile.setName("tfCSVCsvFile"); // NOI18N

        bCSVPath.setText("...");
        bCSVPath.setName("bCSVPath"); // NOI18N
        bCSVPath.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bCSVPathActionPerformed(evt);
            }
        });

        bCSVGetHeader.setText("Get Header");
        bCSVGetHeader.setName("bCSVGetHeader"); // NOI18N
        bCSVGetHeader.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bCSVGetHeaderActionPerformed(evt);
            }
        });

        jLabel12.setText("Destination");
        jLabel12.setName("jLabel12"); // NOI18N

        tfCSVOutputFile.setName("tfCSVOutputFile"); // NOI18N

        bCSVPathOutpuFile.setText("...");
        bCSVPathOutpuFile.setName("bCSVPathOutpuFile"); // NOI18N
        bCSVPathOutpuFile.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bCSVPathOutpuFileActionPerformed(evt);
            }
        });

        jLabel13.setText("FILTER OPTIONS");
        jLabel13.setName("jLabel13"); // NOI18N

        tfCSVFilterText.setName("tfCSVFilterText"); // NOI18N

        jLabel9.setText("LINE");
        jLabel9.setName("jLabel9"); // NOI18N

        tfCSVLineData.setName("tfCSVLineData"); // NOI18N

        jLabel2.setText("Output");
        jLabel2.setName("jLabel2"); // NOI18N

        jScrollPane1.setName("jScrollPane1"); // NOI18N

        taCSVOutput.setColumns(20);
        taCSVOutput.setRows(5);
        taCSVOutput.setName("jtaOutput"); // NOI18N
        jScrollPane1.setViewportView(taCSVOutput);

        jLabel8.setText("Info");
        jLabel8.setName("jLabel8"); // NOI18N

        tfCSVInfo.setName("tfCSVInfo"); // NOI18N

        jLabel18.setText("Header index. index starts with 0");
        jLabel18.setName("jLabel18"); // NOI18N

        jLabel20.setText("Filter text");
        jLabel20.setName("jLabel20"); // NOI18N

        tfCSVFilterIndex.setName("tfCSVFilterIndex"); // NOI18N

        bCSVBeginFilter.setText("begin Filtering");
        bCSVBeginFilter.setName("bCSVBeginFilter"); // NOI18N
        bCSVBeginFilter.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bCSVBeginFilterActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout pCsvLayout = new javax.swing.GroupLayout(pCsv);
        pCsv.setLayout(pCsvLayout);
        pCsvLayout.setHorizontalGroup(
            pCsvLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, pCsvLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pCsvLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                    .addComponent(jScrollPane1, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE, 1101, Short.MAX_VALUE)
                    .addGroup(pCsvLayout.createSequentialGroup()
                        .addGroup(pCsvLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                            .addComponent(jLabel13, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(jLabel8)
                            .addComponent(jLabel20)
                            .addComponent(jLabel9)
                            .addComponent(jLabel7)
                            .addComponent(jLabel12, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(pCsvLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(tfCSVInfo, javax.swing.GroupLayout.DEFAULT_SIZE, 1013, Short.MAX_VALUE)
                            .addComponent(tfCSVLineData, javax.swing.GroupLayout.DEFAULT_SIZE, 1013, Short.MAX_VALUE)
                            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, pCsvLayout.createSequentialGroup()
                                .addGroup(pCsvLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                                    .addComponent(tfCSVFilterText, javax.swing.GroupLayout.DEFAULT_SIZE, 893, Short.MAX_VALUE)
                                    .addGroup(javax.swing.GroupLayout.Alignment.LEADING, pCsvLayout.createSequentialGroup()
                                        .addComponent(jLabel18)
                                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                        .addComponent(tfCSVFilterIndex, javax.swing.GroupLayout.DEFAULT_SIZE, 729, Short.MAX_VALUE))
                                    .addComponent(tfCSVOutputFile, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE, 893, Short.MAX_VALUE)
                                    .addComponent(tfCSVCsvFile, javax.swing.GroupLayout.DEFAULT_SIZE, 893, Short.MAX_VALUE))
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                                .addGroup(pCsvLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                    .addComponent(bCSVPathOutpuFile, javax.swing.GroupLayout.PREFERRED_SIZE, 28, javax.swing.GroupLayout.PREFERRED_SIZE)
                                    .addGroup(pCsvLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                        .addComponent(bCSVBeginFilter, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.PREFERRED_SIZE, 110, javax.swing.GroupLayout.PREFERRED_SIZE)
                                        .addComponent(bCSVGetHeader, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.PREFERRED_SIZE, 110, javax.swing.GroupLayout.PREFERRED_SIZE)
                                        .addComponent(bCSVPath, javax.swing.GroupLayout.PREFERRED_SIZE, 28, javax.swing.GroupLayout.PREFERRED_SIZE))))))
                    .addComponent(jLabel2, javax.swing.GroupLayout.Alignment.LEADING))
                .addContainerGap())
        );
        pCsvLayout.setVerticalGroup(
            pCsvLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pCsvLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pCsvLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel7)
                    .addComponent(tfCSVCsvFile, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(bCSVPath))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(pCsvLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel12)
                    .addComponent(tfCSVOutputFile, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(bCSVPathOutpuFile))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(pCsvLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(bCSVGetHeader)
                    .addComponent(jLabel13)
                    .addComponent(jLabel18)
                    .addComponent(tfCSVFilterIndex, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(pCsvLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(bCSVBeginFilter)
                    .addGroup(pCsvLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                        .addComponent(tfCSVFilterText, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addComponent(jLabel20)))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(pCsvLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(tfCSVInfo, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jLabel8))
                .addGroup(pCsvLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(pCsvLayout.createSequentialGroup()
                        .addGap(9, 9, 9)
                        .addComponent(jLabel9))
                    .addGroup(pCsvLayout.createSequentialGroup()
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(tfCSVLineData, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel2)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jScrollPane1, javax.swing.GroupLayout.DEFAULT_SIZE, 437, Short.MAX_VALUE)
                .addContainerGap())
        );

        tpMain.addTab("PREP: CSV Filter", pCsv);

        pCsvToDatabase.setName("pCsvToDatabase"); // NOI18N

        bSelectCsvToDbCsvFile.setText("...");
        bSelectCsvToDbCsvFile.setName("bSelectCsvToDbCsvFile"); // NOI18N
        bSelectCsvToDbCsvFile.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bSelectCsvToDbCsvFileActionPerformed(evt);
            }
        });

        tfCTBCsvFiles.setName("tfCTBCsvFiles"); // NOI18N

        jLabel3.setText("Select CSV File(S)");
        jLabel3.setName("jLabel3"); // NOI18N

        jLabel10.setText("Database Location");
        jLabel10.setName("jLabel10"); // NOI18N

        jLabel14.setText("Project");
        jLabel14.setName("jLabel14"); // NOI18N

        jLabel15.setText("DB Name");
        jLabel15.setName("jLabel15"); // NOI18N

        jLabel16.setText("Password");
        jLabel16.setName("jLabel16"); // NOI18N

        jLabel17.setText("User");
        jLabel17.setName("jLabel17"); // NOI18N

        tfCTDDbLocation.setText("node-152.dev.socialhistoryservices.org");
        tfCTDDbLocation.setName("tfCTDDbLocation"); // NOI18N

        tfCTDPreFix.setText("genlias");
        tfCTDPreFix.setName("tfCTDPreFix"); // NOI18N

        tfCTDDbName.setText("source_internal");
        tfCTDDbName.setName("tfCTDDbName"); // NOI18N

        tfCTDDbUser.setText("linksdev");
        tfCTDDbUser.setName("tfCTDDbUser"); // NOI18N

        tfCTDDbPass.setText("devlinks");
        tfCTDDbPass.setName("tfCTDDbPass"); // NOI18N
        tfCTDDbPass.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                tfCTDDbPassActionPerformed(evt);
            }
        });

        jScrollPane2.setName("jScrollPane2"); // NOI18N

        taCTDOutput.setColumns(20);
        taCTDOutput.setRows(5);
        taCTDOutput.setName("taCTDOutput"); // NOI18N
        jScrollPane2.setViewportView(taCTDOutput);

        bCsvToDbStart.setText("Start Process");
        bCsvToDbStart.setName("bCsvToDbStart"); // NOI18N
        bCsvToDbStart.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bCsvToDbStartActionPerformed(evt);
            }
        });

        jLabel11.setFont(new java.awt.Font("Tahoma", 1, 11)); // NOI18N
        jLabel11.setText(" Latest information and Log (below)");
        jLabel11.setName("jLabel11"); // NOI18N

        tfCTDLatestOutput.setName("tfCTDLatestOutput"); // NOI18N

        jLabel21.setText("Line number");
        jLabel21.setName("jLabel21"); // NOI18N

        tfCTDLineNumber.setName("tfCTDLineNumber"); // NOI18N

        pbDTCTotal.setName("pbDTCTotal"); // NOI18N

        jLabel22.setText("Total ");
        jLabel22.setName("jLabel22"); // NOI18N

        jLabel23.setText("Current File");
        jLabel23.setName("jLabel23"); // NOI18N

        pbDTCFile.setName("pbDTCFile"); // NOI18N

        tfCTDdelay.setText("0");
        tfCTDdelay.setName("tfCTDdelay"); // NOI18N

        jLabel32.setText("Delay minutes");
        jLabel32.setName("jLabel32"); // NOI18N

        buttonGroup1.add(rbBebiIndexGeen);
        rbBebiIndexGeen.setText("Geen Indexering");
        rbBebiIndexGeen.setName("rbBebiIndexGeen"); // NOI18N

        buttonGroup1.add(rbBebiIndexGenlias);
        rbBebiIndexGenlias.setText("Genlias 7-tabellen indexering");
        rbBebiIndexGenlias.setName("rbBebiIndexGenlias"); // NOI18N
        rbBebiIndexGenlias.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                rbBebiIndexGenliasActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout pCsvToDatabaseLayout = new javax.swing.GroupLayout(pCsvToDatabase);
        pCsvToDatabase.setLayout(pCsvToDatabaseLayout);
        pCsvToDatabaseLayout.setHorizontalGroup(
            pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                        .addComponent(jLabel3)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addComponent(tfCTBCsvFiles, javax.swing.GroupLayout.DEFAULT_SIZE, 973, Short.MAX_VALUE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(bSelectCsvToDbCsvFile, javax.swing.GroupLayout.PREFERRED_SIZE, 28, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                        .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                            .addComponent(bCsvToDbStart)
                            .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                                .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                    .addComponent(jLabel15)
                                    .addComponent(jLabel10)
                                    .addComponent(jLabel17)
                                    .addComponent(jLabel16)
                                    .addComponent(jLabel14)
                                    .addComponent(jLabel32))
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                    .addComponent(tfCTDDbPass, javax.swing.GroupLayout.DEFAULT_SIZE, 330, Short.MAX_VALUE)
                                    .addComponent(tfCTDDbUser, javax.swing.GroupLayout.DEFAULT_SIZE, 330, Short.MAX_VALUE)
                                    .addComponent(tfCTDDbName, javax.swing.GroupLayout.DEFAULT_SIZE, 330, Short.MAX_VALUE)
                                    .addComponent(tfCTDDbLocation, javax.swing.GroupLayout.DEFAULT_SIZE, 330, Short.MAX_VALUE)
                                    .addComponent(tfCTDPreFix, javax.swing.GroupLayout.DEFAULT_SIZE, 330, Short.MAX_VALUE)
                                    .addComponent(tfCTDdelay, javax.swing.GroupLayout.DEFAULT_SIZE, 330, Short.MAX_VALUE)))
                            .addComponent(rbBebiIndexGeen, javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(rbBebiIndexGenlias, javax.swing.GroupLayout.Alignment.LEADING))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                                .addGap(58, 58, 58)
                                .addComponent(jLabel11))
                            .addComponent(jScrollPane2, javax.swing.GroupLayout.DEFAULT_SIZE, 668, Short.MAX_VALUE)
                            .addComponent(tfCTDLatestOutput, javax.swing.GroupLayout.DEFAULT_SIZE, 668, Short.MAX_VALUE)
                            .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                                .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                    .addComponent(jLabel21)
                                    .addComponent(jLabel23)
                                    .addComponent(jLabel22))
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                    .addComponent(pbDTCFile, javax.swing.GroupLayout.DEFAULT_SIZE, 606, Short.MAX_VALUE)
                                    .addComponent(pbDTCTotal, javax.swing.GroupLayout.DEFAULT_SIZE, 606, Short.MAX_VALUE)
                                    .addComponent(tfCTDLineNumber, javax.swing.GroupLayout.DEFAULT_SIZE, 606, Short.MAX_VALUE))))))
                .addContainerGap())
        );
        pCsvToDatabaseLayout.setVerticalGroup(
            pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                        .addGap(45, 45, 45)
                        .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(tfCTDDbLocation, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel10)
                            .addComponent(jLabel11)))
                    .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                        .addContainerGap()
                        .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(jLabel3)
                            .addComponent(tfCTBCsvFiles, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(bSelectCsvToDbCsvFile))))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                    .addGroup(pCsvToDatabaseLayout.createSequentialGroup()
                        .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(tfCTDDbName, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel15))
                        .addGap(7, 7, 7))
                    .addComponent(jLabel23)
                    .addComponent(pbDTCFile, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                    .addComponent(pbDTCTotal, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                        .addComponent(tfCTDDbUser, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addComponent(jLabel17))
                    .addComponent(jLabel22))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(tfCTDDbPass, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jLabel16)
                    .addComponent(jLabel21)
                    .addComponent(tfCTDLineNumber, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(tfCTDPreFix, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jLabel14)
                    .addComponent(tfCTDLatestOutput, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                    .addComponent(jScrollPane2, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE, 444, Short.MAX_VALUE)
                    .addGroup(javax.swing.GroupLayout.Alignment.LEADING, pCsvToDatabaseLayout.createSequentialGroup()
                        .addGroup(pCsvToDatabaseLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(tfCTDdelay, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel32))
                        .addGap(18, 18, 18)
                        .addComponent(rbBebiIndexGeen)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addComponent(rbBebiIndexGenlias))
                    .addComponent(bCsvToDbStart))
                .addContainerGap())
        );

        tpMain.addTab("INTERNAL", pCsvToDatabase);

        pBronInternToLinksOrigineel.setName("pBronInternToLinksOrigineel"); // NOI18N

        jLabel25.setText("Database Location");
        jLabel25.setName("jLabel25"); // NOI18N

        tfBILODbLocation.setText("hebe");
        tfBILODbLocation.setName("tfBILODbLocation"); // NOI18N

        jLabel27.setText("User");
        jLabel27.setName("jLabel27"); // NOI18N

        tfBILODbUser.setText("linksbeta");
        tfBILODbUser.setName("tfBILODbUser"); // NOI18N

        tfBILODbPass.setText("betalinks");
        tfBILODbPass.setName("tfBILODbPass"); // NOI18N

        jLabel28.setText("Pass");
        jLabel28.setName("jLabel28"); // NOI18N

        jLabel29.setText("Project name");
        jLabel29.setName("jLabel29"); // NOI18N

        bBronInternToLinksOrigineel.setText("Start Process");
        bBronInternToLinksOrigineel.setName("bBronInternToLinksOrigineel"); // NOI18N
        bBronInternToLinksOrigineel.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bBronInternToLinksOrigineelActionPerformed(evt);
            }
        });

        tfBILOProjectName.setText("drenthe");
        tfBILOProjectName.setName("tfBILOProjectName"); // NOI18N
        tfBILOProjectName.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyReleased(java.awt.event.KeyEvent evt) {
                tfBILOProjectNameKeyReleased(evt);
            }
        });

        jLabel31.setFont(new java.awt.Font("Tahoma", 1, 11)); // NOI18N
        jLabel31.setText(" Latest information and Log (below)");
        jLabel31.setName("jLabel31"); // NOI18N

        jScrollPane3.setName("jScrollPane3"); // NOI18N

        taBILOOutput.setColumns(20);
        taBILOOutput.setRows(5);
        taBILOOutput.setName("taBILOOutput"); // NOI18N
        jScrollPane3.setViewportView(taBILOOutput);

        tfBILOLatestOutput.setName("tfBILOLatestOutput"); // NOI18N

        jLabel46.setText("Delay minutes");
        jLabel46.setName("jLabel46"); // NOI18N

        tfBILOdelay.setText("0");
        tfBILOdelay.setName("tfBILOdelay"); // NOI18N

        tfBILODeleteSource.setName("tfBILODeleteSource"); // NOI18N
        tfBILODeleteSource.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyReleased(java.awt.event.KeyEvent evt) {
                tfBILODeleteSourceKeyReleased(evt);
            }
        });

        jLabel33.setText("Source to delete");
        jLabel33.setName("jLabel33"); // NOI18N

        cbOrunCleaned.setText("RUN CLEANED");
        cbOrunCleaned.setName("cbOrunCleaned"); // NOI18N

        javax.swing.GroupLayout pBronInternToLinksOrigineelLayout = new javax.swing.GroupLayout(pBronInternToLinksOrigineel);
        pBronInternToLinksOrigineel.setLayout(pBronInternToLinksOrigineelLayout);
        pBronInternToLinksOrigineelLayout.setHorizontalGroup(
            pBronInternToLinksOrigineelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pBronInternToLinksOrigineelLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jLabel25)
                    .addComponent(jLabel27)
                    .addComponent(jLabel28)
                    .addComponent(jLabel29)
                    .addComponent(jLabel33)
                    .addComponent(jLabel46))
                .addGap(18, 18, 18)
                .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(pBronInternToLinksOrigineelLayout.createSequentialGroup()
                        .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(tfBILODbPass, javax.swing.GroupLayout.DEFAULT_SIZE, 372, Short.MAX_VALUE)
                            .addComponent(tfBILODbUser, javax.swing.GroupLayout.DEFAULT_SIZE, 372, Short.MAX_VALUE)
                            .addComponent(tfBILODbLocation, javax.swing.GroupLayout.DEFAULT_SIZE, 372, Short.MAX_VALUE)
                            .addComponent(tfBILOProjectName, javax.swing.GroupLayout.DEFAULT_SIZE, 372, Short.MAX_VALUE)
                            .addComponent(tfBILODeleteSource, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, 372, Short.MAX_VALUE)
                            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, pBronInternToLinksOrigineelLayout.createSequentialGroup()
                                .addComponent(tfBILOdelay, javax.swing.GroupLayout.DEFAULT_SIZE, 269, Short.MAX_VALUE)
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addComponent(bBronInternToLinksOrigineel)))
                        .addGap(14, 14, 14))
                    .addGroup(pBronInternToLinksOrigineelLayout.createSequentialGroup()
                        .addComponent(cbOrunCleaned, javax.swing.GroupLayout.PREFERRED_SIZE, 137, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)))
                .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jScrollPane3, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, 608, Short.MAX_VALUE)
                    .addComponent(tfBILOLatestOutput, javax.swing.GroupLayout.DEFAULT_SIZE, 608, Short.MAX_VALUE)
                    .addComponent(jLabel31))
                .addContainerGap())
        );
        pBronInternToLinksOrigineelLayout.setVerticalGroup(
            pBronInternToLinksOrigineelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pBronInternToLinksOrigineelLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel25)
                    .addComponent(tfBILODbLocation, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jLabel31))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel27)
                    .addComponent(tfBILODbUser, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(tfBILOLatestOutput, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(11, 11, 11)
                .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                    .addGroup(javax.swing.GroupLayout.Alignment.LEADING, pBronInternToLinksOrigineelLayout.createSequentialGroup()
                        .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(jLabel28)
                            .addComponent(tfBILODbPass, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(tfBILOProjectName, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel29))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(tfBILODeleteSource, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel33))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(pBronInternToLinksOrigineelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(bBronInternToLinksOrigineel)
                            .addComponent(tfBILOdelay, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel46))
                        .addGap(18, 18, 18)
                        .addComponent(cbOrunCleaned))
                    .addComponent(jScrollPane3, javax.swing.GroupLayout.DEFAULT_SIZE, 563, Short.MAX_VALUE))
                .addContainerGap())
        );

        tpMain.addTab("ORIGINAL", pBronInternToLinksOrigineel);

        pLOLC.setName("pLOLC"); // NOI18N

        tbLOLCurl.setText("localhost");
        tbLOLCurl.setName("tbLOLCurl"); // NOI18N

        jLabel4.setText("Database Url");
        jLabel4.setName("jLabel4"); // NOI18N

        tbLOLCuser.setText("links");
        tbLOLCuser.setName("tbLOLCuser"); // NOI18N

        tbLOLCpass.setText("mslinks");
        tbLOLCpass.setName("tbLOLCpass"); // NOI18N

        jLabel42.setText("User");
        jLabel42.setName("jLabel42"); // NOI18N

        jLabel43.setText("Pass");
        jLabel43.setName("jLabel43"); // NOI18N

        tbLOLCBronNr.setName("tbLOLCBronNr"); // NOI18N

        jLabel44.setText("Bron nr.");
        jLabel44.setName("jLabel44"); // NOI18N

        bnLOLCstartProcess.setText("Start Process");
        bnLOLCstartProcess.setName("bnLOLCstartProcess"); // NOI18N
        bnLOLCstartProcess.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bnLOLCstartProcessActionPerformed(evt);
            }
        });

        jScrollPane6.setName("jScrollPane6"); // NOI18N

        taLOLCoutput.setColumns(20);
        taLOLCoutput.setRows(5);
        taLOLCoutput.setName("taLOLCoutput"); // NOI18N
        jScrollPane6.setViewportView(taLOLCoutput);

        jLabel45.setFont(new java.awt.Font("Tahoma", 1, 11)); // NOI18N
        jLabel45.setText(" Latest information and Log (below)");
        jLabel45.setName("jLabel45"); // NOI18N

        tbLOLClatestOutput.setName("tbLOLClatestOutput"); // NOI18N

        jLabel51.setFont(new java.awt.Font("Tahoma", 1, 11)); // NOI18N
        jLabel51.setText("Tasks:");
        jLabel51.setName("jLabel51"); // NOI18N

        cbCdoOpmerkingen.setText("Parse Remarks");
        cbCdoOpmerkingen.setName("cbCdoOpmerkingen"); // NOI18N

        cbCdoNames.setSelected(true);
        cbCdoNames.setText("Names");
        cbCdoNames.setName("cbCdoNames"); // NOI18N

        cbCdoStaatSex.setSelected(true);
        cbCdoStaatSex.setText("Status and sex");
        cbCdoStaatSex.setName("cbCdoStaatSex"); // NOI18N

        cbCdoDates.setSelected(true);
        cbCdoDates.setText("Dates");
        cbCdoDates.setName("cbCdoDates"); // NOI18N

        cbCdoJaarLeeftijd.setSelected(true);
        cbCdoJaarLeeftijd.setText("Jaar Leeftijd");
        cbCdoJaarLeeftijd.setName("cbCdoJaarLeeftijd"); // NOI18N

        cbCdoVernieuwen.setSelected(true);
        cbCdoVernieuwen.setText("Remove previous data");
        cbCdoVernieuwen.setName("cbCdoVernieuwen"); // NOI18N

        cbCdoType.setSelected(true);
        cbCdoType.setText("Type");
        cbCdoType.setName("cbCdoType"); // NOI18N

        cbLOLCdoSequentie.setSelected(true);
        cbLOLCdoSequentie.setText("Sequentie");
        cbLOLCdoSequentie.setName("cbLOLCdoSequentie"); // NOI18N

        cbCdoLocations.setSelected(true);
        cbCdoLocations.setText("Locations");
        cbCdoLocations.setName("cbCdoLocations"); // NOI18N

        cbCdoRelation.setText("Relation");
        cbCdoRelation.setName("cbCdoRelation"); // NOI18N

        cbLOLCdoMinMaxDate.setSelected(true);
        cbLOLCdoMinMaxDate.setText("Min Max Date");
        cbLOLCdoMinMaxDate.setName("cbLOLCdoMinMaxDate"); // NOI18N

        cbLOLCdoMinMaxMarriage.setSelected(true);
        cbLOLCdoMinMaxMarriage.setText("Min Max Marriage");
        cbLOLCdoMinMaxMarriage.setName("cbLOLCdoMinMaxMarriage"); // NOI18N

        cbLOLCdoPartsToFullDate.setSelected(true);
        cbLOLCdoPartsToFullDate.setText("Parts to Full Date");
        cbLOLCdoPartsToFullDate.setName("cbLOLCdoPartsToFullDate"); // NOI18N

        cbCdoDaysSinceBegin.setSelected(true);
        cbCdoDaysSinceBegin.setText("Days since begin");
        cbCdoDaysSinceBegin.setName("cbCdoDaysSinceBegin"); // NOI18N

        cbLOLCdoRole.setSelected(true);
        cbLOLCdoRole.setText("Role");
        cbLOLCdoRole.setName("cbLOLCdoRole"); // NOI18N

        cbLOLCdoPostTasks.setSelected(true);
        cbLOLCdoPostTasks.setText("POST TASKS");
        cbLOLCdoPostTasks.setName("cbLOLCdoPostTasks"); // NOI18N

        cbCdoPrematch.setText("RUN PREMATCH");
        cbCdoPrematch.setName("cbCdoPrematch"); // NOI18N

        cbCdoPreBasicNames.setText("Basic names Temp");
        cbCdoPreBasicNames.setName("cbCdoPreBasicNames"); // NOI18N

        javax.swing.GroupLayout pLOLCLayout = new javax.swing.GroupLayout(pLOLC);
        pLOLC.setLayout(pLOLCLayout);
        pLOLCLayout.setHorizontalGroup(
            pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pLOLCLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(pLOLCLayout.createSequentialGroup()
                        .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                            .addComponent(cbCdoPreBasicNames, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(cbCdoLocations, javax.swing.GroupLayout.PREFERRED_SIZE, 162, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addGroup(javax.swing.GroupLayout.Alignment.LEADING, pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING, false)
                                .addComponent(cbCdoNames, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                .addComponent(cbCdoOpmerkingen, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                            .addComponent(cbLOLCdoRole, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.PREFERRED_SIZE, 93, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(cbCdoJaarLeeftijd, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(cbCdoRelation, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.PREFERRED_SIZE, 101, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(cbLOLCdoSequentie, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.PREFERRED_SIZE, 91, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(cbCdoType, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.PREFERRED_SIZE, 73, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(cbCdoStaatSex, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.PREFERRED_SIZE, 113, javax.swing.GroupLayout.PREFERRED_SIZE))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(cbLOLCdoMinMaxDate)
                            .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING, false)
                                .addComponent(tbLOLCuser, javax.swing.GroupLayout.Alignment.LEADING)
                                .addComponent(tbLOLCpass, javax.swing.GroupLayout.Alignment.LEADING)
                                .addComponent(tbLOLCurl, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.PREFERRED_SIZE, 1, Short.MAX_VALUE)
                                .addComponent(tbLOLCBronNr, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.PREFERRED_SIZE, 154, javax.swing.GroupLayout.PREFERRED_SIZE))
                            .addComponent(cbLOLCdoMinMaxMarriage)
                            .addComponent(cbLOLCdoPartsToFullDate)
                            .addComponent(cbCdoDaysSinceBegin)
                            .addGroup(pLOLCLayout.createSequentialGroup()
                                .addComponent(cbCdoDates, javax.swing.GroupLayout.PREFERRED_SIZE, 122, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addGap(67, 67, 67)
                                .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                                    .addGroup(pLOLCLayout.createSequentialGroup()
                                        .addComponent(cbLOLCdoPostTasks, javax.swing.GroupLayout.PREFERRED_SIZE, 104, javax.swing.GroupLayout.PREFERRED_SIZE)
                                        .addGap(10, 10, 10))
                                    .addComponent(cbCdoPrematch, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)))))
                    .addComponent(bnLOLCstartProcess, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addGroup(pLOLCLayout.createSequentialGroup()
                        .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(jLabel44)
                            .addComponent(jLabel4)
                            .addComponent(jLabel42)
                            .addComponent(jLabel43)
                            .addComponent(jLabel51, javax.swing.GroupLayout.PREFERRED_SIZE, 113, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(cbCdoVernieuwen, javax.swing.GroupLayout.PREFERRED_SIZE, 155, javax.swing.GroupLayout.PREFERRED_SIZE))
                        .addGap(336, 336, 336)))
                .addGap(6, 6, 6)
                .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(jLabel45)
                    .addComponent(tbLOLClatestOutput)
                    .addComponent(jScrollPane6, javax.swing.GroupLayout.PREFERRED_SIZE, 628, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(1357, 1357, 1357))
        );
        pLOLCLayout.setVerticalGroup(
            pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pLOLCLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                    .addComponent(jLabel44)
                    .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                        .addComponent(tbLOLCBronNr, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addComponent(jLabel45)))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel4)
                    .addComponent(tbLOLCurl, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(tbLOLClatestOutput, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(6, 6, 6)
                .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(pLOLCLayout.createSequentialGroup()
                        .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(jLabel42)
                            .addComponent(tbLOLCuser, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(jLabel43)
                            .addComponent(tbLOLCpass, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                        .addGap(29, 29, 29)
                        .addComponent(jLabel51)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(cbCdoVernieuwen)
                            .addComponent(cbLOLCdoPostTasks)
                            .addComponent(cbCdoDates))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(cbCdoPrematch)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(cbCdoPreBasicNames)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(cbCdoOpmerkingen)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(cbCdoNames)
                        .addGap(118, 118, 118)
                        .addComponent(cbCdoLocations)
                        .addGap(46, 46, 46)
                        .addComponent(cbCdoStaatSex)
                        .addGap(2, 2, 2)
                        .addComponent(cbCdoType)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(cbLOLCdoMinMaxDate)
                            .addComponent(cbLOLCdoSequentie))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(cbLOLCdoMinMaxMarriage)
                            .addComponent(cbCdoRelation))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(cbLOLCdoPartsToFullDate)
                            .addComponent(cbCdoJaarLeeftijd))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(pLOLCLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(cbCdoDaysSinceBegin)
                            .addComponent(cbLOLCdoRole))
                        .addGap(5, 5, 5)
                        .addComponent(bnLOLCstartProcess))
                    .addComponent(jScrollPane6))
                .addGap(584, 584, 584))
        );

        tpMain.addTab("CLEANED", pLOLC);

        jPanel1.setName("jPanel1"); // NOI18N

        cbPdoUniqueNameTables.setText("UNIQUE TABLES");
        cbPdoUniqueNameTables.setName("cbPdoUniqueNameTables"); // NOI18N

        cbPdoLevenshtein.setText("LEVENSHTEIN");
        cbPdoLevenshtein.setName("cbPdoLevenshtein"); // NOI18N

        cbPdoCreateBaseTable.setText("BASE TABLE");
        cbPdoCreateBaseTable.setName("cbPdoCreateBaseTable"); // NOI18N
        cbPdoCreateBaseTable.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                cbPdoCreateBaseTableActionPerformed(evt);
            }
        });

        cbPdoSplitName.setText("SPLIT NAMES");
        cbPdoSplitName.setName("cbPdoSplitName"); // NOI18N

        bnPstartProcess.setText("START");
        bnPstartProcess.setName("bnPstartProcess"); // NOI18N
        bnPstartProcess.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bnPstartProcessActionPerformed(evt);
            }
        });

        jScrollPane7.setName("jScrollPane7"); // NOI18N

        taPresult.setColumns(20);
        taPresult.setRows(5);
        taPresult.setName("taPresult"); // NOI18N
        jScrollPane7.setViewportView(taPresult);

        cbPdoNameToNo.setText("NAME TO NO");
        cbPdoNameToNo.setName("cbPdoNameToNo"); // NOI18N

        taPinfo.setName("taPinfo"); // NOI18N

        javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
        jPanel1.setLayout(jPanel1Layout);
        jPanel1Layout.setHorizontalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(cbPdoLevenshtein, javax.swing.GroupLayout.PREFERRED_SIZE, 261, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jScrollPane7, javax.swing.GroupLayout.DEFAULT_SIZE, 1101, Short.MAX_VALUE)
                    .addComponent(taPinfo, javax.swing.GroupLayout.DEFAULT_SIZE, 1101, Short.MAX_VALUE)
                    .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, jPanel1Layout.createSequentialGroup()
                        .addComponent(cbPdoNameToNo, javax.swing.GroupLayout.PREFERRED_SIZE, 261, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 733, Short.MAX_VALUE)
                        .addComponent(bnPstartProcess, javax.swing.GroupLayout.PREFERRED_SIZE, 107, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(jPanel1Layout.createSequentialGroup()
                        .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(cbPdoSplitName, javax.swing.GroupLayout.PREFERRED_SIZE, 261, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(cbPdoUniqueNameTables, javax.swing.GroupLayout.PREFERRED_SIZE, 261, javax.swing.GroupLayout.PREFERRED_SIZE))
                        .addGap(71, 71, 71)
                        .addComponent(cbPdoCreateBaseTable, javax.swing.GroupLayout.PREFERRED_SIZE, 261, javax.swing.GroupLayout.PREFERRED_SIZE)))
                .addContainerGap())
        );
        jPanel1Layout.setVerticalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(cbPdoSplitName)
                    .addComponent(cbPdoCreateBaseTable))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(cbPdoUniqueNameTables)
                .addGap(23, 23, 23)
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(jPanel1Layout.createSequentialGroup()
                        .addGap(25, 25, 25)
                        .addComponent(bnPstartProcess))
                    .addGroup(jPanel1Layout.createSequentialGroup()
                        .addComponent(cbPdoLevenshtein)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addComponent(cbPdoNameToNo)))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(taPinfo, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jScrollPane7, javax.swing.GroupLayout.DEFAULT_SIZE, 479, Short.MAX_VALUE)
                .addContainerGap())
        );

        tpMain.addTab("PREMATCH", jPanel1);

        fileMenu.setText("File");

        openMenuItem.setText("Open");
        openMenuItem.setName("openMenuItem"); // NOI18N
        openMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                openMenuItemActionPerformed(evt);
            }
        });
        fileMenu.add(openMenuItem);

        saveMenuItem.setText("Save");
        saveMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                saveMenuItemActionPerformed(evt);
            }
        });
        fileMenu.add(saveMenuItem);

        exitMenuItem.setText("Exit");
        exitMenuItem.setName("exitMenuItem"); // NOI18N
        exitMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                exitMenuItemActionPerformed(evt);
            }
        });
        fileMenu.add(exitMenuItem);

        menuBar.add(fileMenu);

        editMenu.setText("Edit");

        cutMenuItem.setText("Cut");
        editMenu.add(cutMenuItem);

        copyMenuItem.setText("Copy");
        editMenu.add(copyMenuItem);

        pasteMenuItem.setText("Paste");
        editMenu.add(pasteMenuItem);

        deleteMenuItem.setText("Delete");
        editMenu.add(deleteMenuItem);

        menuBar.add(editMenu);

        helpMenu.setText("Help");

        contentsMenuItem.setText("Contents");
        helpMenu.add(contentsMenuItem);

        aboutMenuItem.setText("About");
        helpMenu.add(aboutMenuItem);

        menuBar.add(helpMenu);

        setJMenuBar(menuBar);

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(tpMain, javax.swing.GroupLayout.PREFERRED_SIZE, 1126, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addComponent(tpMain, javax.swing.GroupLayout.PREFERRED_SIZE, 675, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void createDomProject() {
        doc = moduleMain.LinksSpecific.createDomDocument();
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
                // TODO: Schrijf naar systeemlog
            }
        }
    }

    // alle selectors
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
            lMAINProjects.setListData(al.toArray());
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

    private void bBronInternToLinksOrigineelActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bBronInternToLinksOrigineelActionPerformed
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

    private void bCsvToDbStartActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bCsvToDbStartActionPerformed

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

    private void bCSVPathOutpuFileActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bCSVPathOutpuFileActionPerformed
        String selectedPath = LinksSpecific.SelectFileWithCheck("csv");
        tfCSVOutputFile.setText(selectedPath);
}//GEN-LAST:event_bCSVPathOutpuFileActionPerformed

    private void bCSVGetHeaderActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bCSVGetHeaderActionPerformed
        // TODO add your handling code here:
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

    private void lMAINProjectsValueChanged(javax.swing.event.ListSelectionEvent evt) {//GEN-FIRST:event_lMAINProjectsValueChanged
        // TODO add your handling code here:
        Element el = (Element) doc.getDocumentElement().getChildNodes().item(lMAINProjects.getSelectedIndex());

        tMAINProjectName.setText(el.getAttribute("name"));
        tMAINDbLocation.setText(el.getAttribute("url"));
        tMAINUser.setText(el.getAttribute("user"));
        tMAINPass.setText(el.getAttribute("pass"));
        taMAINOpmerkingen.setText(el.getAttribute("comment"));
    }//GEN-LAST:event_lMAINProjectsValueChanged

    private void bMAINSaveActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bMAINSaveActionPerformed
        // TODO add your handling code here:
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
            lMAINProjects.setListData(al.toArray());
        } catch (Exception e) {
        }
        lMAINProjects.setSelectedIndex(al.size() - 1);
    }//GEN-LAST:event_bMAINSaveActionPerformed

    private void bMAINDeleteActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bMAINDeleteActionPerformed
        // TODO add your handling code here:
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
            lMAINProjects.setListData(al.toArray());
        } catch (Exception e) {
        }
}//GEN-LAST:event_bMAINDeleteActionPerformed

    private void bMAINAddActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bMAINAddActionPerformed


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
            lMAINProjects.setListData(al.toArray());
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

        bnLOLCstartProcessActionPerformed(null);

    }

    private void bnLOLCstartProcessActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bnLOLCstartProcessActionPerformed

        // new DoSet
        DoSet dos = new DoSet();

        // check selection
        if (cbCdoVernieuwen.isSelected()) {
            dos.setDoRenewData(true);
        }
        
        if (cbCdoNames.isSelected()) {
            dos.setDoNames(true);
        }
        if (cbCdoDates.isSelected()) {
            dos.setDoDates(true);
        }
        if (cbCdoOpmerkingen.isSelected()) {
            dos.setDoRemarks(true);
        }
        if (cbCdoStaatSex.isSelected()) {
            dos.setDoStatusSex(true);
        }
        if (cbCdoJaarLeeftijd.isSelected()) {
            dos.setDoAgeYear(true);
        }
        if (cbCdoLocations.isSelected()) {
            dos.setDoLocations(true);
        }
        if (cbCdoType.isSelected()) {
            dos.setDoType(true);
        }
        if (cbLOLCdoSequentie.isSelected()) {
            dos.setDoSequence(true);
        }
        if (cbLOLCdoPartsToFullDate.isSelected()) {
            dos.setDoPartsToFullDate(true);
        }
        if (cbCdoDaysSinceBegin.isSelected()) {
            dos.setDoDaysSinceBegin(true);
        }
        if (cbLOLCdoMinMaxDate.isSelected()) {
            dos.setDoMinMaxDate(true);
        }
        if (cbLOLCdoMinMaxMarriage.isSelected()) {
            dos.setDoMinMaxMarriage(true);
        }
        if (cbCdoRelation.isSelected()) {
            dos.setDoRelation(true);
        }
        if (cbLOLCdoRole.isSelected()) {
            dos.setDoRole(true);
        }
        if (cbLOLCdoPostTasks.isSelected()) {
            dos.setDoPostTasks(true);
        }
        if (cbCdoPrematch.isSelected()) {
            dos.setDoPrematch(true);
        }
        if (cbCdoPreBasicNames.isSelected()) {
            dos.setDoPreBasicNames(true);
        }
        int bronNr = 0;
        if (!tbLOLCBronNr.getText().isEmpty()) {
            bronNr = Integer.parseInt(tbLOLCBronNr.getText());
        }

        LinksCleaned lolc = new LinksCleaned(
                bronNr,
                tbLOLCurl.getText(),
                tbLOLCuser.getText(),
                tbLOLCpass.getText(),
                tbLOLClatestOutput,
                taLOLCoutput,
                dos,
                this);
        lolc.start();
    }//GEN-LAST:event_bnLOLCstartProcessActionPerformed

    private void tfBILODeleteSourceKeyReleased(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_tfBILODeleteSourceKeyReleased
        // TODO add your handling code here:
    }//GEN-LAST:event_tfBILODeleteSourceKeyReleased

    public void firePrematch() {
        bnPstartProcessActionPerformed(null);
    }

    private void bnPstartProcessActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bnPstartProcessActionPerformed

        boolean a = false;
        boolean b = false;
        boolean c = false;
        boolean d = false;
        boolean e = false;

        if (cbPdoSplitName.isSelected()) {
            a = true;
        }
        if (cbPdoUniqueNameTables.isSelected()) {
            b = true;
        }
        if (cbPdoLevenshtein.isSelected()) {
            c = true;
        }
        if (cbPdoNameToNo.isSelected()) {
            d = true;
        }
        if (cbPdoCreateBaseTable.isSelected()) {
            e = true;
        }

        try{
        LinksPrematch lpm = new LinksPrematch(taPresult, taPinfo, a, b, c, d, e);
        
        lpm.start();
        
        }
        catch(Exception ex){
            taPinfo.setText( "ERROR: "  + ex.getMessage() ) ;
        }
    }//GEN-LAST:event_bnPstartProcessActionPerformed

    private void cbPdoCreateBaseTableActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_cbPdoCreateBaseTableActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_cbPdoCreateBaseTableActionPerformed

    private void tfCTDDbPassActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_tfCTDDbPassActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_tfCTDDbPassActionPerformed

    private void rbBebiIndexGenliasActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_rbBebiIndexGenliasActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_rbBebiIndexGenliasActionPerformed

    /**
     * Main method, to start Gui manager
     * @param args
     */
    public static void main(String args[]) {
        java.awt.EventQueue.invokeLater(new Runnable() {

            public void run() {
                new ManagerGui().setVisible(true);
            }
        });
    }
    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JMenuItem aboutMenuItem;
    private javax.swing.JButton bBronInternToLinksOrigineel;
    private javax.swing.JButton bCSVBeginFilter;
    private javax.swing.JButton bCSVGetHeader;
    private javax.swing.JButton bCSVPath;
    private javax.swing.JButton bCSVPathOutpuFile;
    private javax.swing.JButton bCsvToDbStart;
    private javax.swing.JButton bMAINActivate;
    private javax.swing.JButton bMAINAdd;
    private javax.swing.JButton bMAINClear;
    private javax.swing.JButton bMAINDelete;
    private javax.swing.JButton bMAINSave;
    private javax.swing.JButton bSelectCsvToDbCsvFile;
    private javax.swing.JButton bnLOLCstartProcess;
    private javax.swing.JButton bnPstartProcess;
    private javax.swing.ButtonGroup buttonGroup1;
    private javax.swing.ButtonGroup buttonGroup2;
    private javax.swing.JCheckBox cbCdoDates;
    private javax.swing.JCheckBox cbCdoDaysSinceBegin;
    private javax.swing.JCheckBox cbCdoJaarLeeftijd;
    private javax.swing.JCheckBox cbCdoLocations;
    private javax.swing.JCheckBox cbCdoNames;
    private javax.swing.JCheckBox cbCdoOpmerkingen;
    private javax.swing.JCheckBox cbCdoPreBasicNames;
    private javax.swing.JCheckBox cbCdoPrematch;
    private javax.swing.JCheckBox cbCdoRelation;
    private javax.swing.JCheckBox cbCdoStaatSex;
    private javax.swing.JCheckBox cbCdoType;
    private javax.swing.JCheckBox cbCdoVernieuwen;
    private javax.swing.JCheckBox cbLOLCdoMinMaxDate;
    private javax.swing.JCheckBox cbLOLCdoMinMaxMarriage;
    private javax.swing.JCheckBox cbLOLCdoPartsToFullDate;
    private javax.swing.JCheckBox cbLOLCdoPostTasks;
    private javax.swing.JCheckBox cbLOLCdoRole;
    private javax.swing.JCheckBox cbLOLCdoSequentie;
    private javax.swing.JCheckBox cbMAINbebi;
    private javax.swing.JCheckBox cbMAINbilo;
    private javax.swing.JCheckBox cbMAINlolc;
    private javax.swing.JCheckBox cbOrunCleaned;
    private javax.swing.JCheckBox cbPdoCreateBaseTable;
    private javax.swing.JCheckBox cbPdoLevenshtein;
    private javax.swing.JCheckBox cbPdoNameToNo;
    private javax.swing.JCheckBox cbPdoSplitName;
    private javax.swing.JCheckBox cbPdoUniqueNameTables;
    private javax.swing.JMenuItem contentsMenuItem;
    private javax.swing.JMenuItem copyMenuItem;
    private javax.swing.JMenuItem cutMenuItem;
    private javax.swing.JMenuItem deleteMenuItem;
    private javax.swing.JMenu editMenu;
    private javax.swing.JMenuItem exitMenuItem;
    private javax.swing.JMenu fileMenu;
    private javax.swing.JMenu helpMenu;
    private javax.swing.JButton jButton2;
    private javax.swing.JLabel jLabel1;
    private javax.swing.JLabel jLabel10;
    private javax.swing.JLabel jLabel11;
    private javax.swing.JLabel jLabel12;
    private javax.swing.JLabel jLabel13;
    private javax.swing.JLabel jLabel14;
    private javax.swing.JLabel jLabel15;
    private javax.swing.JLabel jLabel16;
    private javax.swing.JLabel jLabel17;
    private javax.swing.JLabel jLabel18;
    private javax.swing.JLabel jLabel19;
    private javax.swing.JLabel jLabel2;
    private javax.swing.JLabel jLabel20;
    private javax.swing.JLabel jLabel21;
    private javax.swing.JLabel jLabel22;
    private javax.swing.JLabel jLabel23;
    private javax.swing.JLabel jLabel24;
    private javax.swing.JLabel jLabel25;
    private javax.swing.JLabel jLabel26;
    private javax.swing.JLabel jLabel27;
    private javax.swing.JLabel jLabel28;
    private javax.swing.JLabel jLabel29;
    private javax.swing.JLabel jLabel3;
    private javax.swing.JLabel jLabel30;
    private javax.swing.JLabel jLabel31;
    private javax.swing.JLabel jLabel32;
    private javax.swing.JLabel jLabel33;
    private javax.swing.JLabel jLabel37;
    private javax.swing.JLabel jLabel38;
    private javax.swing.JLabel jLabel39;
    private javax.swing.JLabel jLabel4;
    private javax.swing.JLabel jLabel40;
    private javax.swing.JLabel jLabel41;
    private javax.swing.JLabel jLabel42;
    private javax.swing.JLabel jLabel43;
    private javax.swing.JLabel jLabel44;
    private javax.swing.JLabel jLabel45;
    private javax.swing.JLabel jLabel46;
    private javax.swing.JLabel jLabel51;
    private javax.swing.JLabel jLabel7;
    private javax.swing.JLabel jLabel8;
    private javax.swing.JLabel jLabel9;
    private javax.swing.JPanel jPanel1;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JScrollPane jScrollPane2;
    private javax.swing.JScrollPane jScrollPane3;
    private javax.swing.JScrollPane jScrollPane4;
    private javax.swing.JScrollPane jScrollPane5;
    private javax.swing.JScrollPane jScrollPane6;
    private javax.swing.JScrollPane jScrollPane7;
    private javax.swing.JList lMAINProjects;
    private javax.swing.JMenuBar menuBar;
    private javax.swing.JMenuItem openMenuItem;
    private javax.swing.JPanel pBronInternToLinksOrigineel;
    private javax.swing.JPanel pCsv;
    private javax.swing.JPanel pCsvToDatabase;
    private javax.swing.JPanel pLOLC;
    private javax.swing.JPanel pMain;
    private javax.swing.JMenuItem pasteMenuItem;
    private javax.swing.JProgressBar pbDTCFile;
    private javax.swing.JProgressBar pbDTCTotal;
    private javax.swing.JRadioButton rbBebiIndexGeen;
    private javax.swing.JRadioButton rbBebiIndexGenlias;
    private javax.swing.JMenuItem saveMenuItem;
    private javax.swing.JTextField tMAINDbLocation;
    private javax.swing.JTextField tMAINPass;
    private javax.swing.JTextField tMAINPass1;
    private javax.swing.JTextField tMAINPass2;
    private javax.swing.JTextField tMAINPass3;
    private javax.swing.JTextField tMAINPass4;
    private javax.swing.JTextField tMAINProjectName;
    private javax.swing.JTextField tMAINUser;
    private javax.swing.JTextArea taBILOOutput;
    private javax.swing.JTextArea taCSVOutput;
    private javax.swing.JTextArea taCTDOutput;
    private javax.swing.JTextArea taLOLCoutput;
    private javax.swing.JTextArea taMAINOpmerkingen;
    private javax.swing.JTextField taPinfo;
    private javax.swing.JTextArea taPresult;
    private javax.swing.JTextField tbLOLCBronNr;
    private javax.swing.JTextField tbLOLClatestOutput;
    private javax.swing.JTextField tbLOLCpass;
    private javax.swing.JTextField tbLOLCurl;
    private javax.swing.JTextField tbLOLCuser;
    private javax.swing.JTextField tfBILODbLocation;
    private javax.swing.JTextField tfBILODbPass;
    private javax.swing.JTextField tfBILODbUser;
    private javax.swing.JTextField tfBILODeleteSource;
    private javax.swing.JTextField tfBILOLatestOutput;
    private javax.swing.JTextField tfBILOProjectName;
    private javax.swing.JTextField tfBILOdelay;
    private javax.swing.JTextField tfCSVCsvFile;
    private javax.swing.JTextField tfCSVFilterIndex;
    private javax.swing.JTextField tfCSVFilterText;
    private javax.swing.JTextField tfCSVInfo;
    private javax.swing.JTextField tfCSVLineData;
    private javax.swing.JTextField tfCSVOutputFile;
    private javax.swing.JTextField tfCTBCsvFiles;
    private javax.swing.JTextField tfCTDDbLocation;
    private javax.swing.JTextField tfCTDDbName;
    private javax.swing.JTextField tfCTDDbPass;
    private javax.swing.JTextField tfCTDDbUser;
    private javax.swing.JTextField tfCTDLatestOutput;
    private javax.swing.JTextField tfCTDLineNumber;
    private javax.swing.JTextField tfCTDPreFix;
    private javax.swing.JTextField tfCTDdelay;
    private javax.swing.JTabbedPane tpMain;
    // End of variables declaration//GEN-END:variables
}
