/*
 *-------------------
 * The ASHDatabasePG10.java is part of ASH Viewer
 *-------------------
 *
 * ASH Viewer is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ASH Viewer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with ASH Viewer.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (c) 2009, Alex Kardapolov, All rights reserved.
 *
 */
package org.ash.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.math.BigInteger;

import org.ash.conn.model.Model;
import org.ash.datamodel.ActiveSessionHistory;
import org.ash.datamodel.AshIdTime;
import org.ash.datamodel.AshSqlIdTypeText;
import org.ash.datamodel.AshSqlPlanDetail;
import org.ash.datamodel.AshSqlPlanParent;
import org.ash.datatemp.SessionsTemp;
import org.ash.datatemp.SqlsTemp;
import org.ash.explainplanmodel.ExplainPlanModel10g2;
import org.ash.util.Utils;
import org.jdesktop.swingx.treetable.TreeTableModel;
import org.jfree.data.xy.CategoryTableXYDataset;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Sequence;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;

// dcvetkov import
import org.ash.util.Options;
import java.security.MessageDigest;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

/**
 * The Class ASHDatabasePG10.
 */
public class ASHDatabasePG10 extends ASHDatabase {

    public static String GetSQLCommandType(String nsql) {

        String[] array = nsql.split(" ", -1);

        for (int i = 0; i < array.length; i++) {
            if (array[i].equals("select")) {
                return "SELECT";
            } else if (array[i].equals("insert")) {
                return "INSERT";
            } else if (array[i].equals("update")) {
                return "UPDATE";
            } else if (array[i].equals("insert")) {
                return "INSERT";
            } else if (array[i].equals("delete")) {
                return "DELETE";
            } else if (array[i].equals("drop")) {
                return "DROP";
            } else if (array[i].equals("truncate")) {
                return "TRUNCATE";
            } else if (array[i].equals("end")) {
                return "COMMIT";
            } else if (array[i].equals("begin")) {
                return "BEGIN";
            } else if (array[i].equals("vacuum")) {
                return "VACUUM";
            } else if (array[i].equals("autovacuum:")) {
                return "AUTOVACUUM";
            } else if (array[i].equals("analyze")) {
                return "ANALYZE";
            } else if (array[i].equals("copy")) {
                return "COPY";
            } else if (array[i].equals("backup")) {
                return "BACKUP";
            } else if (array[i].equals("wal")) {
                return "WAL EXCHANGE";
            } else if (array[i].equals("alter")) {
                if (array[i + 1].equals("index")) {
                    return "ALTER INDEX";
                } else if (array[i + 1].equals("table")) {
                    return "ALTER TABLE";
                } else {
                    return "ALTER";
                }
            } else if (array[i].equals("create")) {
                if (array[i + 1].equals("index")) {
                    return "CREATE INDEX";
                } else if (array[i + 1].equals("table")) {
                    return "CREATE TABLE";
                } else if (array[i + 1].equals("database")) {
                    return "CREATE DATABASE";
                } else {
                    return "CREATE";
                }
            }
            return "UNKNOWN (" + array[0] + ")";
        }

        return "";
    }

    public static String NormalizeSQL(String sql) {
        sql = sql.replaceAll("\\(", " ( ");
        sql = sql.replaceAll("\\)", " ) ");
        sql = sql.replaceAll(",", " , ");
        sql = sql.replaceAll("=", " = ");
        sql = sql.replaceAll("=", " = ");
        sql = sql.replaceAll("<", " < ");
        sql = sql.replaceAll(">", " > ");
        sql = sql.replaceAll(">=", " >= ");
        sql = sql.replaceAll("<=", " <= ");
        sql = sql.replaceAll("\\n", " ");
        sql = sql.replaceAll(";", "");
        sql = sql.replaceAll("[ ]+", " ");
        sql = sql.toLowerCase().trim();
        String[] array = sql.split(" ", -1);
        int bvn = 0;
        String nsql = "";
        for (int i = 0; i < array.length; i++) {
            if (array[i].matches("-?\\d+(\\.\\d+)?")) {
                bvn++;
                array[i] = "$" + bvn;
            } else if ((array[i].charAt(0) == '\'') && (array[i].charAt(array[i].length() - 1) == '\'')) {
                bvn++;
                array[i] = "$" + bvn;
            }
            nsql += array[i] + " ";
        }
        return nsql;
    }

    public static String md5Custom(String st) {
        MessageDigest messageDigest = null;
        byte[] digest = new byte[0];

        try {
            messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.reset();
            messageDigest.update(st.getBytes());
            digest = messageDigest.digest();
        } catch (java.security.NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        BigInteger bigInt = new BigInteger(1, digest);
        String md5Hex = bigInt.toString(16).substring(0, 13);

        return md5Hex;
    }

    /**
     * The model.
     */
    private Model model;

    /**
     * The sequence.
     */
    private Sequence seq;

    /**
     * The BDB store.
     */
    private EntityStore store;

    /**
     * The BDB dao.
     */
    private AshDataAccessor dao;

    /**
     * The range for sqls and sessions temp (gantt)
     */
    private int rangeHalf = 7500;

    /**
     * The query ash.
     */
    private String queryASH = "SELECT current_timestamp, "
            + "datid, datname, "
            + "pid, usesysid, "
            + "coalesce(usename, backend_type, 'unknown') as usename, "
            + "application_name, "
            + "coalesce(client_hostname, client_addr::text, 'localhost') as client_hostname, "
            + "wait_event_type, wait_event, state, backend_type, query "
            + "from pg_stat_activity "
            + "where state='active' and pid != pg_backend_pid()";

    private String FILESEPARATOR = System.getProperty("file.separator");

    /**
     * The k for sample_id after reconnect
     */
    private long kReconnect = 0;

    /**
     * Is reconnect
     */
    private boolean isReconnect = false;

    /**
     * Instantiates a new PG10 Database
     *
     * @param model0 the model0
     */
    public ASHDatabasePG10(Model model0) {
        super(model0);
        this.model = model0;
        this.store = super.getStore();
        this.dao = super.getDao();
    }

    /* (non-Javadoc)
     * @see org.ash.database.DatabaseMain#loadToLocalBDB()
     */
    public void loadToLocalBDB() {

        // Get max value of ash
        super.initializeVarsOnLoad();

        // Load data to activeSessionHistoryById
        loadAshDataToLocal();

        // Load data locally
        super.loadToSubByEventAnd10Sec();

    }

    /* (non-Javadoc)
     * @see org.ash.database.DatabaseMain#loadToLocalBDBCollector()
     */
    public synchronized void loadToLocalBDBCollector() {
        // Get max value of ash
        super.initializeVarsAfterLoad9i();

        // Load data to activeSessionHistoryById
        loadAshDataToLocal();
    }

    /* (non-Javadoc)
     * @see org.ash.database.DatabaseMain#loadDataToChartPanelDataSet(org.jfree.data.xy.CategoryTableXYDataset)
     */
    public void loadDataToChartPanelDataSet(CategoryTableXYDataset _dataset) {
        super.loadDataToChartPanelDataSet(_dataset);
    }

    /* (non-Javadoc)
     * @see org.ash.database.DatabaseMain#updateDataToChartPanelDataSet()
     */
    public void updateDataToChartPanelDataSet() {
        super.updateDataToChartPanelDataSet();
    }

    /**
     * Load ash data to local.
     */
    private void loadAshDataToLocal() {

        ResultSet resultSetAsh = null;
        PreparedStatement statement = null;
        Connection conn = null;

        // Get sequence activeSessionHistoryId
        try {
            seq = store.getSequence("activeSessionHistoryId");
        } catch (DatabaseException e) {
            // e.printStackTrace();
        }

        try {

            if (model.getConnectionPool() != null) {

                conn = this.model.getConnectionPool().getConnection();

                statement = conn.prepareStatement(this.queryASH);

                // set ArraySize for current statement to improve performance
                statement.setFetchSize(5000);

                resultSetAsh = statement.executeQuery();

                while (resultSetAsh.next()) {

                    long activeSessionHistoryIdWait = 0;
                    try {
                        activeSessionHistoryIdWait = seq.get(null, 1);
                    } catch (DatabaseException e) {
                        e.printStackTrace();
                    }

                    // Calculate sample time
                    java.sql.Timestamp PGDateSampleTime = resultSetAsh.getTimestamp("current_timestamp");
                    Long valueSampleIdTimeLongWait = (new Long(PGDateSampleTime.getTime()));

                    Long sessionId = resultSetAsh.getLong("pid");
                    String sessionType = resultSetAsh.getString("backend_type");

                    String ConnDBName = getParameter("ASH.db");
                    String databaseName = resultSetAsh.getString("datname");

                    Long userId = resultSetAsh.getLong("usesysid");
                    String userName = resultSetAsh.getString("usename");

                    String program = resultSetAsh.getString("application_name");

                    String query_text = resultSetAsh.getString("query");
                    if ((query_text == null) || (query_text.equals(""))) {
                        if (program.equals("pg_basebackup")) {
                            query_text = "backup";
                        } else if (program.equals("walreceiver") || program.equals("walsender")) {
                            query_text = "wal";
                        } else if (sessionType.equals("walreceiver") || sessionType.equals("walsender")) {
                            query_text = "wal";
                        } else {
                            query_text = "empty";
                        }
                    }
                    String query_text_norm = NormalizeSQL(query_text);
                    String command_type = GetSQLCommandType(query_text_norm);
                    String sqlId = md5Custom(query_text_norm);

                    /* System.out.println(query_text);
                    System.out.println(query_text_norm);
                    System.out.println(sqlId);
                    System.out.println("");
                     */
                    String hostname = resultSetAsh.getString("client_hostname");
                    String event = resultSetAsh.getString("wait_event");
                    String waitClass = resultSetAsh.getString("wait_event_type");

                    if (waitClass == null) {
                        waitClass = "CPU";
                        event = "CPU";
                    }

                    Double waitClassId = 0.0;

                    if (waitClass.equals("CPU")) {
                        waitClassId = 0.0;
                    } else if (waitClass.equals("IO")) {
                        waitClassId = 1.0;
                    } else if (waitClass.equals("Lock")) {
                        waitClassId = 2.0;
                    } else if (waitClass.equals("LWLock")) {
                        waitClassId = 3.0;
                    } else if (waitClass.equals("BufferPin")) {
                        waitClassId = 4.0;
                    } else if (waitClass.equals("Activity")) {
                        waitClassId = 5.0;
                    } else if (waitClass.equals("Extension")) {
                        waitClassId = 6.0;
                    } else if (waitClass.equals("Client")) {
                        waitClassId = 7.0;
                    } else if (waitClass.equals("IPC")) {
                        waitClassId = 8.0;
                    } else if (waitClass.equals("Timeout")) {
                        waitClassId = 9.0;
                    }

                    // Create row for wait event
                    try {
                        dao.ashById.putNoOverwrite(new AshIdTime(valueSampleIdTimeLongWait, valueSampleIdTimeLongWait.doubleValue()));
                    } catch (DatabaseException e) {
                        e.printStackTrace();
                    }

                    // Load data for active session history (wait event)
                    try {
                        dao.activeSessionHistoryById
                                .putNoReturn(new ActiveSessionHistory(
                                        activeSessionHistoryIdWait,
                                        valueSampleIdTimeLongWait,
                                        sessionId, sessionType, userId, userName, sqlId, command_type,
                                        event, waitClass, waitClassId, program, hostname));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    // dcvetkov: Load data for sqlid: command type and query text
                    try {
                        dao.ashSqlIdTypeTextId.putNoReturn(new AshSqlIdTypeText(sqlId, command_type, query_text_norm));
                    } catch (DatabaseException e) {
                        e.printStackTrace();
                    }

                    // dcvetkov: explain plan
                    if (command_type.equals("SELECT") || command_type.equals("UPDATE") || command_type.equals("DELETE") || command_type.equals("INSERT")) {

                        String planFileName = Options.getInstance().getPlanDir() + FILESEPARATOR + sqlId + ".plan";
                        String textFileName = Options.getInstance().getPlanDir() + FILESEPARATOR + sqlId + ".sql";
                        File planFile = new File(planFileName);
                        // если mtime файла старше часа - запрашиваем план заново
                        if (System.currentTimeMillis() - planFile.lastModified() > 3600000) {

                            String plan = "EXPLAIN PLAN FOR SQLID " + sqlId + " (" + command_type + "):\n"
                                    + "------------------------------------------------------------\n\n";

                            if (ConnDBName.equals(databaseName)) {

                                ResultSet rs1 = null;
                                PreparedStatement st1 = null;
                                try {
                                    st1 = conn.prepareStatement("EXPLAIN " + query_text);
                                    rs1 = st1.executeQuery();
                                } catch (Exception e) {
                                    plan = plan + e.toString();
                                    // System.out.println("--- Error while getting plan for sqlid = " + sqlId + " (" + command_type + "):\n");
                                    // e.printStackTrace();
                                }

                                if (rs1 != null) {
                                    while (rs1.next()) {
                                        plan = plan + rs1.getString(1) + "\n";
                                    }
                                    rs1.close();
                                }

                                if (st1 != null) {
                                    st1.close();
                                }
                            } else {
                                plan = plan + "You are connected to database " + ConnDBName + " while query " + sqlId + " executed in database " + databaseName;
                                plan = plan + ".\nSo sorry.";
                            }

                            if (ConnDBName.length() > 0) {
                                Writer writer = null;
                                try {
                                    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(planFileName), "utf-8"));
                                    writer.write(plan);
                                } catch (IOException ex) {
                                } finally {
                                    try {
                                        writer.close();
                                    } catch (Exception ex) {/*ignore*/                                    }
                                }

                                writer = null;
                                try {
                                    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(textFileName), "utf-8"));
                                    writer.write(query_text);
                                } catch (IOException ex) {
                                } finally {
                                    try {
                                        writer.close();
                                    } catch (Exception ex) {/*ignore*/
                                    }
                                }
                            }
                        }
                    }
                }
                if (conn != null) {
                    model.getConnectionPool().free(conn);
                }
            } else {
                // Connect is lost
                setReconnect(true);
                model.closeConnectionPool();
                model.connectionPoolInitReconnect();
            }

        } catch (SQLException e) {
            System.out.println("SQL Exception occured: " + e.getMessage());
            model.closeConnectionPool();
        } finally {
            if (resultSetAsh != null) {
                try {
                    resultSetAsh.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /*
     * (non-Javadoc)
     *
     * @see org.ash.database.DatabaseMain#calculateSqlsSessionsData(double,
     *      double)
     */
    public void calculateSqlsSessionsData(double beginTime, double endTime,
            String eventFlag) {

        try {

            SqlsTemp tmpSqlsTemp = null;
            SessionsTemp tmpSessionsTemp = null;

            if (eventFlag.equalsIgnoreCase("All")) {
                tmpSqlsTemp = super.getSqlsTemp();
                tmpSessionsTemp = super.getSessionsTemp();
            } else {
                tmpSqlsTemp = super.getSqlsTempDetail();
                tmpSessionsTemp = super.getSessionsTempDetail();
            }

            // get sample id's for beginTime and endTime
            EntityCursor<AshIdTime> ashSampleIds;
            ashSampleIds = dao.doRangeQuery(dao.ashBySampleTime, beginTime
                    - rangeHalf, true, endTime + rangeHalf, true);
            /* Iterate on Ash by SampleTime. */
            Iterator<AshIdTime> ashIter = ashSampleIds.iterator();

            while (ashIter.hasNext()) {

                AshIdTime ashSumMain = ashIter.next();

                // get rows from ActiveSessionHistory for samplId
                EntityCursor<ActiveSessionHistory> ActiveSessionHistoryCursor;
                ActiveSessionHistoryCursor = dao.doRangeQuery(
                        dao.activeSessionHistoryByAshId, ashSumMain
                                .getsampleId(), true, ashSumMain.getsampleId(),
                        true);
                Iterator<ActiveSessionHistory> ActiveSessionHistoryIter = ActiveSessionHistoryCursor
                        .iterator();

                while (ActiveSessionHistoryIter.hasNext()) {
                    ActiveSessionHistory ASH = ActiveSessionHistoryIter.next();

                    // sql data
                    String sqlId = ASH.getSqlId();
                    double waitClassId = ASH.getWaitClassId();
                    // session data
                    Long sessionId = (Long) ASH.getSessionId();
                    String sessionidS = sessionId.toString().trim();
                    Long useridL = (Long) ASH.getUserId();
                    String usernameSess = ASH.getUserName();
                    String programSess = ASH.getProgram();
                    programSess = programSess + "@" + ASH.getHostname();
                    String waitClass = ASH.getWaitClass();
                    String eventName = ASH.getEvent();

                    // Exit when current eventClas != eventFlag
                    if (!eventFlag.equalsIgnoreCase("All")) {
                        if (waitClass != null && waitClass.equalsIgnoreCase(eventFlag)) {
                            this.loadDataToTempSqlSession(tmpSqlsTemp,
                                    tmpSessionsTemp, sqlId, waitClassId,
                                    sessionId, sessionidS, 0.0, "",
                                    useridL, usernameSess, programSess, true, eventName, 0);
                        }

                    } else {
                        this.loadDataToTempSqlSession(tmpSqlsTemp,
                                tmpSessionsTemp, sqlId, waitClassId,
                                sessionId, sessionidS, 0.0, "",
                                useridL, usernameSess, programSess, false, eventFlag, 0);
                    }
                }
                // Close cursor!!
                ActiveSessionHistoryCursor.close();
            }
            tmpSqlsTemp.set_sum();
            tmpSessionsTemp.set_sum();

            // Close cursor!!
            ashSampleIds.close();

        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }


    /*
     * (non-Javadoc)
     *
     * @see org.ash.database.DatabaseMain#loadCommandTypeFromDB(java.util.List)
     */
    public void loadSqlTextCommandTypeFromDB(List<String> arraySqlId) {

        // Load all sqlId
        ArrayList<String> sqlIdAll = new ArrayList<String>();

        Iterator<String> arraySqlIdIter = arraySqlId.iterator();
        while (arraySqlIdIter.hasNext()) {
            String sqlId = arraySqlIdIter.next();
            if (!isSqlTextExist(sqlId)) {
                sqlIdAll.add(sqlId);
            }
        }

        this.loadSqlTextSqlIdFromDB(sqlIdAll);
    }


    /*
     * (non-Javadoc)
     *
     * @see org.ash.database.DatabaseMain#isSqlPlanHashValueExist(java.lang.boolean)
     */
    public boolean isSqlPlanHashValueExist(Double sqlPlanHashValue, String sqlId) {
        boolean res = false;

        EntityCursor<AshSqlPlanDetail> ashSqlPlan = null;
        try {

            ashSqlPlan = dao.doRangeQuery(dao
                    .getAshSqlPlanHashValueDetail(), sqlPlanHashValue,
                    true, sqlPlanHashValue, true);
            Iterator<AshSqlPlanDetail> ashSqlPlanIter = ashSqlPlan
                    .iterator();

            while (ashSqlPlanIter.hasNext()) {
                AshSqlPlanDetail ashSqlPlanMain = ashSqlPlanIter.next();
                String sqlIdTmp = ashSqlPlanMain.getSqlId();

                if (sqlIdTmp.equalsIgnoreCase(sqlId)) {
                    res = true;
                    break;
                } else {
                    res = false;
                }
            }

            if (!res) {
                res = dao.getAshSqlPlanPKParent().contains(sqlPlanHashValue);
            }

        } catch (DatabaseException e) {
            res = false;
        } finally {
            try {
                ashSqlPlan.close();
            } catch (DatabaseException e) {
                e.printStackTrace();
            }
        }

        return res;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.ash.database.DatabaseMain#getSqlPlanHashValueBySqlId(java.lang.String)
     */
    public List<Double> getSqlPlanHashValueBySqlId(String sqlId) {

        List<Double> res = new ArrayList<Double>();

        EntityCursor<AshSqlPlanParent> ashSqlPlan = null;
        try {

            ashSqlPlan = dao.doRangeQuery(dao.getAshSqlPlanHashValueParent(),
                    sqlId, true, sqlId, true);
            Iterator<AshSqlPlanParent> ashSqlPlanIter = ashSqlPlan
                    .iterator();

            while (ashSqlPlanIter.hasNext()) {
                AshSqlPlanParent ashSqlPlanMain = ashSqlPlanIter.next();
                Double planHashValue = ashSqlPlanMain.getPlanHashValue();
                res.add(planHashValue);
            }

        } catch (DatabaseException e) {
            return res;
        } finally {
            try {
                ashSqlPlan.close();
            } catch (DatabaseException e) {
                e.printStackTrace();
            }
        }

        return res;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.ash.database.DatabaseMain#isSqlPlanHashValueExist(java.lang.String)
     */
    public boolean isSqlTextExist(String sqlId) {
        boolean res = false;
        try {
            res = dao.getAshSqlIdTypeTextId().contains(sqlId);
        } catch (DatabaseException e) {
            res = false;
        }

        return res;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.ash.database.DatabaseMain#isSqlPlanHashValueExist(ExplainPlanModel)
     */
    public TreeTableModel getSqlPlanModelByPlanHashValue(
            Double sqlPlanHashValue, String sqlId) {

        ExplainPlanModel10g2 model = null;
        ExplainPlanModel10g2.ExplainRow lastRow = null;
        EntityCursor<AshSqlPlanDetail> ashSqlPlan = null;
        long previousLevel = 1;
        boolean isRecalculateDepth = false;
        boolean exitFromCycle = false;
        boolean isChildNumberSaved = false;
        long childNumberBySql = 0;
        long ii = 0;

        while (!exitFromCycle) {
            exitFromCycle = true;

            try {
                ashSqlPlan = dao.doRangeQuery(dao
                        .getAshSqlPlanHashValueDetail(), sqlPlanHashValue,
                        true, sqlPlanHashValue, true);
                Iterator<AshSqlPlanDetail> ashSqlPlanIter = ashSqlPlan
                        .iterator();

                // Iterate
                while (ashSqlPlanIter.hasNext()) {
                    AshSqlPlanDetail ashSqlPlanMain = ashSqlPlanIter.next();
                    String sqlIdTmp = ashSqlPlanMain.getSqlId();
                    long childNumberTmp = ashSqlPlanMain.getChildNumber();

                    if (!isChildNumberSaved && sqlId.equalsIgnoreCase(sqlIdTmp)) {
                        childNumberBySql = childNumberTmp;
                        isChildNumberSaved = true;
                    } else {
                        if (!sqlId.equalsIgnoreCase(sqlIdTmp)) {
                            continue;
                        }
                    }

                    Long id = ashSqlPlanMain.getId();

                    if (id == ii && childNumberBySql == childNumberTmp) {
                        exitFromCycle = false;

                        String address = ashSqlPlanMain.getAddress();
                        Double hashValue = ashSqlPlanMain.getHashValue();
                        //String sqlId = ashSqlPlanMain.getSqlId();
                        //Double planHashValue = sqlPlanHashValue;
                        String childAddress = ashSqlPlanMain.getChildAddress();
                        Long childNumber = ashSqlPlanMain.getChildNumber();
                        String operation = ashSqlPlanMain.getOperation();
                        String options = ashSqlPlanMain.getOptions();
                        String objectNode = ashSqlPlanMain.getObjectNode();
                        Double object = ashSqlPlanMain.getObject();
                        String objectOwner = ashSqlPlanMain.getObjectOwner();
                        String objectName = ashSqlPlanMain.getObjectName();
                        String objectAlias = ashSqlPlanMain.getObjectAlias();
                        String objectType = ashSqlPlanMain.getObjectType();
                        String optimizer = ashSqlPlanMain.getOptimizer();
                        Long Id = ashSqlPlanMain.getId();
                        Long parentId = ashSqlPlanMain.getParentId();
                        /*Depth*/
                        Long level = ashSqlPlanMain.getDepth() + 1;
                        Long position = ashSqlPlanMain.getPosition();
                        Long searchColumns = ashSqlPlanMain.getSearchColumns();
                        Double cost = ashSqlPlanMain.getCost();
                        Double cardinality = ashSqlPlanMain.getCardinality();
                        Double bytes = ashSqlPlanMain.getBytes();
                        String otherTag = ashSqlPlanMain.getOtherTag();
                        String partitionStart = ashSqlPlanMain.getPartitionStart();
                        String partitionStop = ashSqlPlanMain.getPartitionStop();
                        Double partitionId = ashSqlPlanMain.getPartitionId();
                        String other = ashSqlPlanMain.getOther();
                        String distribution = ashSqlPlanMain.getDistribution();
                        Double cpuCost = ashSqlPlanMain.getCpuCost();
                        Double ioCost = ashSqlPlanMain.getIoCost();
                        Double tempSpace = ashSqlPlanMain.getTempSpace();
                        String accessPredicates = ashSqlPlanMain.getAccessPredicates();
                        String filterPredicates = ashSqlPlanMain.getFilterPredicates();
                        String projection = ashSqlPlanMain.getProjection();
                        Double time = ashSqlPlanMain.getTime();
                        String qblockName = ashSqlPlanMain.getQblockName();
                        String remarks = ashSqlPlanMain.getRemarks();

                        ExplainPlanModel10g2.ExplainRow parent = null;

                        if (level == 1) {
                            long tmp = 0;
                            ExplainPlanModel10g2.ExplainRow rowRoot = new ExplainPlanModel10g2.ExplainRow(
                                    parent, null, null, null, null, null, null,
                                    null, null, null, null, null, null, null,
                                    null, null, tmp, null, null, null,
                                    null, null, null, null, null, null, null,
                                    null, null, null, null, null, null, null,
                                    null, null, null, null, null);
                            model = new ExplainPlanModel10g2(rowRoot);

                            ExplainPlanModel10g2.ExplainRow row = new ExplainPlanModel10g2.ExplainRow(
                                    rowRoot, address, hashValue, sqlIdTmp, sqlPlanHashValue,
                                    childAddress, childNumber, operation, options,
                                    objectNode, object, objectOwner, objectName, objectAlias,
                                    objectType, optimizer, Id, parentId, /*Depth*/ level, position,
                                    searchColumns, cost, cardinality, bytes, otherTag,
                                    partitionStart, partitionStop, partitionId, other, distribution,
                                    cpuCost, ioCost, tempSpace, accessPredicates, filterPredicates,
                                    projection, time, qblockName, remarks);

                            rowRoot.addChild(row);
                            lastRow = row;
                            previousLevel = level;
                            continue;
                        } else if (previousLevel == level) {
                            parent = ((ExplainPlanModel10g2.ExplainRow) lastRow
                                    .getParent().getParent())
                                    .findChild(parentId.intValue());
                        } else if (level > previousLevel) {
                            parent = ((ExplainPlanModel10g2.ExplainRow) lastRow
                                    .getParent()).findChild(parentId
                                            .intValue());
                        } else if (level < previousLevel) {
                            parent = (ExplainPlanModel10g2.ExplainRow) lastRow
                                    .getParent();
                            for (long i = previousLevel - level; i >= 0; i--) {
                                parent = (ExplainPlanModel10g2.ExplainRow) parent
                                        .getParent();
                            }
                            parent = parent.findChild(parentId.intValue());
                        }
                        if (parent == null) {
                            isRecalculateDepth = true;
                            break;
                        }

                        ExplainPlanModel10g2.ExplainRow row = new ExplainPlanModel10g2.ExplainRow(
                                parent, address, hashValue, sqlIdTmp, sqlPlanHashValue,
                                childAddress, childNumber, operation, options,
                                objectNode, object, objectOwner, objectName, objectAlias,
                                objectType, optimizer, Id, parentId, /*Depth*/ level, position,
                                searchColumns, cost, cardinality, bytes, otherTag,
                                partitionStart, partitionStop, partitionId, other, distribution,
                                cpuCost, ioCost, tempSpace, accessPredicates, filterPredicates,
                                projection, time, qblockName, remarks);
                        parent.addChild(row);
                        lastRow = row;
                        previousLevel = level;

                        break;
                    }
                }
            } catch (DatabaseException e) {
                e.printStackTrace();
            } finally {
                try {
                    ashSqlPlan.close();
                } catch (DatabaseException e) {
                    e.printStackTrace();
                }
                ii++;
            }

        }

        // Recalculate wrong node levels
        if (isRecalculateDepth) {
            HashMap<Long, Long> idParentId = new HashMap<Long, Long>();
            HashMap<Long, Long> idLevel = new HashMap<Long, Long>();

            EntityCursor<AshSqlPlanDetail> ashSqlPlanP = null;

            try {
                ashSqlPlanP = dao.doRangeQuery(dao.ashSqlPlanHashValueDetail,
                        sqlPlanHashValue, true, sqlPlanHashValue, true);
                Iterator<AshSqlPlanDetail> ashSqlPlanIterP = ashSqlPlanP
                        .iterator();

                // Iterate
                while (ashSqlPlanIterP.hasNext()) {
                    AshSqlPlanDetail ashSqlPlanMainP = ashSqlPlanIterP.next();

                    Long idP = ashSqlPlanMainP.getId();
                    Long parent_idP = ashSqlPlanMainP.getParentId();
                    long tmp = -1;
                    if (idP == 0) {
                        idParentId.put(idP, tmp);
                    } else {
                        idParentId.put(idP, parent_idP);
                    }
                }

            } catch (DatabaseException e) {
                e.printStackTrace();
            } finally {
                try {
                    ashSqlPlanP.close();
                } catch (DatabaseException e) {
                    e.printStackTrace();
                }
            }

            idLevel = Utils.getLevels(idParentId);
            model = (ExplainPlanModel10g2) getSqlPlanModelByPlanHashValueP(idLevel, sqlPlanHashValue, sqlId);
        }

        return model;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.ash.database.DatabaseMain#getSqlPlanModelByPlanHashValueP(ExplainPlanModel)
     */
    public TreeTableModel getSqlPlanModelByPlanHashValueP(
            HashMap<Long, Long> idLevel, Double sqlPlanHashValue, String sqlId) {

        ExplainPlanModel10g2 model = null;
        ExplainPlanModel10g2.ExplainRow lastRow = null;
        EntityCursor<AshSqlPlanDetail> ashSqlPlan = null;
        long previousLevel = 1;
        boolean exitFromCycle = false;
        boolean isChildNumberSaved = false;
        long childNumberBySql = 0;
        long ii = 0;

        while (!exitFromCycle) {
            exitFromCycle = true;

            try {
                ashSqlPlan = dao.doRangeQuery(dao
                        .getAshSqlPlanHashValueDetail(), sqlPlanHashValue,
                        true, sqlPlanHashValue, true);
                Iterator<AshSqlPlanDetail> ashSqlPlanIter = ashSqlPlan
                        .iterator();

                // Iterate
                while (ashSqlPlanIter.hasNext()) {
                    AshSqlPlanDetail ashSqlPlanMain = ashSqlPlanIter.next();
                    String sqlIdTmp = ashSqlPlanMain.getSqlId();
                    long childNumberTmp = ashSqlPlanMain.getChildNumber();

                    if (!isChildNumberSaved && sqlId.equalsIgnoreCase(sqlIdTmp)) {
                        childNumberBySql = childNumberTmp;
                        isChildNumberSaved = true;
                    } else {
                        if (!sqlId.equalsIgnoreCase(sqlIdTmp)) {
                            continue;
                        }
                    }

                    Long id = ashSqlPlanMain.getId();

                    if (id == ii && childNumberBySql == childNumberTmp) {
                        exitFromCycle = false;

                        String address = ashSqlPlanMain.getAddress();
                        Double hashValue = ashSqlPlanMain.getHashValue();
                        //String sqlId = ashSqlPlanMain.getSqlId();
                        //Double planHashValue = sqlPlanHashValue;
                        String childAddress = ashSqlPlanMain.getChildAddress();
                        Long childNumber = ashSqlPlanMain.getChildNumber();
                        String operation = ashSqlPlanMain.getOperation();
                        String options = ashSqlPlanMain.getOptions();
                        String objectNode = ashSqlPlanMain.getObjectNode();
                        Double object = ashSqlPlanMain.getObject();
                        String objectOwner = ashSqlPlanMain.getObjectOwner();
                        String objectName = ashSqlPlanMain.getObjectName();
                        String objectAlias = ashSqlPlanMain.getObjectAlias();
                        String objectType = ashSqlPlanMain.getObjectType();
                        String optimizer = ashSqlPlanMain.getOptimizer();
                        Long Id = ashSqlPlanMain.getId();
                        Long parentId = ashSqlPlanMain.getParentId();
                        /*Depth*/
                        Long level = idLevel.get(id);// ashSqlPlanMain.getDepth()+1;
                        Long position = ashSqlPlanMain.getPosition();
                        Long searchColumns = ashSqlPlanMain.getSearchColumns();
                        Double cost = ashSqlPlanMain.getCost();
                        Double cardinality = ashSqlPlanMain.getCardinality();
                        Double bytes = ashSqlPlanMain.getBytes();
                        String otherTag = ashSqlPlanMain.getOtherTag();
                        String partitionStart = ashSqlPlanMain.getPartitionStart();
                        String partitionStop = ashSqlPlanMain.getPartitionStop();
                        Double partitionId = ashSqlPlanMain.getPartitionId();
                        String other = ashSqlPlanMain.getOther();
                        String distribution = ashSqlPlanMain.getDistribution();
                        Double cpuCost = ashSqlPlanMain.getCpuCost();
                        Double ioCost = ashSqlPlanMain.getIoCost();
                        Double tempSpace = ashSqlPlanMain.getTempSpace();
                        String accessPredicates = ashSqlPlanMain.getAccessPredicates();
                        String filterPredicates = ashSqlPlanMain.getFilterPredicates();
                        String projection = ashSqlPlanMain.getProjection();
                        Double time = ashSqlPlanMain.getTime();
                        String qblockName = ashSqlPlanMain.getQblockName();
                        String remarks = ashSqlPlanMain.getRemarks();

                        ExplainPlanModel10g2.ExplainRow parent = null;

                        if (level == 1) {
                            long tmp = 0;
                            ExplainPlanModel10g2.ExplainRow rowRoot = new ExplainPlanModel10g2.ExplainRow(
                                    parent, null, null, null, null, null, null,
                                    null, null, null, null, null, null, null,
                                    null, null, tmp, null, null, null,
                                    null, null, null, null, null, null, null,
                                    null, null, null, null, null, null, null,
                                    null, null, null, null, null);
                            model = new ExplainPlanModel10g2(rowRoot);

                            ExplainPlanModel10g2.ExplainRow row = new ExplainPlanModel10g2.ExplainRow(
                                    rowRoot, address, hashValue, sqlIdTmp, sqlPlanHashValue,
                                    childAddress, childNumber, operation, options,
                                    objectNode, object, objectOwner, objectName, objectAlias,
                                    objectType, optimizer, Id, parentId, /*Depth*/ level, position,
                                    searchColumns, cost, cardinality, bytes, otherTag,
                                    partitionStart, partitionStop, partitionId, other, distribution,
                                    cpuCost, ioCost, tempSpace, accessPredicates, filterPredicates,
                                    projection, time, qblockName, remarks);

                            rowRoot.addChild(row);
                            lastRow = row;
                            previousLevel = level;
                            continue;
                        } else if (previousLevel == level) {
                            parent = ((ExplainPlanModel10g2.ExplainRow) lastRow
                                    .getParent().getParent()).findChild(parentId
                                            .intValue());
                        } else if (level > previousLevel) {
                            parent = ((ExplainPlanModel10g2.ExplainRow) lastRow
                                    .getParent()).findChild(parentId.intValue());
                        } else if (level < previousLevel) {
                            parent = (ExplainPlanModel10g2.ExplainRow) lastRow
                                    .getParent();
                            for (long i = previousLevel - level; i >= 0; i--) {
                                parent = (ExplainPlanModel10g2.ExplainRow) parent
                                        .getParent();
                            }
                            parent = parent.findChild(parentId.intValue());
                        }
                        if (parent == null) {
                            break;
                        }

                        ExplainPlanModel10g2.ExplainRow row = new ExplainPlanModel10g2.ExplainRow(
                                parent, address, hashValue, sqlIdTmp, sqlPlanHashValue,
                                childAddress, childNumber, operation, options,
                                objectNode, object, objectOwner, objectName, objectAlias,
                                objectType, optimizer, Id, parentId, /*Depth*/ level, position,
                                searchColumns, cost, cardinality, bytes, otherTag,
                                partitionStart, partitionStop, partitionId, other, distribution,
                                cpuCost, ioCost, tempSpace, accessPredicates, filterPredicates,
                                projection, time, qblockName, remarks);
                        parent.addChild(row);
                        lastRow = row;
                        previousLevel = level;

                        break;
                    }
                }
            } catch (DatabaseException e) {
                e.printStackTrace();
            } finally {
                try {
                    ashSqlPlan.close();
                } catch (DatabaseException e) {
                    e.printStackTrace();
                }
                ii++;
            }
        }

        return model;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.ash.database.DatabaseMain#getSqlType(java.lang.String)
     */
    public String getSqlType(String sqlId) {
        String sqlType = null;
        try {
            AshSqlIdTypeText ash = dao.ashSqlIdTypeTextId.get(sqlId);
            if (ash != null) {
                sqlType = ash.getCommandType();
            } else {
                sqlType = "";
            }
        } catch (DatabaseException e) {
            sqlType = "";
            e.printStackTrace();
        }
        return sqlType;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.ash.database.DatabaseMain#getSqlText(java.lang.String)
     */
    public String getSqlText(String sqlId) {
        String sqlText = null;
        try {
            AshSqlIdTypeText ash = dao.ashSqlIdTypeTextId.get(sqlId);
            if (ash != null) {
                sqlText = ash.getSqlText();
            } else {
                sqlText = "";
            }

        } catch (DatabaseException e) {
            sqlText = "";
            e.printStackTrace();
        }
        return sqlText;
    }

    /**
     * Load data to temporary sql and sessions (gantt data)
     *
     * @param tmpSqlsTemp
     * @param tmpSessionsTemp
     * @param sqlId
     * @param waitClassId
     * @param sessionId
     * @param sessionidS
     * @param sessionSerial
     * @param sessioniSerialS
     * @param useridL
     * @param programSess
     * @param isDetail
     * @param sqlPlanHashValue
     */
    private void loadDataToTempSqlSession(SqlsTemp tmpSqlsTemp,
            SessionsTemp tmpSessionsTemp, String sqlId, double waitClassId, Long sessionId,
            String sessionidS, Double sessionSerial, String sessioniSerialS,
            Long useridL, String usernameSess, String programSess, boolean isDetail,
            String eventDetail, double sqlPlanHashValue) {

        int count = 1;

        /**
         * Save data for sql row
         */
        if (sqlId != null) {
            // Save SQL_ID and init
            tmpSqlsTemp.setSqlId(sqlId);
            // Save SqlPlanHashValue
            tmpSqlsTemp.saveSqlPlanHashValue(sqlId, sqlPlanHashValue);
            // Save group event
            tmpSqlsTemp.setTimeOfGroupEvent(sqlId, waitClassId, count);
        }

        /**
         * Save data for session row
         */
        tmpSessionsTemp.setSessionId(sessionidS, sessioniSerialS, programSess, "", usernameSess);
        tmpSessionsTemp.setTimeOfGroupEvent(sessionidS + "_" + sessioniSerialS, waitClassId, count);

        /**
         * Save event detail data for sql and sessions row
         */
        if (isDetail) {
            if (sqlId != null) {
                tmpSqlsTemp.setTimeOfEventName(sqlId, waitClassId, eventDetail, count);
            }
            tmpSessionsTemp.setTimeOfEventName(sessionidS + "_" + sessioniSerialS, waitClassId, eventDetail, count);
        }
    }

    /**
     * @return the kReconnect
     */
    private long getKReconnect() {
        return kReconnect;
    }

    /**
     * @param reconnect the kReconnect to set
     */
    private void setKReconnect(long reconnect) {
        kReconnect = reconnect;
    }

    /**
     * @return the isReconnect
     */
    private boolean isReconnect() {
        return isReconnect;
    }

    /**
     * @param isReconnect the isReconnect to set
     */
    private void setReconnect(boolean isReconnect) {
        this.isReconnect = isReconnect;
    }

}
