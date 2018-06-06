/*
 *-------------------
 * The ASHDatabase.java is part of ASH Viewer
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

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;
import org.ash.conn.model.Model;
import org.ash.datamodel.*;
import org.ash.datatemp.SessionsTemp;
import org.ash.datatemp.SqlsTemp;
import org.ash.detail.StackedChartDetail;
import org.ash.util.Options;
import org.jdesktop.swingx.treetable.TreeTableModel;
import org.jfree.data.xy.CategoryTableXYDataset;

import javax.swing.table.DefaultTableModel;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * The Class DatabaseMain.
 */
public class ASHDatabase {

    /**
     * The model.
     */
    private Model model;

    /**
     * The store.
     */
    private EntityStore store;

    /**
     * The dao.
     */
    private AshDataAccessor dao;

    /**
     * The BDB environment.
     */
    private Environment env;

    /**
     * The BDB store config.
     */
    private StoreConfig storeConfig;

    /**
     * The BDB env config.
     */
    private EnvironmentConfig envConfig;

    /**
     * The sample id.
     */
    private long sampleId = -1;

    /**
     * The current date.
     */
    private Date currentDate;

    /**
     * The sysdate delta double.
     */
    private Double sysdateDeltaDouble;

    /**
     * The first key for update.
     */
    private Double firstKeyForUpdate = 0.0;

    /**
     * The sqls temp.
     */
    private SqlsTemp sqlsTemp;

    /**
     * The sessions temp.
     */
    private SessionsTemp sessionsTemp;

    /**
     * The sqls temp (detail).
     */
    private SqlsTemp sqlsTempDetail;

    /**
     * The sessions temp (detail).
     */
    private SessionsTemp sessionsTempDetail;

    /**
     * The current window.
     */
	// ��� ������ �������� ������� � ������������
    private Double currentWindow = 3900000.0;

    /**
     * The dataset.
     */
    private CategoryTableXYDataset dataset = null;

    /**
     * The half range for one 15 sec storage (details)
     */
    private int rangeHalf = 7500;

    /**
     * Local begin time (detail)
     */
    private double beginTimeOnRunDetail = 0.0;

    /**
     * The store of event Class and corresponding StackedXYAreaChartDetail object
     */
    private HashMap<String, StackedChartDetail> storeStackedXYAreaChartDetail;

    /**
     * The store of event Class and corresponding StackedXYAreaChartDetail object
     */
    private HashMap<String, Boolean> storeEventAndIsAddPointsToLeftSideFlag;

    /**
     * Instantiates a new main database object.
     *
     * @param model0 the model0
     */
    public ASHDatabase(Model model0) {

        this.model = model0;

        try {
            this.initialize();
        } catch (DatabaseException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        this.sqlsTemp = new SqlsTemp();
        this.sessionsTemp = new SessionsTemp(this.store, this.dao);
        this.sqlsTempDetail = new SqlsTemp();
        this.sessionsTempDetail = new SessionsTemp(this.store, this.dao);

        this.storeStackedXYAreaChartDetail = new HashMap<String, StackedChartDetail>();
        this.storeEventAndIsAddPointsToLeftSideFlag = new HashMap<String, Boolean>();
    }

    /**
     * Initialize.
     *
     * @throws DatabaseException the database exception
     * @throws SQLException      the SQL exception
     * @throws IOException       Signals that an I/O exception has occurred.
     */
    public void initialize() throws DatabaseException, SQLException,
            IOException {

        /* Open a transactional Berkeley DB engine environment. */
        envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(false);
        envConfig.setCachePercent(20); // Set BDB cache size = 20%

        env = new Environment(new File(Options.getInstance().getEnvDir()), envConfig);

        /* Open a transactional entity store. */
        storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        storeConfig.setTransactional(false);
        storeConfig.setDeferredWrite(true);
        store = new EntityStore(env, "ash.db", storeConfig);

        /* Initialize the data access object. */
        dao = new AshDataAccessor(store);
    }

    /**
     * Load to local bdb.
     */
    public void loadToLocalBDB() {
    }

    /**
     * Load to local bdb collector.
     */
    public synchronized void loadToLocalBDBCollector() {
    }

    /**
     * Load command type, sql_text from v$sql
     */
    public void loadSqlTextCommandTypeFromDB(List<String> arraySqlId) {
    }

    /**
     * Load sql plan from v$sql_plan
     *
     * @param arraySqlId
     * @param isDetail
     */
    public void loadSqlPlanFromDB(List<String> arraySqlId, boolean isDetail) {
    }

    /**
     * Is sql plan loaded to local storage (by keys planHashValue and sqlId).
     *
     * @param sqlPlanHashValue
     * @param sqlId
     * @return
     */
    public boolean isSqlPlanHashValueExist(Double sqlPlanHashValue, String sqlId) {
        return true;
    }


    /**
     * Get list plan hash values by sql id
     *
     * @param sqlId
     * @return
     */
    public List<Double> getSqlPlanHashValueBySqlId(String sqlId) {
        return null;
    }

    /**
     * Is sql text loaded to local storage.
     */
    public boolean isSqlTextExist(String sqlId) {
        return true;
    }

    /**
     * Return ExplainPlanModel for sql plan.
     *
     * @param sqlPlanHashValue
     * @return
     */
    public TreeTableModel getSqlPlanModelByPlanHashValue(Double sqlPlanHashValue, String sqlId) {
        return null;
    }

    /**
     * Return ExplainPlanModel for bad depth in sql plan.
     *
     * @param idLevel
     * @param sqlPlanHashValue
     * @param sqlId
     * @return
     */
    public TreeTableModel getSqlPlanModelByPlanHashValueP(HashMap<Long, Long> idLevel,
                                                          Double sqlPlanHashValue, String sqlId) {
        return null;
    }

    /**
     * Load sql_text, sql_id from database.
     *
     * @param array10
     */
    public void loadSqlTextSqlIdFromDB(List<String> array10) {

    }

    /**
     * Get ASH report
     *
     * @param begin
     * @param end
     * @return
     */
    public StringBuffer getASHReport(double begin, double end) {
        return new StringBuffer();
    }


    /**
     * Get sql_type for sql_id
     *
     * @param sqlId
     * @return sqlType
     */
    public String getSqlType(String sqlId) {
        return "";
    }

    /**
     * Get sql plan by PLAN_HASH_VALUE
     *
     * @param sqlPlanHashValue
     * @return
     */
    public String getSqlPlan(Double sqlPlanHashValue) {
        return "";
    }

    /**
     * Get sql_text for sql_id
     *
     * @param sqlId
     * @return sqlType
     */
    public String getSqlText(String sqlId) {
        return "";
    }

    /**
     * Load to sub by event and 10sec.
     */
    public void loadToSubByEventAnd10Sec() {

        Double lastKey = getSysdate();

        if (sampleId == -1) { // first run
            try {
                store.sync();
                dao.loadAshCalcSumByEventById15SecOnRun(lastKey - currentWindow, lastKey, this);
                store.sync();
            } catch (DatabaseException e) {
                e.printStackTrace();
            }
        } else { // subsequent runs
            try {
                store.sync();
                Double firstKey = dao.ashBySampleTime.sortedMap().lastKey();

                // for 9i version
                if (firstKey == null) firstKey = lastKey;

                // for updateDataToChartPanelDataSet
                if (firstKey < lastKey - currentWindow) {
                    dao.loadAshCalcSumByEventById15SecOnRun(lastKey - currentWindow, lastKey, this);
                } else {
                    dao.loadAshCalcSumByEventById15SecOnRun(firstKey, lastKey, this);
                }

                store.sync();

            } catch (DatabaseException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Initialize vars on load.
     */
    protected void initializeVarsOnLoad() {
        try {
            if (dao.ashById.sortedMap() != null) {
                sampleId = dao.ashById.sortedMap().lastKey();
            } else {
                sampleId = -1;
            }
        } catch (Exception e) {
            sampleId = -1;
        }
    }

    /**
     * Initialize vars after load9i.
     */
    protected void initializeVarsAfterLoad9i() {
        try {
            if (dao.ashById.sortedMap() != null) {
                sampleId = dao.ashById.sortedMap().lastKey();
            } else {
                sampleId = 0;
            }
        } catch (Exception e) {
            sampleId = 0;
        }
    }

    /**
     * Load parameter/value to local BDB
     *
     * @param parameter
     * @param value
     */
    public void saveParameterToLocalBDB(String parameter, String value) {
        try {
            this.dao.ashParamValue.putNoReturn(
                    new AshParamValue(parameter, value));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }

        try {
            store.sync();
        } catch (DatabaseException e) {
            e.printStackTrace();
        }

    }

    /**
     * Gets the parameter value from local BDB.
     *
     * @param parameter parameter id
     * @return the value
     */
    public String getParameter(String parameter) {

        String value = null;
        try {
            AshParamValue ashParamValue = dao.getAshParamValue().get(parameter);
            if (ashParamValue != null) {
                value = ashParamValue.getValue();
            } else {
                value = "";
            }
        } catch (DatabaseException e) {
            value = "";
            e.printStackTrace();
        }
        return value;
    }

    /**
     * Load data to chart panel data set.
     *
     * @param _dataset the _dataset
     */
    public void loadDataToChartPanelDataSet(CategoryTableXYDataset _dataset) {

        try {
            int i = 0;
            this.dataset = _dataset;
            EntityCursor<ActiveSessionHistory15> items;

            items = dao.doRangeQuery(
                    dao.ashCalcSumByEventById115Sec, getSysdate() - currentWindow, true,
                    getSysdate(), true);

            /* Do a filter on Ash by SampleTime. */
            Iterator<ActiveSessionHistory15> deptIter = items.iterator();

            while (deptIter.hasNext()) {
                ActiveSessionHistory15 ashSumMain = deptIter.next();
                double tempSampleTime = ashSumMain.getSampleTime();

                updateDataset(ashSumMain, tempSampleTime);
                firstKeyForUpdate = tempSampleTime;
                i++;
            }

            items.close();

        } catch (DatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Save reference to StackedXYAreaChartDetail for wait classes or cpu used
     *
     * @param detailValue  StackedXYAreaChartDetail object
     * @param waitClasskey
     */
    public void saveStackedXYAreaChartDetail(StackedChartDetail detailValue,
                                             String waitClasskey) {
        this.storeStackedXYAreaChartDetail.put(waitClasskey, detailValue);
        this.storeEventAndIsAddPointsToLeftSideFlag.put(waitClasskey, false);
    }

    /**
     * Load data to chart panel dataset (detail charts).
     */
    public void initialLoadingDataToChartPanelDataSetDetail() {
        this.loadDataToChartPanelDataSetDetail();
    }

    /**
     * Load data to chart panel dataset (detail charts).
     */
    private void loadDataToChartPanelDataSetDetail() {



        try {
            String cpuUsed = Options.getInstance().getResource("cpuLabel.text");

            double startSampleTime = getSysdate() - currentWindow;
            double endSampleTime = getSysdate();

            if (this.beginTimeOnRunDetail == 0.0) {// Init
                this.initSeriesPaint();
                this.beginTimeOnRunDetail = startSampleTime;
            } else { // Check if wait event is exist and the load to dataset
                this.checkAndInitSeriesPaintAfterRunning();
            }

            for (double dd = this.beginTimeOnRunDetail; dd < endSampleTime; dd += rangeHalf * 2) {

                /* Do a filter on AshIdTime by SampleTime. (detail) */
                EntityCursor<AshIdTime> ashIdTimeCursor =
                        dao.doRangeQuery(dao.getAshBySampleTime(),
                                dd, true, dd + rangeHalf * 2, false);

                Iterator<AshIdTime> ashIdTimeIter = ashIdTimeCursor.iterator();

                // Iterate over AshIdTime (detail)
                while (ashIdTimeIter.hasNext()) {
                    AshIdTime ashIdTimeMain = ashIdTimeIter.next();

                    /* Do a filter on ActiveSessionHistory by SampleID (detail). */
                    EntityCursor<ActiveSessionHistory> ActiveSessionHistoryCursor =
                            dao.getActiveSessionHistoryByAshId().subIndex(ashIdTimeMain.getsampleId()).entities();
                    Iterator<ActiveSessionHistory> ActiveSessionHistoryIter =
                            ActiveSessionHistoryCursor.iterator();

                    // Iterate over ActiveSessionHistory (detail)
                    while (ActiveSessionHistoryIter.hasNext()) {
                        ActiveSessionHistory ASH = ActiveSessionHistoryIter.next();
                        double count = 1.0;

                        // If waitclass is empty - go to next row
                        if (ASH.getWaitClass() == null ||
                                ASH.getWaitClass().equals("") ||
                                ASH.getWaitClass().equals("Idle")) {
                            continue;
                        }

                        String eventName = ASH.getEvent();
                        String waitClassEvent = ASH.getWaitClass();

                        StackedChartDetail tmpStackedObj = this.storeStackedXYAreaChartDetail.get(waitClassEvent);

                        // Checking and loading data
                        if (tmpStackedObj.isSeriesContainName(eventName)) {
                            double tmp = tmpStackedObj.getSeriesNameSum(eventName);
                            tmpStackedObj.setSeriesNameSum(eventName, tmp + count);
                        } else {
                            int nextNumber = 0;
                            if (!tmpStackedObj.isSeriesIdNameEmpty()) {
                                nextNumber = tmpStackedObj
                                        .getSizeSeriesIdName();
                                tmpStackedObj.setSeriesIdName(nextNumber, eventName);
                                tmpStackedObj.setSeriesNameSum(eventName, count);
                                tmpStackedObj.setSeriesPaint(nextNumber, eventName, "loadDataToChartPanelDataSetDetail");

                            } else {
                                tmpStackedObj.setSeriesIdName(0, eventName);
                                tmpStackedObj.setSeriesNameSum(eventName, count);
                                tmpStackedObj.setSeriesPaint(0, eventName, "initSeriesPaint");
                                this.storeEventAndIsAddPointsToLeftSideFlag.put(waitClassEvent, true);
                            }
                        }
                    }
                    ActiveSessionHistoryCursor.close();
                }
                ashIdTimeCursor.close();

                // Calculate, save and clear
                Set entriesSum = this.storeStackedXYAreaChartDetail.entrySet();
                Iterator iterSum = entriesSum.iterator();
                while (iterSum.hasNext()) {
                    Map.Entry entry = (Map.Entry) iterSum.next();
                    String keyClass = (String) entry.getKey();
                    StackedChartDetail valueChart = (StackedChartDetail) entry.getValue();
                    valueChart.calcSaveAndClear(rangeHalf, dd);
                    // Add points to left side
                    if (this.storeEventAndIsAddPointsToLeftSideFlag.get(keyClass)) {
                        valueChart.addPointsToLeft(getSysdate() - currentWindow, dd, rangeHalf);
                        this.storeEventAndIsAddPointsToLeftSideFlag.put(keyClass, false);
                    }
                }

                this.beginTimeOnRunDetail = dd;

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Update data to chart panel data set.
     */
    public void updateDataToChartPanelDataSet() {
        try {
            int iCountValues = 0;

            // Filter AshCalcSumByEvent10Sec for update dataset
            EntityCursor<ActiveSessionHistory15> ashSumCursor = dao.doRangeQuery(dao.ashCalcSumByEventById115Sec, firstKeyForUpdate, false, getSysdate(), true);


		double QWEGSD = getSysdate();

            /* Do a filter on Ash by SampleTime. */
            Iterator<ActiveSessionHistory15> deptIter = ashSumCursor.iterator();

            while (deptIter.hasNext()) {
                ActiveSessionHistory15 ashSumMain = deptIter.next();
                double tempSampleTime = ashSumMain.getSampleTime();

		/*
		java.sql.Timestamp QWETS1 = new java.sql.Timestamp(System.currentTimeMillis());
		QWETS1.setTime((long)tempSampleTime);
		java.sql.Timestamp QWETS2 = new java.sql.Timestamp(System.currentTimeMillis());
		QWETS2.setTime((long)QWEGSD);
		double QWEDF = QWEGSD - tempSampleTime;
		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
		System.out.println("--- " + timeStamp + " --- " + " from = " + QWETS1.toString() + " to = " + QWETS2.toString() + " diff = " + QWEDF + ", ashSumMain.getActivity() = " + ashSumMain.getActivity());
		*/

                updateDataset(ashSumMain, tempSampleTime);

                firstKeyForUpdate = tempSampleTime;
                iCountValues++;
            }

            ashSumCursor.close();

            // Delete old values from main dataset
            this.deleteValuesFromDataset();

            // Update and delete from detail dataset
            this.loadDataToChartPanelDataSetDetail();
            this.deleteValuesFromDatasetDetail();
        } catch (DatabaseException e) {
            e.printStackTrace();
        }

    }


    public DefaultTableModel getASHRawData(double begin, double end, String detail) throws DatabaseException {

        DefaultTableModel model = new DefaultTableModel(new String[]{
                "SampleID",
                "SampleTime",
                "SessionID",
                "Username",
                "Program",
                "Sql type",
                "SQL ID",
                "Event",
                "Wait Class",
                "Wait Class id",
                "UserID",
                "Hostname"
        }, 0);

        try {

            /* Do a filter on AshIdTime by SampleTime. (detail) */
            EntityCursor<AshIdTime> ashIdTimeCursor =
                    dao.doRangeQuery(dao.getAshBySampleTime(),
                            begin, true, end, false);

            Iterator<AshIdTime> ashIdTimeIter = ashIdTimeCursor.iterator();

            // Iterate over AshIdTime (detail)
            while (ashIdTimeIter.hasNext()) {
                AshIdTime ashIdTimeMain = ashIdTimeIter.next();

                Long sampleTimeLong = (long) ashIdTimeMain.getsampleTime();
                Date td = new Date(sampleTimeLong.longValue());
                DateFormat df = new SimpleDateFormat("dd.MM.yyyy HH.mm.ss");
                String reportDateStr = df.format(td);

                /* Do a filter on ActiveSessionHistory by SampleID (detail). */
                EntityCursor<ActiveSessionHistory> ActiveSessionHistoryCursor =
                        dao.getActiveSessionHistoryByAshId().subIndex(ashIdTimeMain.getsampleId()).entities();
                Iterator<ActiveSessionHistory> ActiveSessionHistoryIter =
                        ActiveSessionHistoryCursor.iterator();

                // Iterate over ActiveSessionHistory (detail)
                while (ActiveSessionHistoryIter.hasNext()) {
                    ActiveSessionHistory ASH = ActiveSessionHistoryIter.next();

			// ���������� ������ � ASH RAW DATA �� WAIT CLASS
			if (detail.length()!=0){
				if (!detail.contains(ASH.getWaitClass())){
					continue;
				}
			}

                    model.addRow(new Object[]{
                            ASH.getSampleId(),
                            reportDateStr,
                            ASH.getSessionId(),
                            ASH.getUserName(),
                            ASH.getProgram(),
                            ASH.getCommand_type(),
                            ASH.getSqlId(),
                            ASH.getEvent(),
                            ASH.getWaitClass(),
                            (long) ASH.getWaitClassId(),
                            ASH.getUserId(),
                            ASH.getHostname()
                    });
                }
                ActiveSessionHistoryCursor.close();
            }
            ashIdTimeCursor.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return model;
    }

    public String getUsername(Long userId) {

        String userName = null;
        try {
            AshUserIdUsername userIdU = dao.getUserIdUsernameById().get(userId);
            if (userIdU != null) {
                userName = userIdU.getUsername();
            } else {
                userName = "";
            }
        } catch (DatabaseException e) {
            // TODO Auto-generated catch block
            userName = "";
            e.printStackTrace();
        }
        return userName;
    }

    /**
     * Delete data from database.
     *
     * @param start
     * @param end
     */
    public void deleteData(long start, long end) {
        dao.deleteData(start, end);
    }


    /**
     * DB env log cleaning.
     */
    public void cleanLogs() {
        boolean anyCleaned = false;
        try {
            while (this.store.getEnvironment().cleanLog() > 0) {
                anyCleaned = true;
            }
        } catch (DatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (anyCleaned) {
            CheckpointConfig force = new CheckpointConfig();
            force.setForce(true);
            try {
                this.store.getEnvironment().checkpoint(force);
            } catch (DatabaseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * Gets the sample id.
     *
     * @return the sample id
     */
    protected long getSampleId() {
        return sampleId;
    }

    /**
     * Gets the sample id.
     *
     * @return the sample id
     */
    protected void setSampleId(long sampleId0) {
        sampleId = sampleId0;
    }

    /**
     * Delete values from dataset.
     */
    private void deleteValuesFromDataset() {

        for (int i = 0; i < 50; i++) {

            Double xValue = (Double) dataset.getX(0, i);

            if (xValue > getSysdate() - currentWindow) {
                break;
            }

            try {
                dataset.removeRow(xValue);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * Delete values from dataset (detail)
     */
    private void deleteValuesFromDatasetDetail() {
        Set entriesSum = this.storeStackedXYAreaChartDetail.entrySet();
        Iterator iterSum = entriesSum.iterator();
        while (iterSum.hasNext()) {
            Map.Entry entry = (Map.Entry) iterSum.next();
            String keyClass = (String) entry.getKey();
            StackedChartDetail valueChart = (StackedChartDetail) entry.getValue();
            valueChart.deleteValuesFromDatasetDetail(getSysdate() - currentWindow);
        }
    }

    /**
     * Initialize series paint, renderer, dataset for stacked charts.
     */
    private void initSeriesPaint() {

        String cpuUsed = Options.getInstance().getResource("cpuLabel.text");
        Set entries = this.storeStackedXYAreaChartDetail.entrySet();
        Iterator iter = entries.iterator();

        while (iter.hasNext()) {

            Map.Entry entry = (Map.Entry) iter.next();
            String keyClass = (String) entry.getKey();

            StackedChartDetail valueChart = (StackedChartDetail) entry
                    .getValue();

            if (keyClass.equalsIgnoreCase(cpuUsed)) { // CPU used
                valueChart.setSeriesIdName(0, cpuUsed);
                valueChart.setSeriesNameSum(cpuUsed, 0.0);
                valueChart.setSeriesPaint(0, cpuUsed, "initSeriesPaint");
            } else { // Event class
                if (dao.getEventClassName(keyClass) != null) {
                    valueChart.setSeriesIdName(0, dao
                            .getEventClassName(keyClass));
                    valueChart.setSeriesNameSum(dao
                            .getEventClassName(keyClass), 0.0);
                    valueChart.setSeriesPaint(0, dao
                            .getEventClassName(keyClass), "initSeriesPaint");
                }
            }
        }
    }


    /**
     * Initialize subsequent stacked charts.
     */
    private void checkAndInitSeriesPaintAfterRunning() {

        String cpuUsed = Options.getInstance().getResource("cpuLabel.text");

        Set entries = this.storeStackedXYAreaChartDetail.entrySet();
        Iterator iter = entries.iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String keyClass = (String) entry.getKey();

            StackedChartDetail valueChart = (StackedChartDetail) entry
                    .getValue();
            if (!keyClass.equalsIgnoreCase(cpuUsed)) { //Initialize only for event class

                if (valueChart.isDatasetEmpty() &&
                        dao.getEventClassName(keyClass) != null) {

                    valueChart.setSeriesIdName(0, dao
                            .getEventClassName(keyClass));
                    valueChart.setSeriesNameSum(dao
                            .getEventClassName(keyClass), 0.0);
                    valueChart.setSeriesPaint(0, dao
                            .getEventClassName(keyClass), "checkAndInitSeriesPaintAfterRunning after");

                    valueChart.addPointsToLeft(getSysdate() - currentWindow, getSysdate(), rangeHalf);
                }
            }
        }
    }

    /**
     * Gets the database sysdate.
     *
     * @return the sysdate
     */
    public double getSysdate() {

        currentDate = new Date();

        double sysdate = 0.0;
        double sysdateLocal = (new Long(currentDate.getTime()).doubleValue());
        if (sysdateDeltaDouble == null) {
            try {
                sysdateDeltaDouble = this.model.getSysdate() - sysdateLocal;
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                sysdateDeltaDouble = null;
            }
            sysdate = sysdateLocal + sysdateDeltaDouble;

        } else {
            sysdate = sysdateLocal + sysdateDeltaDouble;
        }

        return sysdate;
    }

    /**
     * Update dataset.
     *
     * @param ashSumMain     the ash sum main
     * @param tempSampleTime the temp sample time
     */
    private void updateDataset(ActiveSessionHistory15 ashSumMain, double tempSampleTime) {
        dataset.add(tempSampleTime, ashSumMain.getCPU(), Options.getInstance().getResource("CPULabel.text"));
        dataset.add(tempSampleTime, ashSumMain.getIO(), Options.getInstance().getResource("IOLabel.text"));
        dataset.add(tempSampleTime, ashSumMain.getLock(), Options.getInstance().getResource("LockLabel.text"));
        dataset.add(tempSampleTime, ashSumMain.getLWLock(), Options.getInstance().getResource("LWLockLabel.text"));
        dataset.add(tempSampleTime, ashSumMain.getBufferPin(), Options.getInstance().getResource("BufferPinLabel.text"));
        dataset.add(tempSampleTime, ashSumMain.getActivity(), Options.getInstance().getResource("ActivityLabel.text"));
        dataset.add(tempSampleTime, ashSumMain.getExtension(), Options.getInstance().getResource("ExtensionLabel.text"));
        dataset.add(tempSampleTime, ashSumMain.getClient(), Options.getInstance().getResource("ClientLabel.text"));
        dataset.add(tempSampleTime, ashSumMain.getIPC(), Options.getInstance().getResource("IPCLabel.text"));
        dataset.add(tempSampleTime, ashSumMain.getTimeout(), Options.getInstance().getResource("TimeoutLabel.text"));
    }

    /**
     * Gets the BDB store.
     *
     * @return the store
     */
    public EntityStore getStore() {
        return store;
    }

    /**
     * Gets the BDB dao object.
     *
     * @return the BDB dao object
     */
    public AshDataAccessor getDao() {
        return dao;
    }

    /**
     * Close BDB.
     */
    public void close() {

        if (store != null) {
            try {
                store.close();
            } catch (DatabaseException dbe) {
                System.err.println("Error closing store: " + dbe.toString());
            }
        }

        if (env != null) {
            try {
                // Finally, close environment.
                env.close();
            } catch (DatabaseException dbe) {
                System.err.println("Error closing env: " + dbe.toString());
            }
        }
    }


    /**
     * Calculate sqls, sessions data.
     *
     * @param beginTime the begin time
     * @param endTime   the end time
     * @param eventFlag All for main top activity, event class - for detail
     */
    public void calculateSqlsSessionsData(double beginTime, double endTime,
                                          String eventFlag) {
    }

    /**
     * Gets the sqls temp.
     *
     * @return the sqls temp
     */
    public SqlsTemp getSqlsTemp() {
        return sqlsTemp;
    }

    /**
     * Gets the sessions temp.
     *
     * @return the sessions temp
     */
    public SessionsTemp getSessionsTemp() {
        return sessionsTemp;
    }

    /**
     * Gets the sqls temp.
     *
     * @return the sqls temp
     */
    public SqlsTemp getSqlsTempDetail() {
        return sqlsTempDetail;
    }

    /**
     * Gets the sessions temp.
     *
     * @return the sessions temp
     */
    public SessionsTemp getSessionsTempDetail() {
        return sessionsTempDetail;
    }
}
