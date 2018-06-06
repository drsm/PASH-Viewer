/*
 *-------------------
 * The SqlsTemp.java is part of ASH Viewer
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
package org.ash.datatemp;

import org.ash.util.Options;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * The Class SqlsTemp.
 */
public class SqlsTemp {

    /**
     * The main sqls.
     */
    private HashMap<String, HashMap<String, Object>> mainSqls;

    /**
     * The SQL_TEXT.
     */
    private String SQL_TEXT = "SQL_TEXT";

    /**
     * The SQL_TYPE.
     */
    private String SQL_TYPE = "SQL_TYPE";

    /**
     * The cpu0.
     */
    private String cpu0 = "0";

    /**
     * The io1.
     */
    private String io1 = "1";

    /**
     * The lock2.
     */
    private String lock2 = "2";

    /**
     * The lwlock3.
     */
    private String lwlock3 = "3";

    /**
     * The bufferpin4.
     */
    private String bufferpin4 = "4";

    /**
     * The activity5.
     */
    private String activity5 = "5";

    /**
     * The extension6.
     */
    private String extension6 = "6";

    /**
     * The user i o8.
     */
    private String client7 = "7";

    /**
     * The system i o9.
     */
    private String ipc8 = "8";

    /**
     * The timeout9.
     */
    private String timeout9 = "9";

    /**
     * The COUNT.
     */
    private String COUNT = "COUNT";

    /**
     * The SQL PLAN.
     */
    private String SQLPLAN = "SQLPLAN";

    /**
     * The _cpu0_sum.
     */
    private double _cpu0_sum = 0;

    /**
     * The _io1_sum.
     */
    private double _io1_sum = 0;

    /**
     * The _lock2_sum.
     */
    private double _lock2_sum = 0;

    /**
     * The _lwlock3_sum.
     */
    private double _lwlock3_sum = 0;

    /**
     * The _bufferpin4_sum.
     */
    private double _bufferpin4_sum = 0;

    /**
     * The _activity5_sum.
     */
    private double _activity5_sum = 0;

    /**
     * The _extension6_sum.
     */
    private double _extension6_sum = 0;

    /**
     * The _client7_sum.
     */
    private double _client7_sum = 0;

    /**
     * The _user i o8_sum.
     */
    private double _ipc8_sum = 0;

    /**
     * The _system i o9_sum.
     */
    private double _timeout9_sum = 0;


    /**
     * The _sum.
     */
    private double _sum = 0;

    /**
     * The count of sql rows
     */
    private double _countSql = 0;

    /**
     * The event list.
     */
    private List eventList = new ArrayList();

    /**
     * The sql hash value list.
     */
    private HashMap<String, List<Double>> sqlHashValueHashMap = new HashMap<String, List<Double>>();

    /**
     * Instantiates a new sqls temp.
     */
    public SqlsTemp() {
        mainSqls = new HashMap<String, HashMap<String, Object>>();
    }

    /**
     * Sets the sql_id.
     *
     * @param sqlId the new sql_id
     */
    public void setSqlId(String sqlId) {
        if (!mainSqls.containsKey(sqlId)) {
            // Add SQL_ID, init storage for rows
            mainSqls.put(sqlId, new HashMap<String, Object>());
            // Save 0 values for group event
            mainSqls.get(sqlId).put(cpu0, 0.0);
            mainSqls.get(sqlId).put(io1, 0.0);
            mainSqls.get(sqlId).put(lock2, 0.0);
            mainSqls.get(sqlId).put(lwlock3, 0.0);
            mainSqls.get(sqlId).put(bufferpin4, 0.0);
            mainSqls.get(sqlId).put(activity5, 0.0);
            mainSqls.get(sqlId).put(extension6, 0.0);
            mainSqls.get(sqlId).put(client7, 0.0);
            mainSqls.get(sqlId).put(ipc8, 0.0);
            mainSqls.get(sqlId).put(timeout9, 0.0);
            // Set count to 0
            mainSqls.get(sqlId).put(COUNT, 0.0);
            // Set SQL_TYPE to UNKNOWN
            mainSqls.get(sqlId).put(SQL_TYPE,
                    Options.getInstance().getResource(
                            Options.getInstance().getResource("0")));
            // Initialize list for sqlid
            sqlHashValueHashMap.put(sqlId, new ArrayList<Double>());
        }
    }

    /**
     * Add sqlPlanHashValue to temporary variable.
     *
     * @param sqlId
     * @param sqlPlanHashValue
     */
    public void saveSqlPlanHashValue(String sqlId, double sqlPlanHashValue) {
        //System.out.println("Add to temp object1 "+sqlId+"  "+sqlPlanHashValue);
        List<Double> list = sqlHashValueHashMap.get(sqlId);
        Iterator<Double> listIter = list.iterator();
        boolean isExist = false;
        while (listIter.hasNext()) {
            if (listIter.next() == sqlPlanHashValue) {
                isExist = true;
            }
        }
        if (!isExist && sqlPlanHashValue != 0.0) {
            list.add(sqlPlanHashValue);
            //System.out.println("Add to temp object2 "+sqlId+"  "+sqlPlanHashValue);
        }
    }

    /**
     * Get sql hash value by sql id
     *
     * @param sqlId
     * @return
     */
    public List<Double> getSqlPlanHashValue(String sqlId) {
        List<Double> list = sqlHashValueHashMap.get(sqlId);
        return list;
    }

    public void putSqlType(String sqlId, String sqlType) {
        mainSqls.get(sqlId).put(SQL_TYPE, sqlType);
    }

    /**
     * Sets the time of group event.
     *
     * @param sqlId       the sql_id
     * @param waitClassId the wait class id
     */
    public void setTimeOfGroupEvent(
            String sqlId,
            double waitClassId,
            double _count) {

        boolean isCountWaitEvent = false;

        if (waitClassId == 0.0) {
            double _cpu0 = (Double) mainSqls.get(sqlId).get(cpu0);
            this.set_cpu0_sum(_count);
            mainSqls.get(sqlId).put(cpu0, _cpu0 + _count);
            isCountWaitEvent = true;
        } else if (waitClassId == 1.0) {
            double _io1 = (Double) mainSqls.get(sqlId).get(io1);
            this.set_io1_sum(_count);
            mainSqls.get(sqlId).put(io1, _io1 + _count);
            isCountWaitEvent = true;
        } else if (waitClassId == 2.0) {
            double _lock2 = (Double) mainSqls.get(sqlId).get(lock2);
            this.set_lock2_sum(_count);
            mainSqls.get(sqlId).put(lock2, _lock2 + _count);
            isCountWaitEvent = true;
        } else if (waitClassId == 3.0) {
            double _lwlock3 = (Double) mainSqls.get(sqlId).get(lwlock3);
            this.set_lwlock3_sum(_count);
            mainSqls.get(sqlId).put(lwlock3, _lwlock3 + _count);
            isCountWaitEvent = true;
        } else if (waitClassId == 4.0) {
            double _bufferpin4 = (Double) mainSqls.get(sqlId).get(bufferpin4);
            this.set_bufferpin4_sum(_count);
            mainSqls.get(sqlId).put(bufferpin4, _bufferpin4 + _count);
            isCountWaitEvent = true;
        } else if (waitClassId == 5.0) {
            double _activity5 = (Double) mainSqls.get(sqlId).get(activity5);
            this.set_activity5_sum(_count);
            mainSqls.get(sqlId).put(activity5, _activity5 + _count);
            isCountWaitEvent = true;
        } else if (waitClassId == 6.0) {
            double _extension6 = (Double) mainSqls.get(sqlId).get(extension6);
            this.set_extension6_sum(_count);
            mainSqls.get(sqlId).put(extension6, _extension6 + _count);
            isCountWaitEvent = true;
        } else if (waitClassId == 7.0) {
            double _client7 = (Double) mainSqls.get(sqlId).get(client7);
            this.set_client7_sum(_count);
            mainSqls.get(sqlId).put(client7, _client7 + _count);
            isCountWaitEvent = true;
        } else if (waitClassId == 8.0) {
            double _ipc8 = (Double) mainSqls.get(sqlId).get(ipc8);
            this.set_ipc8_sum(_count);
            mainSqls.get(sqlId).put(ipc8, _ipc8 + _count);
            isCountWaitEvent = true;
        } else if (waitClassId == 9.0) {
            double _timeout9 = (Double) mainSqls.get(sqlId).get(timeout9);
            this.set_timeout9_sum(_count);
            mainSqls.get(sqlId).put(timeout9, _timeout9 + _count);
            isCountWaitEvent = true;
        }

        /** Set count of sql rows */
        if (isCountWaitEvent) {
            double _counts = (Double) mainSqls.get(sqlId).get(COUNT);
            this.setCountSql_sum(_count);

            mainSqls.get(sqlId).put(COUNT, _counts + _count);
        }
    }

    /**
     * Sets the time of group event.
     *
     * @param sqlId       the sql_id
     * @param waitEvent   the wait event
     * @param waitClassId the wait class id
     */
    public void setTimeOfEventName(
            String sqlId,
            double waitClassId,
            String waitEvent,
            double _count) {

        if (!mainSqls.get(sqlId).containsKey(waitEvent)) {
            mainSqls.get(sqlId).put(waitEvent, _count);
            if (!eventList.contains(waitEvent)) {
                eventList.add(waitEvent);
            }
        } else {
            double _eventCount = (Double) mainSqls.get(sqlId).get(waitEvent);
            mainSqls.get(sqlId).put(waitEvent, _eventCount + _count);
        }
    }

    /**
     * Get list of eventName
     *
     * @return List
     */
    public List getEventList() {
        return eventList;
    }

    /**
     * Clear.
     */
    public void clear() {
        mainSqls.clear();
        eventList.clear();
        sqlHashValueHashMap.clear();

        _cpu0_sum = 0;
        _io1_sum = 0;
        _lock2_sum = 0;
        _lwlock3_sum = 0;
        _bufferpin4_sum = 0;
        _activity5_sum = 0;
        _extension6_sum = 0;
        _client7_sum = 0;
        _ipc8_sum = 0;
        _timeout9_sum = 0;
        _countSql = 0;
    }

    /**
     * Gets the main sqls.
     *
     * @return the main sqls
     */
    public HashMap<String, HashMap<String, Object>> getMainSqls() {
        return mainSqls;
    }

    /**
     * Sets the main sqls.
     *
     * @param mainSqls the main sqls
     */
    public void setMainSqls(HashMap<String, HashMap<String, Object>> mainSqls) {
        this.mainSqls = mainSqls;
    }

    /**
     * Gets the sQ l_ text.
     *
     * @return the sQ l_ text
     */
    public String getSQL_TEXT() {
        return SQL_TEXT;
    }

    /**
     * Gets the sQ l_ type.
     *
     * @return the sQ l_ type
     */
    public String getSQL_TYPE() {
        return SQL_TYPE;
    }

    public double get_cpu0_sum() {
        return _cpu0_sum;
    }
    public double get_io1_sum() {
        return _io1_sum;
    }
    public double get_lock2_sum() {
        return _lock2_sum;
    }
    public double get_lwlock3_sum() {
        return _lwlock3_sum;
    }
    public double get_bufferpin4_sum() {
        return _bufferpin4_sum;
    }
    public double get_activity5_sum() {
        return _activity5_sum;
    }
    public double get_extension6_sum() {
        return _extension6_sum;
    }
    public double get_client7_sum() {
        return _client7_sum;
    }
    public double get_ipc8_sum() {
        return _ipc8_sum;
    }
    public double get_timeout9_sum() {        return _timeout9_sum;    }

    /**
     * Sets the _cpu0_sum.
     *
     * @param _cpu0_sum the new _cpu0_sum
     */
    public void set_cpu0_sum(double _cpu0_sum) {
        this._cpu0_sum = this._cpu0_sum + _cpu0_sum;
    }

    /**
     * Sets the _io1_sum.
     *
     * @param _io1_sum the new _io1_sum
     */
    public void set_io1_sum(double _io1_sum) {
        this._io1_sum = this._io1_sum + _io1_sum;
    }

    /**
     * Sets the _lock2_sum.
     *
     * @param _lock2_sum the new _lock2_sum
     */
    public void set_lock2_sum(double _lock2_sum) {
        this._lock2_sum = this._lock2_sum + _lock2_sum;
    }

    /**
     * Sets the _lwlock3_sum.
     *
     * @param _lwlock3_sum the new _lwlock3_sum
     */
    public void set_lwlock3_sum(double _lwlock3_sum) {
        this._lwlock3_sum = this._lwlock3_sum + _lwlock3_sum;
    }

    /**
     * Sets the _bufferpin4_sum.
     *
     * @param _bufferpin4_sum the new _bufferpin4_sum
     */
    public void set_bufferpin4_sum(double _bufferpin4_sum) {
        this._bufferpin4_sum = this._bufferpin4_sum + _bufferpin4_sum;
    }

    /**
     * Sets the _activity5_sum.
     *
     * @param _activity5_sum the new _activity5_sum
     */
    public void set_activity5_sum(double _activity5_sum) {
        this._activity5_sum = this._activity5_sum + _activity5_sum;
    }

    /**
     * Sets the _extension6_sum.
     *
     * @param _extension6_sum the new _extension6_sum
     */
    public void set_extension6_sum(double _extension6_sum) {
        this._extension6_sum = this._extension6_sum + _extension6_sum;
    }

    public void set_client7_sum(double _client7_sum) {        this._client7_sum = this._client7_sum + _client7_sum;    }
    public void set_ipc8_sum (double _ipc8_sum) {        this._ipc8_sum = this._ipc8_sum + _ipc8_sum;    }
    public void set_timeout9_sum(double _timeout9_sum) {        this._timeout9_sum = this._timeout9_sum + _timeout9_sum;    }
    public double get_sum() {
        return _sum;
    }


    /**
     * Set count of sql rows (for range)
     */
    public void setCountSql_sum(double _countSql) {
        this._countSql = this._countSql + _countSql;
    }

    /**
     * Get sum of count (for range)
     *
     * @return
     */
    public double getCountSql() {
        return _countSql;
    }

    /**
     * Set_sum.
     */
    public void set_sum() {
        this._sum = this.get_cpu0_sum()
                + this.get_io1_sum()
                + this.get_lock2_sum()
                + this.get_lwlock3_sum()
                + this.get_bufferpin4_sum()
                + this.get_activity5_sum()
                + this.get_extension6_sum()
                + this.get_client7_sum()
                + this.get_ipc8_sum()
                + this.get_timeout9_sum();
    }

}
