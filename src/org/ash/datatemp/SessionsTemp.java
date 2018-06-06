/*
 *-------------------
 * The SessionsTemp.java is part of ASH Viewer
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.ash.database.AshDataAccessor;
import org.ash.datamodel.AshUserIdUsername;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityStore;

/**
 * The Class SessionsTemp.
 */
public class SessionsTemp {

    /**
     * The BDB store.
     */
    private EntityStore store = null;

    /**
     * The BDB dao.
     */
    private AshDataAccessor dao = null;

    /**
     * The main sessions.
     */
    private HashMap<String, HashMap<String, Object>> mainSessions;

    /**
     * The userid username.
     */
    private HashMap<Long, String> userIdusername;

    /**
     * The USER_ID.
     */
    private String USER_ID = "USER_ID";

    /**
     * The USERNAME.
     */
    private String USERNAME = "USERNAME";

    /**
     * The PROGRAM.
     */
    private String PROGRAM = "PROGRAM";

    /**
     * The COUNT.
     */
    private String COUNT = "COUNT";

    /**
     * The SESSIONID.
     */
    private String SESSIONID = "SESSIONID";

    /**
     * The session serial#.
     */
    private String SESSIONSERIAL = "SESSIONSERIAL";

    /**
     * The lwlock.
     */
    private String cpu = "0";
    private String io = "1";
    private String lock = "2";
    private String lwlock = "3";
    private String bufferpin = "4";
    private String activity = "5";
    private String extension = "6";
    private String client = "7";
    private String ipc = "8";
    private String timeout = "9";

    private double _cpu_sum = 0;
    private double _lwlock_sum = 0;
    private double _lock_sum = 0;
    private double _bufferpin_sum = 0;
    private double _activity_sum = 0;
    private double _extension_sum = 0;
    private double _client_sum = 0;
    private double _ipc_sum = 0;
    private double _timeout_sum = 0;
    private double _io_sum = 0;
    private double _sum = 0;
    private double _countSql = 0;

    /**
     * The event list.
     */
    private List eventList = new ArrayList();

    /**
     * Instantiates a new sessions temp.
     */
    public SessionsTemp(EntityStore store0, AshDataAccessor dao0) {
        this.store = store0;
        this.dao = dao0;
        this.mainSessions = new HashMap<String, HashMap<String, Object>>();
        this.userIdusername = new HashMap<Long, String>();
    }

    /**
     * Sets the session id.
     *
     * @param program  the program
     * @param user_id  the user_id
     * @param username the username
     */
    public void setSessionId(String _sessionId, String _sessionSerial, String program,
                             String user_id, String username) {
        String sessionId = _sessionId + "_" + _sessionSerial;
        if (!mainSessions.containsKey(sessionId)) {
            // Add SQL_ID, init storage for rows
            mainSessions.put(sessionId, new HashMap<String, Object>());
            // Save 0 values for group event
            mainSessions.get(sessionId).put(cpu, 0.0);
            mainSessions.get(sessionId).put(lwlock, 0.0);
            mainSessions.get(sessionId).put(lock, 0.0);
            mainSessions.get(sessionId).put(bufferpin, 0.0);
            mainSessions.get(sessionId).put(activity, 0.0);
            mainSessions.get(sessionId).put(extension, 0.0);
            mainSessions.get(sessionId).put(client, 0.0);
            mainSessions.get(sessionId).put(ipc, 0.0);
            mainSessions.get(sessionId).put(timeout, 0.0);
            mainSessions.get(sessionId).put(io, 0.0);
            // Set SESSIONID
            mainSessions.get(sessionId).put(SESSIONID, _sessionId);
            // Set SESSIONSERIAL
            mainSessions.get(sessionId).put(SESSIONSERIAL, _sessionSerial);
            // Set USERNAME
            mainSessions.get(sessionId).put(USERNAME, username);
            // Set PROGRAM
            mainSessions.get(sessionId).put(PROGRAM, program);
            // Set USER_ID
            mainSessions.get(sessionId).put(USER_ID, user_id);
            // Set count to 0
            mainSessions.get(sessionId).put(COUNT, 0.0);
        }
    }

    /**
     * Sets the time of group event.
     *
     * @param sessionId   the session id
     * @param waitClassId the wait class id
     */
    public void setTimeOfGroupEvent(
            String sessionId,
            double waitClassId,
            double _count) {

        boolean isCountWaitEvent = false;

        if (waitClassId == 0.0) { // CPU
            double _cpu0 = (Double) mainSessions.get(sessionId).get(cpu);
            this.set_cpu_sum(_count);
            mainSessions.get(sessionId).put(cpu, _cpu0 + _count);
            isCountWaitEvent = true;
        } else if (waitClassId == 1.0) {// IO
            double _io1 = (Double) mainSessions.get(sessionId).get(io);
            this.set_io_sum(_count);
            mainSessions.get(sessionId).put(io, _io1 + _count);
            isCountWaitEvent = true;
        } else if (waitClassId == 2.0) {// Lock
            double _lock2 = (Double) mainSessions.get(sessionId).get(lock);
            this.set_lock_sum(_count);
            mainSessions.get(sessionId).put(lock, _lock2 + _count);
            isCountWaitEvent = true;
        } else if (waitClassId == 3.0) {// LWLock
            double _lwlock3 = (Double) mainSessions.get(sessionId).get(lwlock);
            this.set_lwlock_sum(_count);
            mainSessions.get(sessionId).put(lwlock, _lwlock3 + _count);
            isCountWaitEvent = true;
        } else if (waitClassId == 4.0) {// BufferPin
            double _bufferpin4 = (Double) mainSessions.get(sessionId).get(bufferpin);
            this.set_bufferpin_sum(_count);
            mainSessions.get(sessionId).put(bufferpin, _bufferpin4 + _count);
            isCountWaitEvent = true;
        } else if (waitClassId == 5.0) {// Activity
            double _activity5 = (Double) mainSessions.get(sessionId).get(activity);
            this.set_activity_sum(_count);
            mainSessions.get(sessionId).put(activity, _activity5 + _count);
            isCountWaitEvent = true;

        } else if (waitClassId == 6.0) {// Extension
            double _extension6 = (Double) mainSessions.get(sessionId).get(extension);
            this.set_extension_sum(_count);
            mainSessions.get(sessionId).put(extension, _extension6 + _count);
            isCountWaitEvent = true;

        } else if (waitClassId == 7.0) {// Client
            double _client7 = (Double) mainSessions.get(sessionId).get(client);
            this.set_client_sum(_count);
            mainSessions.get(sessionId).put(client, _client7 + _count);
            isCountWaitEvent = true;
        } else if (waitClassId == 8.0) {// IPC
            double _ipc8 = (Double) mainSessions.get(sessionId).get(ipc);
            this.set_ipc_sum(_count);
            mainSessions.get(sessionId).put(ipc, _ipc8 + _count);
            isCountWaitEvent = true;
        } else if (waitClassId == 9.0) {// Timeout
            double _timeout9 = (Double) mainSessions.get(sessionId).get(timeout);
            this.set_timeout_sum(_count);
            mainSessions.get(sessionId).put(timeout, _timeout9 + _count);
            isCountWaitEvent = true;
        }

        /** Set count of sql rows */
        if (isCountWaitEvent) {
            double _counts = (Double) mainSessions.get(sessionId).get(COUNT);
            this.setCountSql_sum(_count);
            mainSessions.get(sessionId).put(COUNT, _counts + _count);
        }
    }

    /**
     * Sets the time of group event.
     *
     * @param waitEvent   the wait event
     * @param waitClassId the wait class id
     */
    public void setTimeOfEventName(
            String sessionId,
            double waitClassId,
            String waitEvent,
            double _count) {

        if (!mainSessions.get(sessionId).containsKey(waitEvent)) {
            mainSessions.get(sessionId).put(waitEvent, _count);
            if (!eventList.contains(waitEvent)) {
                eventList.add(waitEvent);
            }
        } else {
            double _eventCount = (Double) mainSessions.get(sessionId).get(waitEvent);
            mainSessions.get(sessionId).put(waitEvent, _eventCount + _count);
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
     * Add the username to local BDB.
     *
     * @param userId   the user id
     * @param username the username
     */
    public void addUsername(Long userId, String username) {
        try {
            this.dao.getUserIdUsernameById().putNoReturn(
                    new AshUserIdUsername(userId, username));
        } catch (DatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Gets the username from local BDB.
     *
     * @param userId the user id
     * @return the username
     */
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
     * Clear.
     */
    public void clear() {

        mainSessions.clear();

        _cpu_sum = 0;
        _lwlock_sum = 0;
        _lock_sum = 0;
        _bufferpin_sum = 0;
        _activity_sum = 0;
        _extension_sum = 0;
        _client_sum = 0;
        _ipc_sum = 0;
        _timeout_sum = 0;
        _io_sum = 0;
        _countSql = 0;
    }

    /**
     * Gets the main sessions.
     *
     * @return the main sessions
     */
    public HashMap<String, HashMap<String, Object>> getMainSessions() {
        return mainSessions;
    }

    /**
     * Sets the main sqls.
     *
     * @param mainSessions the main sessions
     */
    public void setMainSqls(HashMap<String, HashMap<String, Object>> mainSessions) {
        this.mainSessions = mainSessions;
    }

    public double get_cpu_sum() {
        return _cpu_sum;
    }

    public double get_lwlock_sum() {
        return _lwlock_sum;
    }

    /**
     * Gets the _lock_sum.
     *
     * @return the _lock_sum
     */
    public double get_lock_sum() {
        return _lock_sum;
    }

    /**
     * Gets the _bufferpin_sum.
     *
     * @return the _bufferpin_sum
     */
    public double get_bufferpin_sum() {
        return _bufferpin_sum;
    }

    /**
     * Gets the _activity_sum.
     *
     * @return the _activity_sum
     */
    public double get_activity_sum() {
        return _activity_sum;
    }

    /**
     * Gets the _extension_sum.
     *
     * @return the _extension_sum
     */
    public double get_extension_sum() {
        return _extension_sum;
    }

    /**
     * Gets the _client_sum.
     *
     * @return the _client_sum
     */
    public double get_client_sum() {
        return _client_sum;
    }

    /**
     * Gets the _ipc_sum.
     *
     * @return the _ipc_sum
     */
    public double get_ipc_sum() {
        return _ipc_sum;
    }

    /**
     * Gets the _user i o8_sum.
     *
     * @return the _user i o8_sum
     */
    public double get_timeout_sum() {
        return _timeout_sum;
    }

    /**
     * Gets the _system i o9_sum.
     *
     * @return the _system i o9_sum
     */
    public double get_io_sum() {
        return _io_sum;
    }


    public void set_cpu_sum(double _cpu_sum) {
        this._cpu_sum = this._cpu_sum + _cpu_sum;
    }

    public void set_lwlock_sum(double _lwlock_sum) {
        this._lwlock_sum = this._lwlock_sum + _lwlock_sum;
    }

    /**
     * Sets the _lock_sum.
     *
     * @param _lock_sum the new _lock_sum
     */
    public void set_lock_sum(double _lock_sum) {
        this._lock_sum = this._lock_sum + _lock_sum;
    }

    /**
     * Sets the _bufferpin_sum.
     *
     * @param _bufferpin_sum the new _bufferpin_sum
     */
    public void set_bufferpin_sum(double _bufferpin_sum) {
        this._bufferpin_sum = this._bufferpin_sum + _bufferpin_sum;
    }

    /**
     * Sets the _activity_sum.
     *
     * @param _activity_sum the new _activity_sum
     */
    public void set_activity_sum(double _activity_sum) {
        this._activity_sum = this._activity_sum + _activity_sum;
    }

    /**
     * Sets the _extension_sum.
     *
     * @param _extension_sum the new _extension_sum
     */
    public void set_extension_sum(double _extension_sum) {
        this._extension_sum = this._extension_sum + _extension_sum;
    }

    /**
     * Sets the _client_sum.
     *
     * @param _client_sum the new _client_sum
     */
    public void set_client_sum(double _client_sum) {
        this._client_sum = this._client_sum + _client_sum;
    }

    /**
     * Sets the _ipc_sum.
     *
     * @param _ipc_sum the new _ipc_sum
     */
    public void set_ipc_sum(double _ipc_sum) {
        this._ipc_sum = this._ipc_sum + _ipc_sum;
    }

    /**
     * Sets the _user i o8_sum.
     *
     * @param _userio8_sum the new _user i o8_sum
     */
    public void set_timeout_sum(double _userio8_sum) {
        this._timeout_sum = this._timeout_sum + _userio8_sum;
    }

    /**
     * Sets the _system i o9_sum.
     *
     * @param _systemio9_sum the new _system i o9_sum
     */
    public void set_io_sum(double _systemio9_sum) {
        this._io_sum = this._io_sum + _systemio9_sum;
    }

    /**
     * Sets the _scheduler10_sum.
     *
     * @param _scheduler10_sum the new _scheduler10_sum
     */
    /**
     * Gets the _sum.
     *
     * @return the _sum
     */
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
        this._sum = this.get_activity_sum()
                + this.get_lock_sum()
                + this.get_client_sum()
                + this.get_extension_sum()
                + this.get_bufferpin_sum()
                + this.get_ipc_sum()
                + this.get_lwlock_sum()
                + this.get_cpu_sum()
                + this.get_io_sum()
                + this.get_timeout_sum();
    }

}
