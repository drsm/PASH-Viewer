/*
 *-------------------
 * The AshDataAccessor.java is part of ASH Viewer
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

import java.util.HashMap;
import java.util.Iterator;

import org.ash.datamodel.ActiveSessionHistory;
import org.ash.datamodel.ActiveSessionHistory15;
import org.ash.datamodel.AshIdTime;
import org.ash.datamodel.AshParamValue;
import org.ash.datamodel.AshSqlIdTypeText;
import org.ash.datamodel.AshSqlPlanDetail;
import org.ash.datamodel.AshSqlPlanParent;
import org.ash.datamodel.AshUserIdUsername;
import org.ash.datamodel.AshWaitClass10g1;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityIndex;
import com.sleepycat.persist.EntityJoin;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.ForwardCursor;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;

/**
 * The Class AshDataAccessor.
 */
public class AshDataAccessor {
    /**
     * The active session history by id.
     */
    PrimaryIndex<Long, ActiveSessionHistory> activeSessionHistoryById;

    /**
     * The active session history by ash id.
     */
    SecondaryIndex<Long, Long, ActiveSessionHistory> activeSessionHistoryByAshId;

    /**
     * The ash by id.
     */
    PrimaryIndex<Long, AshIdTime> ashById;

    /**
     * The ash by sample time.
     */
    SecondaryIndex<Double, Long, AshIdTime> ashBySampleTime;

    /**
     * AshCalcSumByEvent10Sec Accessors
     */
    PrimaryIndex<Double, ActiveSessionHistory15> ashCalcSumByEventById115Sec;

    /**
     * The sqlId, commandType and sqlText.
     */
    PrimaryIndex<String, AshSqlIdTypeText> ashSqlIdTypeTextId;

    /**
     * The user id - username.
     */
    PrimaryIndex<Long, AshUserIdUsername> userIdUsernameById;

    /**
     * The parameters and values.
     */
    PrimaryIndex<String, AshParamValue> ashParamValue;

    /**
     * The user id - username.
     */
    PrimaryIndex<Long, AshWaitClass10g1> eventIdWaitClass10g1;

    /**
     * The sql plan by id (detail).
     */
    PrimaryIndex<Long, AshSqlPlanDetail> ashSqlPlanPKDetail;

    /**
     * The sql plan by hash value (detail).
     */
    SecondaryIndex<Double, Long, AshSqlPlanDetail> ashSqlPlanHashValueDetail;

    /**
     * The sql plan by sql id (detail).
     */
    SecondaryIndex<String, Long, AshSqlPlanDetail> ashSqlPlanHashValueSqlIdDetail;

    /**
     * The sql plan hash value (parent).
     */
    PrimaryIndex<Double, AshSqlPlanParent> ashSqlPlanPKParent;

    /**
     * The sql plan by hash value (parent).
     */
    SecondaryIndex<String, Double, AshSqlPlanParent> ashSqlPlanHashValueParent;

    /**
     * The half range for one 15 sec storage
     */
    private int rangeHalf = 7500;

    /**
     * Local begin time
     */
    private double beginTimeOnRun = 0.0;

    /**
     * The store last pare eventClassId(key) and eventName(value) fot details
     */
    private HashMap<String, String> eventClassName;

    /**
     * Instantiates a new ash data accessor.
     *
     * @param store the store
     * @throws DatabaseException the database exception
     */
    public AshDataAccessor(EntityStore store)
            throws DatabaseException {

        eventClassName = new HashMap<String, String>();

        /* Primary key for ActiveSessionHistory classes. */
        activeSessionHistoryById = store.getPrimaryIndex(Long.class, ActiveSessionHistory.class);

        /* Secondary key for ActiveSessionHistory classes. */
        activeSessionHistoryByAshId = store.getSecondaryIndex(activeSessionHistoryById, Long.class, "sampleId");

        /* Primary key for Ash classes. */
        ashById = store.getPrimaryIndex(Long.class, AshIdTime.class);

        /* Secondary key for Ash classes. */
        ashBySampleTime = store.getSecondaryIndex(ashById, Double.class, "sampleTime");

        /* Primary key for ashCalcSumByEventById10Sec. */
        ashCalcSumByEventById115Sec = store.getPrimaryIndex(Double.class, ActiveSessionHistory15.class);

        /* Primary key for AshSqlIdTypeText classes. */
        ashSqlIdTypeTextId = store.getPrimaryIndex(String.class, AshSqlIdTypeText.class);

        /* Primary key for UserIdUsername class. */
        userIdUsernameById =
                store.getPrimaryIndex(Long.class, AshUserIdUsername.class);

        /* Primary key for AshParamValue class. */
        ashParamValue =
                store.getPrimaryIndex(String.class, AshParamValue.class);

        /* Primary key for AshWaitClass10g1 class. */
        eventIdWaitClass10g1 =
                store.getPrimaryIndex(Long.class, AshWaitClass10g1.class);

        /* Primary key for AshSqlPlanDetail. */
        ashSqlPlanPKDetail =
                store.getPrimaryIndex(Long.class, AshSqlPlanDetail.class);

        /* Secondary key for AshSqlPlanDetail (by plan hash value). */
        ashSqlPlanHashValueDetail =
                store.getSecondaryIndex(ashSqlPlanPKDetail, Double.class, "planHashValue");

        /* Secondary key for AshSqlPlanDetail (by sql id value). */
        ashSqlPlanHashValueSqlIdDetail =
                store.getSecondaryIndex(ashSqlPlanPKDetail, String.class, "sqlId");

        /* Primary key for AshSqlPlanParent. */
        ashSqlPlanPKParent =
                store.getPrimaryIndex(Double.class, AshSqlPlanParent.class);

        /* Secondary key for AshSqlPlanParent. */
        ashSqlPlanHashValueParent =
                store.getSecondaryIndex(ashSqlPlanPKParent, String.class, "sqlId");
    }


    /**
     * Do prefix query.
     *
     * @param index  the index
     * @param prefix the prefix
     * @return the entity cursor< v>
     * @throws DatabaseException the database exception
     */
    public <V> EntityCursor<V> doPrefixQuery(EntityIndex<String, V> index,
                                             String prefix)
            throws DatabaseException {

        assert (index != null);
        assert (prefix.length() > 0);

        /* Opens a cursor for traversing entities in a key range. */
        char[] ca = prefix.toCharArray();
        final int lastCharIndex = ca.length - 1;
        ca[lastCharIndex]++;
        return doRangeQuery(index, prefix, true, String.valueOf(ca), false);
    }

    /**
     * Do range query.
     *
     * @param index         the index
     * @param fromKey       the from key
     * @param fromInclusive the from inclusive
     * @param toKey         the to key
     * @param toInclusive   the to inclusive
     * @return the entity cursor< v>
     * @throws DatabaseException the database exception
     */
    public <K, V> EntityCursor<V> doRangeQuery(EntityIndex<K, V> index,
                                               K fromKey,
                                               boolean fromInclusive,
                                               K toKey,
                                               boolean toInclusive)
            throws DatabaseException {

        assert (index != null);

        /* Opens a cursor for traversing entities in a key range. */
        return index.entities(fromKey,
                fromInclusive,
                toKey,
                toInclusive);
    }

    /**
     * Do two conditions join.
     *
     * @param pk   the pk
     * @param sk1  the sk1
     * @param key1 the key1
     * @param sk2  the sk2
     * @param key2 the key2
     * @return the forward cursor< e>
     * @throws DatabaseException the database exception
     */
    public <PK, SK1, SK2, E> ForwardCursor<E>
    doTwoConditionsJoin(PrimaryIndex<PK, E> pk,
                        SecondaryIndex<SK1, PK, E> sk1,
                        SK1 key1,
                        SecondaryIndex<SK2, PK, E> sk2,
                        SK2 key2)
            throws DatabaseException {

        assert (pk != null);
        assert (sk1 != null);
        assert (sk2 != null);

        EntityJoin<PK, E> join = new EntityJoin<PK, E>(pk);
        join.addCondition(sk1, key1);
        join.addCondition(sk2, key2);

        return join.entities();
    }


    /**
     * Load ash calc sum by event by id 10 sec on run.
     *
     * @param startSampleTime the start sample time
     * @param endSampleTime   the end sample time
     * @throws DatabaseException the database exception
     */
    public void loadAshCalcSumByEventById15SecOnRun(double startSampleTime,
                                                    double endSampleTime,
                                                    ASHDatabase database)
            throws DatabaseException {

        if (beginTimeOnRun == 0.0) {
            beginTimeOnRun = startSampleTime;
        }

        loadAshCalcSumByEventById15Sec(startSampleTime, endSampleTime);
    }

    /**
     * Delete all data from period.
     *
     * @param start
     * @param end
     */
    public void deleteData(long start, long end) {
        deleteDataP(start, end);
    }

    /**
     * Load ash sum by event by id 15 sec.
     *
     * @param startSampleTime the start sample time
     * @param endSampleTime   the end sample time
     * @throws DatabaseException the database exception
     */

    private void loadAshCalcSumByEventById15Sec(double startSampleTime,
                                                double endSampleTime) throws DatabaseException {

	int asd = 0;
        for (double i = beginTimeOnRun; i < endSampleTime; i += rangeHalf * 2) {

		// dcvetkov
		// ¬от этот цикл отрабатывал два раза и делал 2 записи, одну из которых подватывал график
		// ≈сли i+rangeHalf*2 получалс€ больше текущего времени, то сумма событий получалась маленька€ и график "падал"
		if(i+rangeHalf*2 > System.currentTimeMillis()) continue;

            /* Init temporary variables */
            double countAAS = 0;

            double cpu = 0;
            double lwlock = 0;
            double lock = 0;
            double bufferpin = 0;
            double activity = 0;
            double extension = 0;
            double client = 0;
            double ipc = 0;
            double timeout = 0;
            double io = 0;

            int rangeHalfSec = (rangeHalf * 2) / 1000;

            /* Do a filter on AshIdTime by SampleTime. */
            EntityCursor<AshIdTime> ashIdTimeCursor = doRangeQuery(this.ashBySampleTime, i, true, i + rangeHalf * 2, false);
            Iterator<AshIdTime> ashIdTimeIter = ashIdTimeCursor.iterator();

            // Iterate over AshIdTime
            while (ashIdTimeIter.hasNext()) {
                AshIdTime ashIdTimeMain = ashIdTimeIter.next();

                /* Do a filter on ActiveSessionHistory by SampleID. */
                EntityCursor<ActiveSessionHistory> ActiveSessionHistoryCursor = this.activeSessionHistoryByAshId.subIndex(ashIdTimeMain.getsampleId()).entities();
                Iterator<ActiveSessionHistory> ActiveSessionHistoryIter = ActiveSessionHistoryCursor.iterator();

                // Iterate over ActiveSessionHistory
                while (ActiveSessionHistoryIter.hasNext()) {
                    ActiveSessionHistory ASH = ActiveSessionHistoryIter.next();
                    int count = 1;
                    boolean isCountWaitEvent = false;

                    if (ASH.getWaitClass().equals("CPU")) {
                        cpu = cpu + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("LWLock")) {
                        lwlock = lwlock + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("Lock")) {
                        lock = lock + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("BufferPin")) {
                        bufferpin = bufferpin + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("Activity")) {
                        activity = activity + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("Extension")) {
                        extension = extension + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("Client")) {
                        client = client + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("IPC")) {
                        ipc = ipc + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("Timeout")) {
                        timeout = timeout + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("IO")) {
                        io = io + count;
                        isCountWaitEvent = true;
                    }

                    setEventClassName(ASH.getWaitClass(), ASH.getEvent());

                    if (isCountWaitEvent) {
                        countAAS++;
                    }
                }
                ActiveSessionHistoryCursor.close();
            }
            ashIdTimeCursor.close();


            // Calculate sum of all event (current window)
            double sumEvent = cpu + lwlock + lock + bufferpin + activity + extension + client + ipc + timeout + io;

            if (sumEvent == 0.0) {
                sumEvent = 0.000001;
            }

            try {
                // Save System Event and CPU statistics
                this.ashCalcSumByEventById115Sec
                        .put(new ActiveSessionHistory15
                                (i + rangeHalf,
                                        countAAS,
                                        ((cpu / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((lwlock / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((lock / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((bufferpin / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((activity / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((extension / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((client / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((ipc / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((timeout / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((io / sumEvent) * sumEvent) / rangeHalfSec
                                ));

            } catch (Exception e) {
                e.printStackTrace();
            }

            countAAS = 0;
            cpu = 0;
            lwlock = 0;
            lock = 0;
            bufferpin = 0;
            activity = 0;
            extension = 0;
            client = 0;
            ipc = 0;
            timeout = 0;
            io = 0;

            beginTimeOnRun = i;
        }
    }

    /**
     * Load ash sum by event by id 10 sec.
     *
     * @param startSampleTime the start sample time
     * @param endSampleTime   the end sample time
     * @throws DatabaseException the database exception
     */

    private void loadAshCalcSumByEventById15Sec9i(double startSampleTime,
                                                  double endSampleTime) throws DatabaseException {

        for (double i = beginTimeOnRun; i < endSampleTime; i += rangeHalf * 2) {

            /* Init temporary variables */
            double countAAS = 0;
            double cpu = 0;
            double lwlock = 0;
            double lock = 0;
            double bufferpin = 0;
            double activity = 0;
            double extension = 0;
            double client = 0;
            double ipc = 0;
            double timeout = 0;
            double io = 0;

            int rangeHalfSec = (rangeHalf * 2) / 1000;

            /* Do a filter on AshIdTime by SampleTime. */
            EntityCursor<AshIdTime> ashIdTimeCursor =
                    doRangeQuery(this.ashBySampleTime,
                            i, true, i + rangeHalf * 2, false);
            Iterator<AshIdTime> ashIdTimeIter = ashIdTimeCursor.iterator();

            // Iterate over AshIdTime
            while (ashIdTimeIter.hasNext()) {
                AshIdTime ashIdTimeMain = ashIdTimeIter.next();

                /* Do a filter on ActiveSessionHistory by SampleID. */
                EntityCursor<ActiveSessionHistory> ActiveSessionHistoryCursor =
                        this.activeSessionHistoryByAshId.subIndex(ashIdTimeMain.getsampleId()).entities();
                Iterator<ActiveSessionHistory> ActiveSessionHistoryIter =
                        ActiveSessionHistoryCursor.iterator();

                // Iterate over ActiveSessionHistory
                while (ActiveSessionHistoryIter.hasNext()) {
                    ActiveSessionHistory ASH = ActiveSessionHistoryIter.next();
                    int count = 1;
                    boolean isCountWaitEvent = false;

                    if (ASH.getWaitClass().equals("CPU")) { //User IO 8
                        cpu = cpu + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("LWLock")) {
                        lwlock = lwlock + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("Lock")) {
                        lock = lock + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("BufferPin")) {
                        bufferpin = bufferpin + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("Activity")) {
                        activity = activity + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("Extension")) {
                        extension = extension + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("Client")) {
                        client = client + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("IPC")) {
                        ipc = ipc + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("Timeout")) {
                        timeout = timeout + count;
                        isCountWaitEvent = true;
                    } else if (ASH.getWaitClass().equals("IO")) {
                        io = io + count;
                        isCountWaitEvent = true;
                    }

                    setEventClassName(ASH.getWaitClass(), ASH.getEvent());

                    if (isCountWaitEvent) {
                        countAAS++;
                    }

                }
                ActiveSessionHistoryCursor.close();
            }
            ashIdTimeCursor.close();


            // Calculate sum of all event (current window)
            double sumEvent = cpu + lwlock + lock + bufferpin + activity + extension + client + ipc + timeout + io;
            if (sumEvent == 0.0) {
                sumEvent = 0.000001;
            }

            try {
                // Save System Event and CPU statistics
                this.ashCalcSumByEventById115Sec
                        .put(new ActiveSessionHistory15
                                (i + rangeHalf,
                                        countAAS,
                                        ((cpu / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((lwlock / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((lock / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((bufferpin / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((activity / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((extension / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((client / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((ipc / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((timeout / sumEvent) * sumEvent) / rangeHalfSec,
                                        ((io / sumEvent) * sumEvent) / rangeHalfSec));

            } catch (Exception e) {
                e.printStackTrace();
            }

            countAAS = 0;
            cpu = 0;
            lwlock = 0;
            lock = 0;
            bufferpin = 0;
            activity = 0;
            extension = 0;
            client = 0;
            ipc = 0;
            timeout = 0;
            io = 0;

            beginTimeOnRun = i;
        }
    }

    /**
     * Delete all data from period.
     *
     * @param start
     * @param end
     */
    private void deleteDataP(long start, long end) {

        double startKey = start;
        double endKey = end;

        // Delete ActiveSessionHistory15 entity
        try {

            EntityCursor<AshIdTime> ashIdTimeCursor =
                    doRangeQuery(this.ashBySampleTime,
                            startKey, true, endKey, true);
            Iterator<AshIdTime> ashIdTimeIterator =
                    ashIdTimeCursor.iterator();

            try {
                // Iterate over AshIdTime
                while (ashIdTimeIterator.hasNext()) {
                    AshIdTime ashIdTimeMain = ashIdTimeIterator.next();

                    /* Do a filter on ActiveSessionHistory by SampleID. */
                    EntityCursor<ActiveSessionHistory> ActiveSessionHistoryCursor =
                            this.activeSessionHistoryByAshId.subIndex(ashIdTimeMain.getsampleId()).entities();

                    try {
                        for (ActiveSessionHistory entity = ActiveSessionHistoryCursor.first();
                             entity != null;
                             entity = ActiveSessionHistoryCursor.next()) {
                            /* Delete data from ActiveSessionHistory15 entry*/
                            ActiveSessionHistoryCursor.delete();
                        }
                    } finally {
                        ActiveSessionHistoryCursor.close();
                    }
                }
            } finally {
                ashIdTimeCursor.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Delete ActiveSessionHistory15 entity
        try {
            EntityCursor<ActiveSessionHistory15> ashActiveSessionHistory15 =
                    doRangeQuery(this.ashCalcSumByEventById115Sec,
                            startKey, true, endKey, true);
            try {
                for (ActiveSessionHistory15 entity = ashActiveSessionHistory15.first();
                     entity != null;
                     entity = ashActiveSessionHistory15.next()) {
                    /* Delete data from ActiveSessionHistory15 entry*/
                    ashActiveSessionHistory15.delete();
                }
            } finally {
                ashActiveSessionHistory15.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Delete AshIdTime entity
        try {
            EntityCursor<AshIdTime> ashIdTimeCursor =
                    doRangeQuery(this.ashBySampleTime,
                            startKey, true, endKey, true);
            try {
                for (AshIdTime entity = ashIdTimeCursor.first();
                     entity != null;
                     entity = ashIdTimeCursor.next()) {
                    /* Delete data from ActiveSessionHistory15 entry*/
                    ashIdTimeCursor.delete();
                }
            } finally {
                ashIdTimeCursor.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Return Event Name for last Event class
     *
     * @return the eventClassName
     */
    public String getEventClassName(String key) {
        return eventClassName.get(key);
    }

    /**
     * Set Event Name for last Event class
     *
     * @param eventClass the eventClass to set
     * @param eventName  the eventNAme to set
     */
    public void setEventClassName(String eventClass, String eventName) {
        if (this.eventClassName.get(eventClass) == null) {
            this.eventClassName.put(eventClass, eventName);
        }
    }


    public void setAshBySampleTime(SecondaryIndex<Double, Long, AshIdTime> ashBySampleTime) {
        this.ashBySampleTime = ashBySampleTime;
    }


    public SecondaryIndex<Double, Long, AshIdTime> getAshBySampleTime() {
        return ashBySampleTime;
    }


    /**
     * @return the activeSessionHistoryById
     */
    public PrimaryIndex<Long, ActiveSessionHistory> getActiveSessionHistoryById() {
        return activeSessionHistoryById;
    }


    /**
     * @return the activeSessionHistoryByAshId
     */
    public SecondaryIndex<Long, Long, ActiveSessionHistory> getActiveSessionHistoryByAshId() {
        return activeSessionHistoryByAshId;
    }


    /**
     * @return the ashById
     */
    public PrimaryIndex<Long, AshIdTime> getAshById() {
        return ashById;
    }


    /**
     * @return the ashCalcSumByEventById115Sec
     */
    public PrimaryIndex<Double, ActiveSessionHistory15> getAshCalcSumByEventById115Sec() {
        return ashCalcSumByEventById115Sec;
    }


    /**
     * @return the ashSqlIdTypeTextId
     */
    public PrimaryIndex<String, AshSqlIdTypeText> getAshSqlIdTypeTextId() {
        return ashSqlIdTypeTextId;
    }


    /**
     * @return the userIdUsernameById
     */
    public PrimaryIndex<Long, AshUserIdUsername> getUserIdUsernameById() {
        return userIdUsernameById;
    }


    /**
     * @return the ashParamValue
     */
    public PrimaryIndex<String, AshParamValue> getAshParamValue() {
        return ashParamValue;
    }


    /**
     * @return the eventIdWaitClass10g1
     */
    public PrimaryIndex<Long, AshWaitClass10g1> getEventIdWaitClass10g1() {
        return eventIdWaitClass10g1;
    }


    /**
     * @return the ashSqlPlan
     */
    public PrimaryIndex<Long, AshSqlPlanDetail> getAshSqlPlanPKDetail() {
        return ashSqlPlanPKDetail;
    }


    /**
     * @return the ashSqlPlanHashValue
     */
    public SecondaryIndex<Double, Long, AshSqlPlanDetail> getAshSqlPlanHashValueDetail() {
        return ashSqlPlanHashValueDetail;
    }


    /**
     * @return the ashSqlPlanPKParent
     */
    public PrimaryIndex<Double, AshSqlPlanParent> getAshSqlPlanPKParent() {
        return ashSqlPlanPKParent;
    }


    /**
     * @return the ashSqlPlanHashValueParent
     */
    public SecondaryIndex<String, Double, AshSqlPlanParent> getAshSqlPlanHashValueParent() {
        return ashSqlPlanHashValueParent;
    }


    /**
     * @return the ashSqlPlanHashValueSqlIdDetail
     */
    public SecondaryIndex<String, Long, AshSqlPlanDetail> getAshSqlPlanHashValueSqlIdDetail() {
        return ashSqlPlanHashValueSqlIdDetail;
    }


}
