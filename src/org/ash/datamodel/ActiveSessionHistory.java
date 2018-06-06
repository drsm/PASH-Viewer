/*
 *-------------------
 * The ActiveSessionHistory.java is part of ASH Viewer
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
package org.ash.datamodel;


import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;
import static com.sleepycat.persist.model.DeleteAction.NULLIFY;
import static com.sleepycat.persist.model.Relationship.MANY_TO_ONE;

/**
 * The Class ActiveSessionHistory.
 */
@Entity
public
class ActiveSessionHistory {

    /** The active session history id. */
    @PrimaryKey(sequence="activeSessionHistoryId")
    long activeSessionHistoryId;
    
    /** The sample id. */
    @SecondaryKey(relate = MANY_TO_ONE, relatedEntity=AshIdTime.class,
                                        onRelatedEntityDelete=NULLIFY)
    long sampleId;

    /** The session id. */
    long sessionId;
    
	/** The session type. */
	String backend_type;
	
	/** The user id. */
	long userId;
	String userName;
	/** The sql id. */
	String sqlId;

	/** The event. */
	String command_type;

	/** The event. */
	String event;

	/** The wait class. */
	String waitClass;
	
	/** The wait class id. */
	double waitClassId;
	
	/** The program. */
	String program;
	String hostname;

	public ActiveSessionHistory(long activeSessionHistoryId, long sampleId, long sessionId, String sessionType, long userId, String userName, String sqlId, String command_type, String event, String waitClass, double waitClassId, String program, String hostname) {


		this.activeSessionHistoryId = activeSessionHistoryId;
		this.sampleId = sampleId;

		this.sessionId = sessionId;
		this.backend_type = sessionType;
		this.userId = userId;
		this.userName = userName;
		this.sqlId = sqlId;
		this.event = event;
		this.command_type = command_type;
		this.waitClass = waitClass;
		this.waitClassId = waitClassId;
		this.program = program;
		this.hostname = hostname;
	}

    /**
     * Instantiates a new active session history.
     */
    ActiveSessionHistory() {} // For bindings

	/**
	 * Gets the active session history id.
	 * 
	 * @return the active session history id
	 */
	public long getActiveSessionHistoryId() {
		return activeSessionHistoryId;
	}

	/**
	 * Gets the sample id.
	 * 
	 * @return the sample id
	 */
	public long getSampleId() {
		return sampleId;
	}

	/**
	 * Gets the session id.
	 * 
	 * @return the session id
	 */
	public long getSessionId() {
		return sessionId;
	}

	/**
	 * Gets the session type.
	 * 
	 * @return the session type
	 */
	public String getBackend_type() {
		return backend_type;
	}

	/**
	 * Gets the user id.
	 * 
	 * @return the user id
	 */
	public long getUserId() {
		return userId;
	}

	/**
	 * Gets the sql id.
	 * 
	 * @return the sql id
	 */
	public String getUserName() {
		return userName;
	}
	public String getSqlId() {
		return sqlId;
	}

	/**
	 * Gets the event.
	 * 
	 * @return the event
	 */
	public String getEvent() {
		return event;
	}
	public String getCommand_type() {
		return command_type;
	}

	/**
	 * Gets the event id.
	 * 
	 * @return the event id
	 */

	/**
	 * Gets the wait class.
	 * 
	 * @return the wait class
	 */
	public String getWaitClass() {
		return waitClass;
	}

	/**
	 * Gets the wait class id.
	 * 
	 * @return the wait class id
	 */
	public double getWaitClassId() {
		return waitClassId;
	}


	/**
	 * Gets the program.
	 * 
	 * @return the program
	 */
	public String getProgram() {
		return program;
	}

	public String getHostname() {
		return hostname;
	}


	/**
	 * Gets the module.
	 * 
	 * @return the module
	 */

	/**
	 * Constructs a <code>String</code> with all attributes
	 * in name = value format.
	 *
	 * @return a <code>String</code> representation 
	 * of this object.
	 */
	@Override
	public String toString() {
		final String TAB = "    ";

		String retValue = "";

		retValue = "ActiveSessionHistory ( "
				+ super.toString() + TAB
				+ "activeSessionHistoryId = " + this.activeSessionHistoryId + TAB
				+ "sampleId = " + this.sampleId + TAB
				+ "sessionId = " + this.sessionId + TAB
				+ "backend_type = " + this.backend_type + TAB
				+ "userId = " + this.userId + TAB
				+ "userName = " + this.userName + TAB
				+ "sqlId = " + this.sqlId + TAB
				+ "command_type = " + this.command_type + TAB
				+ "event = " + this.event + TAB
				+ "waitClass = " + this.waitClass + TAB
				+ "waitClassId = " + this.waitClassId + TAB
				+ "program = " + this.program + TAB
				+ "hostname = " + this.hostname + TAB
				+ " )";

		return retValue;
	}
}
