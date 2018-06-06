/*
 *-------------------
 * The ActiveSessionHistory15.java is part of ASH Viewer
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

/**
 * The Class AshCalcSumByEvent10Sec.
 */
@Entity
public
class ActiveSessionHistory15 {

    /** The sample time. */
    @PrimaryKey
    double sampleTime;
    
    /** The count active sessions. */
    double countActiveSessions;
    
	double cpu;
	double lwlock;
	double lock;
	double bufferpin;
	double activity;
	double extension;
	double client;
	double ipc;
	double timeout;
	double io;
    
    /**
     * Instantiates a new ash calc sum by event10 sec.
     */
    public ActiveSessionHistory15(double sampleTime, double countActiveSessions,
		double cpu,
		double lwlock,
		double lock,
		double bufferpin,
		double activity,
		double extension,
		double client,
		double ipc,
		double timeout,
		double io) {
        this.sampleTime = sampleTime;
        this.countActiveSessions = countActiveSessions;
        
        this.cpu = cpu;
        this.lwlock = lwlock;
        this.lock = lock;
        this.bufferpin = bufferpin;
        this.activity = activity;
        this.extension = extension;
        this.client = client;
        this.ipc = ipc;
        this.timeout = timeout;
        this.io = io;
    }

	/**
	 * Instantiates a new ash calc sum by event10 sec.
	 */
	private ActiveSessionHistory15() {} // For bindings.

    
    /**
     * Gets the count active sessions.
     * 
     * @return the count active sessions
     */
    public double getcountActiveSessions() {
        return countActiveSessions;
    }

	/**
	 * Gets the sample time.
	 * 
	 * @return the sample time
	 */
	public double getSampleTime() {
		return sampleTime;
	}

	public double getCPU() {		return cpu;	}
	public double getLWLock() {		return lwlock;	}
	public double getLock() {		return lock;	}
	public double getBufferPin() {		return bufferpin;	}
	public double getActivity() {		return activity;	}
	public double getExtension() {		return extension;	}
	public double getClient() {		return client;	}
	public double getIPC() {		return ipc;	}
	public double getTimeout() {		return timeout;	}
	public double getIO() {		return io;	}

	/**
	 * Constructs a <code>String</code> with all attributes
	 * in name = value format.
	 *
	 * @return a <code>String</code> representation 
	 * of this object.
	 */
	@Override
	public String toString()
	{
	    final String TAB = "    ";
	    
	    String retValue = "";
	    
	    retValue = "ActiveSessionHistory15 ( "
	        + super.toString() + TAB
	        + "sampleTime = " + this.sampleTime + TAB
	        + "countActiveSessions = " + this.countActiveSessions + TAB
	        + "cpu = " + this.cpu + TAB
	        + "lwlock = " + this.lwlock + TAB
	        + "lock = " + this.lock + TAB
	        + "bufferpin = " + this.bufferpin + TAB
	        + "activity = " + this.activity + TAB
	        + "extension = " + this.extension + TAB
	        + "client = " + this.client + TAB
	        + "ipc = " + this.ipc + TAB
	        + "timeout = " + this.timeout + TAB
	        + "io = " + this.io + TAB
	        + " )";
	
	    return retValue;
	}

}
