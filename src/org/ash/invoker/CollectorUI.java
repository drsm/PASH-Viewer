/*
 *-------------------
 * The CollectorUI.java is part of ASH Viewer
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

package org.ash.invoker;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.ash.database.ASHDatabase;
import org.ash.detail.DetailPanels;
import org.ash.gui.Gantt;
import org.ash.gui.StackedChart;
import org.ash.gui.StatusBar;

/**
 * The Class CollectorAsh9iLower.
 */
public class CollectorUI implements Runnable, Collector {

  /** The m_is running. */
  private boolean m_isRunning = false;
  
  /** The m_latency. */
  private long m_latency = 31000;
  
  /** The m_latency chart. */
  private long m_latencyChart = 200;
  
  /** The m_stop. */
  private boolean m_stop = true;

  /** The database. */
  private ASHDatabase database;
  
  /** The m_thread. */
  private Thread m_thread;
  
  /** The listeners start. */
  private List listenersStart = new ArrayList();
  
  /** The listeners stop. */
  private List listenersStop = new ArrayList();
 
  /** The end time. */
  private double endTime = 0.0;
  
  /** The begin time. */
  private double beginTime = 0.0;
  
  /** The range window. */
  private int rangeWindow = 300000; // 5 minutes default
  
  /** The k. 1 minute */
  private int k = 60000;
  
  /**
   * Instantiates a new collector ash9i lower.
   * 
   * @param database0 the database0
   * @param _latency the _latency
   */
  public CollectorUI( ASHDatabase database0, final long _latency) {
    super();
    this.database = database0;
    this.m_latency = _latency;
    }

  /* (non-Javadoc)
   * @see java.lang.Object#finalize()
   */
  @Override
public void finalize() throws Throwable {
    super.finalize();
    this.stop();
  }

  /* (non-Javadoc)
 * @see org.ash.invoker.CollectorAsh#getLatency()
 */
  public long getLatency() {
    return this.m_latency;
  }
  
  /* (non-Javadoc)
 * @see org.ash.invoker.CollectorAsh#isRunning()
 */
  public boolean isRunning() {
    return this.m_isRunning;
  }

  /* (non-Javadoc)
   * @see java.lang.Runnable#run()
   */
  public void run() {
	    if (Thread.currentThread() != this.m_thread) {
	      throw new IllegalStateException(
	          "You cannot start an own thread for data collectors. Use collector.start()!");
	    }
	    
	    long lasttime;
	    this.m_stop = false;
	    long m_latencyBDBCollector = 1000;
	    long m_latencyTmp = 0;


	    while (!this.m_stop) {
	      lasttime = System.currentTimeMillis();
	      
		// ��� ����� �� ������ ?! ����� ������ ��� � ������� ������ ������ ������ � ��������� �� ?
		// database.loadToLocalBDBCollector();
	     
	      if (m_latencyTmp >= this.m_latency){

	    	  // Wait while user mouse dragged
	          while (isSelectionStackedChart()){
	        	  try {
	        	        Thread.sleep(m_latencyChart);
	        	      } catch (InterruptedException e) {
	        	    	 System.out.println("Error when wait for user dragged!!!");
	        	    	 e.printStackTrace();
	        	        this.stop();
	        	      }
	          }
	          
	          database.loadToSubByEventAnd10Sec();
	    	  database.updateDataToChartPanelDataSet();
	    	  fireRunAction();
	    	  m_latencyTmp = 0;
	      }
	      
	      try {
		// �������� �� 1 ������� - �����, ��������� � �������� ����
	        Thread.sleep(Math.max(m_latencyBDBCollector/*this.m_latency*/ - (System.currentTimeMillis() - lasttime), 0));
	        m_latencyTmp = m_latencyTmp + m_latencyBDBCollector; /* + 1000 */
	      } catch (InterruptedException e) {
	    	 System.out.println("Draw print stack of threads!!!");
	    	 e.printStackTrace();
	        this.stop();
	      }
	      if (Thread.interrupted()) {
	    	  System.out.println("stopped!!!");
	        this.stop();
	      }      
	    }

	  }

  /* (non-Javadoc)
 * @see org.ash.invoker.CollectorAsh#setLatency(long)
 */
  public void setLatency(final long latency) {
    this.m_latency = latency;
  }

  /* (non-Javadoc)
 * @see org.ash.invoker.CollectorAsh#start()
 */
  public void start() {
	  
	    if (this.m_thread == null) {
	      this.m_thread = new Thread(this);
	      
	      // Wait this.m_latency for start first update.
	      try {
	          Thread.sleep(this.m_latency);
	        } catch (InterruptedException e) {
	      	 System.out.println("Draw print stack of threads!!!");
	      	 e.printStackTrace();
	          this.stop();
	        }
	      
	      this.m_thread.start();
	    }
	    
  }

  /* (non-Javadoc)
 * @see org.ash.invoker.CollectorAsh#stop()
 */
  public void stop() {
    this.m_stop = true;
    fireStopAction();
  }
  
  /* (non-Javadoc)
 * @see org.ash.invoker.CollectorAsh#removeListenerStart(java.lang.Object)
 */
  public boolean removeListenerStart(Object l) {
	// Wait until collector end the fireStartAction
	  while (this.m_isRunning){
		  try {
		        Thread.sleep(m_latencyChart);
		      } catch (InterruptedException e) {
		    	 System.out.println("Draw print stack of threads!!!");
		    	 e.printStackTrace();
		      }
	  }
		return listenersStart.remove(l);
	}
  
  /* (non-Javadoc)
 * @see org.ash.invoker.CollectorAsh#addListenerStart(java.lang.Object)
 */
  public void addListenerStart(Object l) {
	  	listenersStart.add(l);
   }
  
  /*
   * @see org.ash.invoker.CollectorAsh#isListenerExist(java.lang.Object)
   */
   public boolean isListenerExist(Object l) {
 	  Iterator iStart = listenersStart.iterator();
 	  while (iStart.hasNext()) {
 		  Object currListeners = iStart.next();		  
 		  if (l.getClass() == currListeners.getClass()){
 			  return true;
 		  }
 	  }
 	   return false;
   }
  
  /* (non-Javadoc)
 * @see org.ash.invoker.CollectorAsh#removeListenerStop(java.lang.Object)
 */
  public boolean removeListenerStop(Object l) {
		return listenersStop.remove(l);
	}
  /* (non-Javadoc)
 * @see org.ash.invoker.CollectorAsh#addListenerStop(java.lang.Object)
 */
  public void addListenerStop(Object l) {
	  listenersStop.add(l);
  }
  
  /**
   * Fire run action.
   */
  protected void fireRunAction() {
   this.m_isRunning = true;
      
	  Iterator iStart = listenersStart.iterator();
      
      // Begin/end time when selection is auto
	  endTime = database.getSysdate();
	  beginTime = endTime - rangeWindow; // 5 minutes default
      
      while (iStart.hasNext()) {
    	  Object currListeners = iStart.next();
    	  // For StackedXYAreaChart update data label (dd.MM.yyyy format)
    	  if (currListeners instanceof StackedChart){
    		  StackedChart 
    		  	tempObj = (StackedChart) currListeners;
    		  	tempObj.updatexAxisLabel(
    		  			new Long(new Date().getTime()).doubleValue());
    		  	if (tempObj.isFlagThresholdBeginTimeAutoSelection()){
    		  		tempObj.setThresholdBeginTimeAutoSelection(beginTime,rangeWindow/k);
    		  	}
    	  }
    	  if (currListeners instanceof DetailPanels){
    		  DetailPanels 
  		  			tempObj = (DetailPanels) currListeners;
    		  tempObj.updatexAxisLabel(
  		  			new Long(new Date().getTime()).doubleValue());
    	  }
    	  if (currListeners instanceof Gantt){
    		  Gantt 
  		  		tempObj = (Gantt) currListeners;
    		  	tempObj.loadDataToJPanels(beginTime, endTime);
    	  }
    	  if (currListeners instanceof StatusBar){
        		StatusBar 
        		  	tempObj = (StatusBar) currListeners;
        		  	tempObj.setRange(beginTime, endTime);
        		  	tempObj.setSelection("Auto");
        	  }
      }
      this.m_isRunning = false;
   }
  
  /**
   * Fire stop action.
   */
  protected void fireStopAction() {
      Iterator iStop = listenersStop.iterator();
      while (iStop.hasNext()) {
    	  Object 
    	  	currListeners = iStop.next();
      }
   }
  
  /**
   * Checks if is selection stacked chart.
   * 
   * @return true, if is selection stacked chart
   */
  protected boolean isSelectionStackedChart() {
	  boolean tmp = false;
      Iterator iStart = listenersStart.iterator();
      while (iStart.hasNext()) {
    	  Object 
    	  	currListeners = iStart.next();
    	  if (currListeners instanceof StackedChart){
    		  StackedChart 
    		  	tempObj = (StackedChart) currListeners;
    		  tmp = tempObj.isMouseDragged();
    	  }
      }  	
	  return tmp;
   }
  
  /* (non-Javadoc)
   * @see org.ash.invoker.CollectorAsh#getBeginTime()
   */
  public double getBeginTime() {
  	return beginTime;
  }

  /* (non-Javadoc)
   * @see org.ash.invoker.CollectorAsh#getRangeWindow()
   */
  public int getRangeWindow() {
  	return rangeWindow;
  }

  /* (non-Javadoc)
   * @see org.ash.invoker.CollectorAsh#setRangeWindow(int)
   */
  public void setRangeWindow(int rangeWindow) {
		this.rangeWindow = rangeWindow*k;
  }
  
  /* (non-Javadoc)
   * @see org.ash.invoker.Collector#getK()
   */
  public int getK() {
  	return k;
  } 
  
}
