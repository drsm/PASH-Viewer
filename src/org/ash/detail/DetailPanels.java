/*
 *-------------------
 * The DetailPanels.java is part of ASH Viewer
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
package org.ash.detail;

import java.awt.BorderLayout;
import java.awt.GridLayout;
import java.awt.image.BufferedImage;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.ash.database.ASHDatabase;
import org.ash.gui.StatusBar;
import org.ash.util.Options;
import org.jfree.chart.ChartPanel;

public class DetailPanels extends JPanel{

	/** The MainFrame. */
	private JFrame mainFrame;
	
	/** The database. */
	private ASHDatabase database;
	
	/** The tabbed pane */
	private JTabbedPane tabsDetail;
	
	/** The stacked chart main object for Wait Types */
	private StackedChartDetail CPUStackedChartMainObjectDetail;
	private StackedChartDetail LWLockStackedChartMainObjectDetail;
	private StackedChartDetail LockStackedChartMainObjectDetail;
	private StackedChartDetail BufferPinStackedChartMainObjectDetail;
	private StackedChartDetail ActivityStackedChartMainObjectDetail;
	private StackedChartDetail ExtensionStackedChartMainObjectDetail;
	private StackedChartDetail ClientStackedChartMainObjectDetail;
	private StackedChartDetail IPCStackedChartMainObjectDetail;
	private StackedChartDetail TimeoutStackedChartMainObjectDetail;
	private StackedChartDetail IOStackedChartMainObjectDetail;
	
	/** The split pane main for Wait Types */
	private JSplitPane CPUSplitPaneMainDetail;
	private JSplitPane LWLockSplitPaneMainDetail;
	private JSplitPane LockSplitPaneMainDetail;
	private JSplitPane BufferPinSplitPaneMainDetail;
	private JSplitPane ActivitySplitPaneMainDetail;
	private JSplitPane ExtensionSplitPaneMainDetail;
	private JSplitPane ClientSplitPaneMainDetail;
	private JSplitPane IPCSplitPaneMainDetail;
	private JSplitPane TimeoutSplitPaneMainDetail;
	private JSplitPane IOSplitPaneMainDetail;
	
	/** The chart chart panel for Wait Types */
	private ChartPanel CPUChartPanel;
	private ChartPanel LWLockChartPanel;
	private ChartPanel LockChartPanel;
	private ChartPanel BufferPinChartPanel;
	private ChartPanel ActivityChartPanel;
	private ChartPanel ExtensionChartPanel;
	private ChartPanel ClientChartPanel;
	private ChartPanel IPCChartPanel;
	private ChartPanel TimeoutChartPanel;
	private ChartPanel IOChartPanel;
	
	/** The top sqls and sessions for Wait Types. */
	private GanttDetails CPUSqlsAndSessions;
	private GanttDetails LWLockSqlsAndSessions;
	private GanttDetails LockSqlsAndSessions;
	private GanttDetails BufferPinSqlsAndSessions;
	private GanttDetails ActivitySqlsAndSessions;
	private GanttDetails ExtensionSqlsAndSessions;
	private GanttDetails ClientSqlsAndSessions;
	private GanttDetails IPCSqlsAndSessions;
	private GanttDetails TimeoutSqlsAndSessions;
	private GanttDetails IOSqlsAndSessions;

	 /** The status bar. */
    private StatusBar statusBar;
	
	/** The max cpu. */
	private double maxCpu;
	
	/** The divider. */
	private int dividerLocation = 290;
	
	/**
	 * Constructor DetailFrame
	 * 
	 * @param mainFrame0
	 * @param database0
	 * @param statusBar0
	 */
	public DetailPanels(JFrame mainFrame0, ASHDatabase database0, StatusBar statusBar0){
		this.mainFrame = mainFrame0;
		this.database = database0;
		this.statusBar = statusBar0;
		this.tabsDetail = new JTabbedPane();
		
		this.CPUStackedChartMainObjectDetail = new StackedChartDetail (this.database, Options.getInstance().getResource("CPULabel.text"));
		this.IOStackedChartMainObjectDetail = new StackedChartDetail (this.database, Options.getInstance().getResource("IOLabel.text"));
		this.LockStackedChartMainObjectDetail = new StackedChartDetail (this.database, Options.getInstance().getResource("LockLabel.text"));
		this.LWLockStackedChartMainObjectDetail = new StackedChartDetail (this.database, Options.getInstance().getResource("LWLockLabel.text"));
		this.BufferPinStackedChartMainObjectDetail = new StackedChartDetail (this.database, Options.getInstance().getResource("BufferPinLabel.text"));
		this.ActivityStackedChartMainObjectDetail = new StackedChartDetail (this.database, Options.getInstance().getResource("ActivityLabel.text"));	
		this.ExtensionStackedChartMainObjectDetail = new StackedChartDetail (this.database, Options.getInstance().getResource("ExtensionLabel.text"));	
		this.ClientStackedChartMainObjectDetail = new StackedChartDetail (this.database, Options.getInstance().getResource("ClientLabel.text"));	
		this.IPCStackedChartMainObjectDetail = new StackedChartDetail (this.database, Options.getInstance().getResource("IPCLabel.text"));
		this.TimeoutStackedChartMainObjectDetail = new StackedChartDetail (this.database, Options.getInstance().getResource("TimeoutLabel.text"));
		
		this.initialize();
	}
	
	/**
	 * Initialize DetailFrame
	 */
	private void initialize() {
		
		this.setLayout(new BorderLayout());
		this.setVisible(true);
		
		this.CPUSplitPaneMainDetail = new JSplitPane();
		this.LWLockSplitPaneMainDetail = new JSplitPane();
		this.LockSplitPaneMainDetail = new JSplitPane();
		this.BufferPinSplitPaneMainDetail = new JSplitPane();
		this.ActivitySplitPaneMainDetail = new JSplitPane();
		this.ExtensionSplitPaneMainDetail = new JSplitPane();
		this.ClientSplitPaneMainDetail = new JSplitPane();
		this.IPCSplitPaneMainDetail = new JSplitPane();
		this.TimeoutSplitPaneMainDetail = new JSplitPane();
		this.IOSplitPaneMainDetail = new JSplitPane();
		
		this.CPUChartPanel = this.CPUStackedChartMainObjectDetail.createChartPanel();
		this.LWLockChartPanel = this.LWLockStackedChartMainObjectDetail.createChartPanel();
		this.LockChartPanel = this.LockStackedChartMainObjectDetail.createChartPanel();
		this.BufferPinChartPanel = this.BufferPinStackedChartMainObjectDetail.createChartPanel();
		this.ActivityChartPanel = this.ActivityStackedChartMainObjectDetail.createChartPanel();
		this.ExtensionChartPanel = this.ExtensionStackedChartMainObjectDetail.createChartPanel();
		this.ClientChartPanel = this.ClientStackedChartMainObjectDetail.createChartPanel();
		this.IPCChartPanel = this.IPCStackedChartMainObjectDetail.createChartPanel();
		this.TimeoutChartPanel = this.TimeoutStackedChartMainObjectDetail.createChartPanel();
		this.IOChartPanel = this.IOStackedChartMainObjectDetail.createChartPanel();
		
		/** Gantt graph */
		this.CPUSqlsAndSessions = new GanttDetails(this.mainFrame, this.database, Options.getInstance().getResource("CPULabel.text"));
		this.LWLockSqlsAndSessions = new GanttDetails(this.mainFrame, this.database, Options.getInstance().getResource("LWLockLabel.text"));
		this.LockSqlsAndSessions = new GanttDetails(this.mainFrame, this.database, Options.getInstance().getResource("LockLabel.text"));
		this.BufferPinSqlsAndSessions = new GanttDetails(this.mainFrame, this.database, Options.getInstance().getResource("BufferPinLabel.text"));
		this.ActivitySqlsAndSessions = new GanttDetails(this.mainFrame, this.database, Options.getInstance().getResource("ActivityLabel.text"));
		this.ExtensionSqlsAndSessions = new GanttDetails(this.mainFrame, this.database, Options.getInstance().getResource("ExtensionLabel.text"));
		this.ClientSqlsAndSessions = new GanttDetails(this.mainFrame, this.database, Options.getInstance().getResource("ClientLabel.text"));
		this.IPCSqlsAndSessions = new GanttDetails(this.mainFrame, this.database, Options.getInstance().getResource("IPCLabel.text"));
		this.TimeoutSqlsAndSessions = new GanttDetails(this.mainFrame, this.database, Options.getInstance().getResource("TimeoutLabel.text"));
		this.IOSqlsAndSessions = new GanttDetails(this.mainFrame, this.database, Options.getInstance().getResource("IOLabel.text"));
		
		this.CPUSplitPaneMainDetail.setOrientation(JSplitPane.VERTICAL_SPLIT);
		this.CPUSplitPaneMainDetail.add(this.CPUChartPanel, "top");
		this.CPUSplitPaneMainDetail.add(this.CPUSqlsAndSessions, "bottom");
		this.CPUSplitPaneMainDetail.setDividerLocation(this.dividerLocation);
		this.CPUSplitPaneMainDetail.setOneTouchExpandable(true);

		this.IOSplitPaneMainDetail.setOrientation(JSplitPane.VERTICAL_SPLIT);
		this.IOSplitPaneMainDetail.add(this.IOChartPanel, "top");
		this.IOSplitPaneMainDetail.add(this.IOSqlsAndSessions, "bottom");
		this.IOSplitPaneMainDetail.setDividerLocation(this.dividerLocation);
		this.IOSplitPaneMainDetail.setOneTouchExpandable(true);

		this.LockSplitPaneMainDetail.setOrientation(JSplitPane.VERTICAL_SPLIT);
		this.LockSplitPaneMainDetail.add(this.LockChartPanel, "top");
		this.LockSplitPaneMainDetail.add(this.LockSqlsAndSessions, "bottom");
		this.LockSplitPaneMainDetail.setDividerLocation(this.dividerLocation);
		this.LockSplitPaneMainDetail.setOneTouchExpandable(true);

		this.LWLockSplitPaneMainDetail.setOrientation(JSplitPane.VERTICAL_SPLIT);
		this.LWLockSplitPaneMainDetail.add(this.LWLockChartPanel, "top");
		this.LWLockSplitPaneMainDetail.add(this.LWLockSqlsAndSessions, "bottom");
		this.LWLockSplitPaneMainDetail.setDividerLocation(this.dividerLocation);
		this.LWLockSplitPaneMainDetail.setOneTouchExpandable(true);
	
		this.BufferPinSplitPaneMainDetail.setOrientation(JSplitPane.VERTICAL_SPLIT);
		this.BufferPinSplitPaneMainDetail.add(this.BufferPinChartPanel, "top");
		this.BufferPinSplitPaneMainDetail.add(this.BufferPinSqlsAndSessions, "bottom");
		this.BufferPinSplitPaneMainDetail.setDividerLocation(this.dividerLocation);
		this.BufferPinSplitPaneMainDetail.setOneTouchExpandable(true);
		
		this.ActivitySplitPaneMainDetail.setOrientation(JSplitPane.VERTICAL_SPLIT);
		this.ActivitySplitPaneMainDetail.add(this.ActivityChartPanel, "top");
		this.ActivitySplitPaneMainDetail.add(this.ActivitySqlsAndSessions, "bottom");
		this.ActivitySplitPaneMainDetail.setDividerLocation(this.dividerLocation);
		this.ActivitySplitPaneMainDetail.setOneTouchExpandable(true);
		
		this.ExtensionSplitPaneMainDetail.setOrientation(JSplitPane.VERTICAL_SPLIT);
		this.ExtensionSplitPaneMainDetail.add(this.ExtensionChartPanel, "top");
		this.ExtensionSplitPaneMainDetail.add(this.ExtensionSqlsAndSessions, "bottom");
		this.ExtensionSplitPaneMainDetail.setDividerLocation(this.dividerLocation);
		this.ExtensionSplitPaneMainDetail.setOneTouchExpandable(true);
		
		this.ClientSplitPaneMainDetail.setOrientation(JSplitPane.VERTICAL_SPLIT);
		this.ClientSplitPaneMainDetail.add(this.ClientChartPanel, "top");
		this.ClientSplitPaneMainDetail.add(this.ClientSqlsAndSessions, "bottom");
		this.ClientSplitPaneMainDetail.setDividerLocation(this.dividerLocation);
		this.ClientSplitPaneMainDetail.setOneTouchExpandable(true);
		
		this.IPCSplitPaneMainDetail.setOrientation(JSplitPane.VERTICAL_SPLIT);
		this.IPCSplitPaneMainDetail.add(this.IPCChartPanel, "top");
		this.IPCSplitPaneMainDetail.add(this.IPCSqlsAndSessions, "bottom");
		this.IPCSplitPaneMainDetail.setDividerLocation(this.dividerLocation);
		this.IPCSplitPaneMainDetail.setOneTouchExpandable(true);
		
		this.TimeoutSplitPaneMainDetail.setOrientation(JSplitPane.VERTICAL_SPLIT);
		this.TimeoutSplitPaneMainDetail.add(this.TimeoutChartPanel, "top");
		this.TimeoutSplitPaneMainDetail.add(this.TimeoutSqlsAndSessions, "bottom");
		this.TimeoutSplitPaneMainDetail.setDividerLocation(this.dividerLocation);
		this.TimeoutSplitPaneMainDetail.setOneTouchExpandable(true);
		
		this.tabsDetail.add(this.CPUSplitPaneMainDetail, Options.getInstance().getResource("CPULabel.text"));
		this.tabsDetail.add(this.IOSplitPaneMainDetail, Options.getInstance().getResource("IOLabel.text"));
		this.tabsDetail.add(this.LockSplitPaneMainDetail, Options.getInstance().getResource("LockLabel.text"));
		this.tabsDetail.add(this.LWLockSplitPaneMainDetail, Options.getInstance().getResource("LWLockLabel.text"));
		this.tabsDetail.add(this.BufferPinSplitPaneMainDetail, Options.getInstance().getResource("BufferPinLabel.text"));
		this.tabsDetail.add(this.ActivitySplitPaneMainDetail, Options.getInstance().getResource("ActivityLabel.text"));
		this.tabsDetail.add(this.ExtensionSplitPaneMainDetail, Options.getInstance().getResource("ExtensionLabel.text"));
		this.tabsDetail.add(this.ClientSplitPaneMainDetail, Options.getInstance().getResource("ClientLabel.text"));
		this.tabsDetail.add(this.IPCSplitPaneMainDetail, Options.getInstance().getResource("IPCLabel.text"));
		this.tabsDetail.add(this.TimeoutSplitPaneMainDetail, Options.getInstance().getResource("TimeoutLabel.text"));
		
		this.CPUChartPanel.addListenerReleaseMouse(this.CPUSqlsAndSessions);
		this.LWLockChartPanel.addListenerReleaseMouse(this.LWLockSqlsAndSessions);
		this.LockChartPanel.addListenerReleaseMouse(this.LockSqlsAndSessions);
		this.BufferPinChartPanel.addListenerReleaseMouse(this.BufferPinSqlsAndSessions);
		this.ActivityChartPanel.addListenerReleaseMouse(this.ActivitySqlsAndSessions);
		this.ExtensionChartPanel.addListenerReleaseMouse(this.ExtensionSqlsAndSessions);
		this.ClientChartPanel.addListenerReleaseMouse(this.ClientSqlsAndSessions);
		this.IPCChartPanel.addListenerReleaseMouse(this.IPCSqlsAndSessions);
		this.TimeoutChartPanel.addListenerReleaseMouse(this.TimeoutSqlsAndSessions);
		this.IOChartPanel.addListenerReleaseMouse(this.IOSqlsAndSessions);
		
		this.CPUChartPanel.addListenerReleaseMouse(this.statusBar);
		this.LWLockChartPanel.addListenerReleaseMouse(this.statusBar);
		this.LockChartPanel.addListenerReleaseMouse(this.statusBar);
		this.BufferPinChartPanel.addListenerReleaseMouse(this.statusBar);
		this.ActivityChartPanel.addListenerReleaseMouse(this.statusBar);
		this.ExtensionChartPanel.addListenerReleaseMouse(this.statusBar);
		this.ClientChartPanel.addListenerReleaseMouse(this.statusBar);
		this.IPCChartPanel.addListenerReleaseMouse(this.statusBar);
		this.TimeoutChartPanel.addListenerReleaseMouse(this.statusBar);
		this.IOChartPanel.addListenerReleaseMouse(this.statusBar);
	
		ChangeListener changeListener = new ChangeListener() {
		      public void stateChanged(ChangeEvent changeEvent) {
		        JTabbedPane sourceTabbedPane = (JTabbedPane) changeEvent.getSource();
		        int index = sourceTabbedPane.getSelectedIndex();
		        statusBar.updateLabelStringDetail(
		        		sourceTabbedPane.getTitleAt(index));
		      }
		    };
		 tabsDetail.addChangeListener(changeListener);
		
		this.add(tabsDetail, BorderLayout.CENTER);
	}
	
	/**
	 * Load data to dataset
	 */
	public void loadDataToDataSet(){
		this.database.saveStackedXYAreaChartDetail(this.CPUStackedChartMainObjectDetail, Options.getInstance().getResource("CPULabel.text"));
		this.database.saveStackedXYAreaChartDetail(this.IOStackedChartMainObjectDetail, Options.getInstance().getResource("IOLabel.text"));
		this.database.saveStackedXYAreaChartDetail(this.LockStackedChartMainObjectDetail, Options.getInstance().getResource("LockLabel.text"));
		this.database.saveStackedXYAreaChartDetail(this.LWLockStackedChartMainObjectDetail, Options.getInstance().getResource("LWLockLabel.text"));
		this.database.saveStackedXYAreaChartDetail(this.BufferPinStackedChartMainObjectDetail, Options.getInstance().getResource("BufferPinLabel.text"));
		this.database.saveStackedXYAreaChartDetail(this.ActivityStackedChartMainObjectDetail, Options.getInstance().getResource("ActivityLabel.text"));	
		this.database.saveStackedXYAreaChartDetail(this.ExtensionStackedChartMainObjectDetail, Options.getInstance().getResource("ExtensionLabel.text"));
		this.database.saveStackedXYAreaChartDetail(this.ClientStackedChartMainObjectDetail, Options.getInstance().getResource("ClientLabel.text"));
		this.database.saveStackedXYAreaChartDetail(this.IPCStackedChartMainObjectDetail, Options.getInstance().getResource("IPCLabel.text"));
		this.database.saveStackedXYAreaChartDetail(this.TimeoutStackedChartMainObjectDetail, Options.getInstance().getResource("TimeoutLabel.text"));
		
		this.database.initialLoadingDataToChartPanelDataSetDetail();
		
		this.CPUStackedChartMainObjectDetail.setTitle();
		this.IOStackedChartMainObjectDetail.setTitle();
		this.LockStackedChartMainObjectDetail.setTitle();
		this.LWLockStackedChartMainObjectDetail.setTitle();
		this.BufferPinStackedChartMainObjectDetail.setTitle();
		this.ActivityStackedChartMainObjectDetail.setTitle();
		this.ExtensionStackedChartMainObjectDetail.setTitle();
		this.ClientStackedChartMainObjectDetail.setTitle();
		this.IPCStackedChartMainObjectDetail.setTitle();
		this.TimeoutStackedChartMainObjectDetail.setTitle();

		this.setThresholdMaxCpu();
	}
	
	/**
	 * Set the threshold max cpu (detail).
	 * @param maxCpu
	 */
	public void setThresholdMaxCpu(Double maxCpu){
		this.maxCpu = maxCpu;
	}
	
	/**
     * Set upper bound of range axis (detail)
     * @param bound
     */
    public void setUpperBoundOfRangeAxis(double bound){
	    	this.CPUStackedChartMainObjectDetail.setUpperBoundOfRangeAxis(bound);
	    	this.LWLockStackedChartMainObjectDetail.setUpperBoundOfRangeAxis(bound);
		this.LockStackedChartMainObjectDetail.setUpperBoundOfRangeAxis(bound);
		this.BufferPinStackedChartMainObjectDetail.setUpperBoundOfRangeAxis(bound);
		this.ActivityStackedChartMainObjectDetail.setUpperBoundOfRangeAxis(bound);
		this.ExtensionStackedChartMainObjectDetail.setUpperBoundOfRangeAxis(bound);
		this.ClientStackedChartMainObjectDetail.setUpperBoundOfRangeAxis(bound);
		this.IPCStackedChartMainObjectDetail.setUpperBoundOfRangeAxis(bound);
		this.TimeoutStackedChartMainObjectDetail.setUpperBoundOfRangeAxis(bound);
		this.IOStackedChartMainObjectDetail.setUpperBoundOfRangeAxis(bound);
    }
	
    /**
     * Get thumbnail details charts
     * 
     * @return
     */
    public JPanel getThumbnailDetailPanel(){
    	JPanel mainPanel = new JPanel(new GridLayout(3, 5));
    	
    	BufferedImage thumbCPU = this.CPUStackedChartMainObjectDetail.createBufferedImage(120, 80, 360, 240,null);
    	ImageIcon imageCPU = new ImageIcon(thumbCPU);
        mainPanel.add(new JButton(imageCPU));

    	BufferedImage thumbIO = this.IOStackedChartMainObjectDetail.createBufferedImage(120, 80, 360, 240,null);
    	ImageIcon imageIO = new ImageIcon(thumbIO);
        mainPanel.add(new JButton(imageIO));

    	BufferedImage thumbLock = this.LockStackedChartMainObjectDetail.createBufferedImage(120, 80, 360, 240,null);
    	ImageIcon imageLock = new ImageIcon(thumbLock);
        mainPanel.add(new JButton(imageLock));

    	BufferedImage thumbLWLock = this.LWLockStackedChartMainObjectDetail.createBufferedImage(120, 80, 360, 240,null);
    	ImageIcon imageLWLock = new ImageIcon(thumbLWLock);
        mainPanel.add(new JButton(imageLWLock));
       
    	BufferedImage thumbBufferPin = this.BufferPinStackedChartMainObjectDetail.createBufferedImage(120, 80, 360, 240,null);
    	ImageIcon imageBufferPin = new ImageIcon(thumbBufferPin);
        mainPanel.add(new JButton(imageBufferPin));
        
    	BufferedImage thumbActivity = this.ActivityStackedChartMainObjectDetail.createBufferedImage(120, 80, 360, 240,null);
    	ImageIcon imageActivity = new ImageIcon(thumbActivity);
        mainPanel.add(new JButton(imageActivity));
        
    	BufferedImage thumbExtension = this.ExtensionStackedChartMainObjectDetail.createBufferedImage(120, 80, 360, 240,null);
    	ImageIcon imageExtension = new ImageIcon(thumbExtension);
        mainPanel.add(new JButton(imageExtension));
        
    	BufferedImage thumbClient = this.ClientStackedChartMainObjectDetail.createBufferedImage(120, 80, 360, 240,null);
    	ImageIcon imageClient = new ImageIcon(thumbClient);
        mainPanel.add(new JButton(imageClient));
        
    	BufferedImage thumbIPC = this.IPCStackedChartMainObjectDetail.createBufferedImage(120, 80, 360, 240,null);
    	ImageIcon imageIPC = new ImageIcon(thumbIPC);
        mainPanel.add(new JButton(imageIPC));
        
    	BufferedImage thumbTimeout = this.TimeoutStackedChartMainObjectDetail.createBufferedImage(120, 80, 360, 240,null);
    	ImageIcon imageTimeout = new ImageIcon(thumbTimeout);
        mainPanel.add(new JButton(imageTimeout));
        
        mainPanel.add(new JPanel());
        
    	return mainPanel;
    }
    
	 /**
     * Sets the threshold max cpu for all charts.
     */
    private void setThresholdMaxCpu(){  	
	    	this.CPUStackedChartMainObjectDetail.setThresholdMaxCpu(this.maxCpu);
	    	this.LWLockStackedChartMainObjectDetail.setThresholdMaxCpu(this.maxCpu);
		this.LockStackedChartMainObjectDetail.setThresholdMaxCpu(this.maxCpu);
		this.BufferPinStackedChartMainObjectDetail.setThresholdMaxCpu(this.maxCpu);
		this.ActivityStackedChartMainObjectDetail.setThresholdMaxCpu(this.maxCpu);
		this.ExtensionStackedChartMainObjectDetail.setThresholdMaxCpu(this.maxCpu);
		this.ClientStackedChartMainObjectDetail.setThresholdMaxCpu(this.maxCpu);
		this.IPCStackedChartMainObjectDetail.setThresholdMaxCpu(this.maxCpu);
		this.TimeoutStackedChartMainObjectDetail.setThresholdMaxCpu(this.maxCpu);
		this.IOStackedChartMainObjectDetail.setThresholdMaxCpu(this.maxCpu);
      }
    
   /**
	* Update xAxis label.
	* @param time the time
	*/
   public void updatexAxisLabel(double time){  	  
	   this.CPUStackedChartMainObjectDetail.updatexAxisLabel(time);
	   this.LWLockStackedChartMainObjectDetail.updatexAxisLabel(time);
		this.LockStackedChartMainObjectDetail.updatexAxisLabel(time);
		this.BufferPinStackedChartMainObjectDetail.updatexAxisLabel(time);
		this.ActivityStackedChartMainObjectDetail.updatexAxisLabel(time);
		this.ExtensionStackedChartMainObjectDetail.updatexAxisLabel(time);
		this.ClientStackedChartMainObjectDetail.updatexAxisLabel(time);
		this.IPCStackedChartMainObjectDetail.updatexAxisLabel(time);
		this.TimeoutStackedChartMainObjectDetail.updatexAxisLabel(time);
		this.IOStackedChartMainObjectDetail.updatexAxisLabel(time);
   }
    
   /**
    * Set top sql value
    * @param value
    */
   public void setTopSqlsSqlText(int value){
	   this.CPUSqlsAndSessions.setTopSqlsSqlText(value);
	   this.LWLockSqlsAndSessions.setTopSqlsSqlText(value);
	   this.LockSqlsAndSessions.setTopSqlsSqlText(value);
	   this.BufferPinSqlsAndSessions.setTopSqlsSqlText(value);
	   this.ActivitySqlsAndSessions.setTopSqlsSqlText(value);
	   this.ExtensionSqlsAndSessions.setTopSqlsSqlText(value);
	   this.ClientSqlsAndSessions.setTopSqlsSqlText(value);
	   this.IPCSqlsAndSessions.setTopSqlsSqlText(value);
	   this.TimeoutSqlsAndSessions.setTopSqlsSqlText(value);
	   this.IOSqlsAndSessions.setTopSqlsSqlText(value);
   }
   
   /**
    * Select top sql plan
    * 
    * @param value
    */
   public void setSelectSqlPlan(boolean isSelect){
	   this.CPUSqlsAndSessions.setSelectSqlPlan(isSelect);
	   this.LWLockSqlsAndSessions.setSelectSqlPlan(isSelect);
	   this.LockSqlsAndSessions.setSelectSqlPlan(isSelect);
	   this.BufferPinSqlsAndSessions.setSelectSqlPlan(isSelect);
	   this.ActivitySqlsAndSessions.setSelectSqlPlan(isSelect);
	   this.ExtensionSqlsAndSessions.setSelectSqlPlan(isSelect);
	   this.ClientSqlsAndSessions.setSelectSqlPlan(isSelect);
	   this.IPCSqlsAndSessions.setSelectSqlPlan(isSelect);
	   this.TimeoutSqlsAndSessions.setSelectSqlPlan(isSelect);
	   this.IOSqlsAndSessions.setSelectSqlPlan(isSelect);
   }
   
   /**
    * Get name of selected tab
    * @return name
    */
   public String getCurrentTabName(){
	   return this.tabsDetail.getTitleAt(this.tabsDetail.getSelectedIndex());
   }
}
