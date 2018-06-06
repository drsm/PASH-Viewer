/*
 *-------------------
 * The DetailsPanelH.java is part of ASH Viewer
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
package org.ash.history.detail;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

import javax.swing.AbstractButton;
import javax.swing.ButtonGroup;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JRadioButton;
import javax.swing.JSplitPane;
import javax.swing.JToolBar;
import javax.swing.border.EtchedBorder;

import org.ash.history.detail.GanttDetailsH;
import org.ash.history.detail.StackedChartDetail;
import org.ash.history.ASHDatabaseH;
import org.ash.util.Options;
import org.ash.util.ProgressBarUtil;
import org.jfree.chart.ChartPanel;

public class DetailsPanelH extends JPanel implements ActionListener{

	/** The database. */
	private ASHDatabaseH database;
			
	/** The main panel. */
	private JPanel mainPanel;
	
	/** The button panel. */
	private JToolBar buttonPanel;
	   
	/** The radio buttons for Wait Types */
	private JRadioButton CPURadioButton = new JRadioButton();
	private JRadioButton LWLockRadioButton = new JRadioButton();
	private JRadioButton LockRadioButton = new JRadioButton();
	private JRadioButton BufferPinRadioButton = new JRadioButton();
	private JRadioButton ActivityRadioButton = new JRadioButton();
	private JRadioButton ExtensionRadioButton = new JRadioButton();
	private JRadioButton ClientRadioButton = new JRadioButton();
	private JRadioButton IPCRadioButton = new JRadioButton();
	private JRadioButton TimeoutRadioButton = new JRadioButton();
	private JRadioButton IORadioButton = new JRadioButton();
	
	/** Button group for details radio buttons */
	private ButtonGroup buttonGroup = new ButtonGroup();
	
	/** The max cpu. */
	private double maxCpu;
	
	/** The begin time. */
	private double beginTime = 0.0;
	 
	/** The end time. */
	private double endTime = 0.0;
	
	/**
	 *  Constructor DetailFrame
	 *  
	 * @param database
	 * @param beginTime
	 * @param endTime
	 * @param maxCpu
	 */
	public DetailsPanelH(ASHDatabaseH database, double beginTime, double endTime, double maxCpu){
		this.database = database;
		this.beginTime = beginTime;
		this.endTime = endTime;
		this.maxCpu = maxCpu;
		this.initialize();
	}
	
	/**
	 * Initialize DetailFrame
	 */
	private void initialize() {
		
		this.setLayout(new BorderLayout());
		JSplitPane splitPaneMainDetail = new JSplitPane();
		
		this.CPURadioButton.setText(Options.getInstance().getResource("CPULabel.text"));
		this.CPURadioButton.addItemListener(new SelectItemListenerRadioButton());
		this.setFont(this.CPURadioButton);

		this.LWLockRadioButton.setText(Options.getInstance().getResource("LWLockLabel.text"));
		this.LWLockRadioButton.addItemListener(new SelectItemListenerRadioButton());
		this.setFont(this.LWLockRadioButton);

		this.LockRadioButton.setText(Options.getInstance().getResource("LockLabel.text"));
		this.LockRadioButton.addItemListener(new SelectItemListenerRadioButton());
		this.setFont(this.LockRadioButton);

		this.BufferPinRadioButton.setText(Options.getInstance().getResource("BufferPinLabel.text"));
		this.BufferPinRadioButton.addItemListener(new SelectItemListenerRadioButton());
		this.setFont(this.BufferPinRadioButton);

		this.ActivityRadioButton.setText(Options.getInstance().getResource("ActivityLabel.text"));
		this.ActivityRadioButton.addItemListener(new SelectItemListenerRadioButton());
		this.setFont(this.ActivityRadioButton);

		this.ExtensionRadioButton.setText(Options.getInstance().getResource("ExtensionLabel.text"));
		this.ExtensionRadioButton.addItemListener(new SelectItemListenerRadioButton());	
		this.setFont(this.ExtensionRadioButton);

		this.ClientRadioButton.setText(Options.getInstance().getResource("ClientLabel.text"));
		this.ClientRadioButton.addItemListener(new SelectItemListenerRadioButton());
		this.setFont(this.ClientRadioButton);

		this.IPCRadioButton.setText(Options.getInstance().getResource("IPCLabel.text"));
		this.IPCRadioButton.addItemListener(new SelectItemListenerRadioButton());		
		this.setFont(this.IPCRadioButton);

		this.TimeoutRadioButton.setText(Options.getInstance().getResource("TimeoutLabel.text"));
		this.TimeoutRadioButton.addItemListener(new SelectItemListenerRadioButton());	
		this.setFont(this.TimeoutRadioButton);

		this.IORadioButton.setText(Options.getInstance().getResource("IOLabel.text"));
		this.IORadioButton.addItemListener(new SelectItemListenerRadioButton());	
		this.setFont(this.IORadioButton);

		this.buttonGroup.add(CPURadioButton);
		this.buttonGroup.add(IORadioButton);
		this.buttonGroup.add(LockRadioButton);
		this.buttonGroup.add(LWLockRadioButton);
		this.buttonGroup.add(BufferPinRadioButton);
		this.buttonGroup.add(ActivityRadioButton);
		this.buttonGroup.add(ExtensionRadioButton);
		this.buttonGroup.add(ClientRadioButton);
		this.buttonGroup.add(IPCRadioButton);
		this.buttonGroup.add(TimeoutRadioButton);

		
		/** Button panel fot buttons */
		this.buttonPanel = new JToolBar("PanelButton");
		this.buttonPanel.setFloatable(false);
		this.buttonPanel.setBorder(new EtchedBorder());
		
		this.buttonPanel.add(this.CPURadioButton);
		this.buttonPanel.add(this.IORadioButton);
		this.buttonPanel.add(this.LockRadioButton);
		this.buttonPanel.add(this.LWLockRadioButton);
		this.buttonPanel.add(this.BufferPinRadioButton);
		this.buttonPanel.add(this.ActivityRadioButton);
		this.buttonPanel.add(this.ExtensionRadioButton);
		this.buttonPanel.add(this.ClientRadioButton);
		this.buttonPanel.add(this.IPCRadioButton);
		this.buttonPanel.add(this.TimeoutRadioButton);
						
		splitPaneMainDetail.setOrientation(JSplitPane.VERTICAL_SPLIT);
		splitPaneMainDetail.add(new JPanel(), "top");
		splitPaneMainDetail.add(new JPanel(), "bottom");
		splitPaneMainDetail.setDividerLocation(230);
		splitPaneMainDetail.setOneTouchExpandable(true);
		
		this.mainPanel = new JPanel();
		this.mainPanel.setLayout(new BorderLayout());
		this.mainPanel.setVisible(true);
		this.mainPanel.add(splitPaneMainDetail, BorderLayout.CENTER);
				
		this.add(this.buttonPanel, BorderLayout.NORTH);
		this.add(this.mainPanel, BorderLayout.CENTER);		
	}
	
	/**
	 * Set Font for JRadioButton. 
	 * 
	 * @param radioButton
	 */
	private void setFont(JRadioButton radioButton){
		radioButton.setFont(new Font("SansSerif", Font.PLAIN, 9));
	}
	
	/**
	 * Set the threshold max cpu.
	 * @param maxCpu
	 */
	public void setThresholdMaxCpu(Double maxCpu){
		this.maxCpu = maxCpu;
	}
	
	
	/**
	 * Item listener for cpu used and events radio buttons
	 *
	 */
	class SelectItemListenerRadioButton implements ItemListener{
		public void itemStateChanged(ItemEvent e){
			//get object
			AbstractButton sel = (AbstractButton)e.getItemSelectable();
			//checkbox select or not
			if(e.getStateChange() == ItemEvent.SELECTED){
				if (sel.getText().equals(Options.getInstance().getResource("LWLockLabel.text"))){
					addChartPanel(Options.getInstance().getResource("LWLockLabel.text"));
				}
				else if (sel.getText().equals(Options.getInstance().getResource("CPULabel.text"))){
					addChartPanel(Options.getInstance().getResource("CPULabel.text"));
				}
				else if (sel.getText().equals(Options.getInstance().getResource("LockLabel.text"))){
					addChartPanel(Options.getInstance().getResource("LockLabel.text"));
				}
				else if (sel.getText().equals(Options.getInstance().getResource("BufferPinLabel.text"))){
					addChartPanel(Options.getInstance().getResource("BufferPinLabel.text"));
				}
				else if (sel.getText().equals(Options.getInstance().getResource("ActivityLabel.text"))){
					addChartPanel(Options.getInstance().getResource("ActivityLabel.text"));
				}
				else if (sel.getText().equals(Options.getInstance().getResource("ExtensionLabel.text"))){
					addChartPanel(Options.getInstance().getResource("ExtensionLabel.text"));
				}
				else if (sel.getText().equals(Options.getInstance().getResource("ClientLabel.text"))){
					addChartPanel(Options.getInstance().getResource("ClientLabel.text"));
				}
				else if (sel.getText().equals(Options.getInstance().getResource("IPCLabel.text"))){
					addChartPanel(Options.getInstance().getResource("IPCLabel.text"));
				}
				else if (sel.getText().equals(Options.getInstance().getResource("TimeoutLabel.text"))){
					addChartPanel(Options.getInstance().getResource("TimeoutLabel.text"));
				}
				else if (sel.getText().equals(Options.getInstance().getResource("IOLabel.text"))){
					addChartPanel(Options.getInstance().getResource("IOLabel.text"));
				}
			}
		}
	}
	
	public void actionPerformed(ActionEvent e) {
		// TODO Auto-generated method stub
	}
	
	/**
	 * Add ChartPanel to mainPanel
	 * @param waitClass
	 */
	private void addChartPanel(final String waitClass){
		
		this.mainPanel.removeAll();		
        JPanel panel = createProgressBar("Loading, please wait...");
        this.mainPanel.add(panel);
        this.validate();
		
        Thread t = new Thread() {
            @Override
			public void run()
            {
            		// delay
                     try
                     {
                         Thread.sleep(50L);
                     }
                     catch(InterruptedException e) {
					e.printStackTrace();
				}
					addChartPanelT(waitClass);
				
			}
		};
		t.start();
		
	}
	
	/**
	 * Add ChartPanel to mainPanel
	 * @param waitClass
	 */
	private void addChartPanelT(String waitClass){
		
		JSplitPane splitPaneMainDetail = new JSplitPane();
		
		StackedChartDetail stackedChartMainObjectDetail = 
			new StackedChartDetail(database,waitClass);
		
		ChartPanel chartDetailPanel = stackedChartMainObjectDetail.createChartPanel();
		stackedChartMainObjectDetail.setThresholdMaxCpu(this.maxCpu);
		
		GanttDetailsH sqlsAndSessions = new GanttDetailsH(database, waitClass);
		chartDetailPanel.addListenerReleaseMouse(sqlsAndSessions);
					
		
		database.clearStackedXYAreaChartDetail();
		database.saveStackedXYAreaChartDetail(stackedChartMainObjectDetail, waitClass);
		database.initialLoadingDataToChartPanelDataSetDetail(waitClass,this.beginTime,this.endTime);
		
		stackedChartMainObjectDetail.setTitle();
		
		this.mainPanel.removeAll();
		
		splitPaneMainDetail.setOrientation(JSplitPane.VERTICAL_SPLIT);
		splitPaneMainDetail.add(chartDetailPanel,"top");
		splitPaneMainDetail.add(sqlsAndSessions, "bottom");
		splitPaneMainDetail.setDividerLocation(215);
		splitPaneMainDetail.setOneTouchExpandable(true);
		
		this.mainPanel.add(splitPaneMainDetail, BorderLayout.CENTER);
		this.validate();
		this.repaint();
	}
	
	
	/**
	 * Creates the progress bar.
	 * 
	 * @param msg the msg
	 * 
	 * @return the j panel
	 */
	private JPanel createProgressBar(String msg) {
		JProgressBar progress = ProgressBarUtil.createJProgressBar(msg);
		progress.setPreferredSize(new Dimension(250, 30));
		JPanel panel = new JPanel();
		panel.add(progress);
		return panel;
	}

}
