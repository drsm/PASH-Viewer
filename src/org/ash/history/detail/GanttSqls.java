/*
 *-------------------
 * The GanttSqls.java is part of ASH Viewer
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

import com.egantt.model.drawing.DrawingState;
import com.egantt.model.drawing.part.ListDrawingPart;
import ext.egantt.drawing.module.BasicPainterModule;
import ext.egantt.model.drawing.state.BasicDrawingState;
import ext.egantt.swing.GanttDrawingPartHelper;
import org.ash.history.ASHDatabaseH;
import org.ash.util.Options;
import org.ash.util.Utils;

import java.util.*;
import java.util.Map.Entry;

public class GanttSqls {

	/** The database. */
	private ASHDatabaseH database;
	
	/** The prev percent. */
	private long percentPrev = 0;
	
	/** The SUM var. */
	private String SUM = "SUM";
	
	/** The COUNT var. */
	private String COUNT = "COUNT";
	
	/** The UNKNOWN var. */
	private String UNKNOWN = "UNKNOWN";

	/** The SQL_TYPE. */
	private String SQL_TYPE = "SQL_TYPE";
	
	/** The scale toggle: 
	 * < 30  => 2 
	 * 30-70  => 1
	 * > 70 	=> 0 
	 * */
	private int scaleToggle = 0;
	
	/** The scale. */
	private double scale = 0.8;
	
	/** The detail for top sql, default 10, max - 50, minimum - 0 values*/
	private int topSqlsSqlText = 10;
	
	/** The TEXT_PAINTER. */
	final String TEXT_PAINTER = "MyTextPainter";
	
	/**
	 * Constructor Gantt for sqls
	 *
	 * @param database0 the database0
	 */
	public GanttSqls(ASHDatabaseH database0){
		this.database = database0;
	}
	
	/**
	 * Load data to sessions gantt.
	 * 
	 * @return SESSIONID, USERNAME, PROGRAM
	 */
	public Object[][] getDataToSqlsGantt(Map<Integer,String> arraySqlIdText50SQLTextTab){
		return this.loadDataToSqlsGanttPr(arraySqlIdText50SQLTextTab);
	}

	/**
	 * Load data to sqls gantt.
	 * 
	 * @return the object[][]
	 */
	private Object[][] loadDataToSqlsGanttPr(Map<Integer,String> arraySqlIdText50SQLTextTab){
		
		int i = 0;		
		int ii = 0;
		int sizeGanttTable = 100;
		int sizeMainSqls = database.getSqlsTempDetail().getMainSqls().size();
		StringBuilder clipBoardContent = new StringBuilder();
		Object[][] data = new Object[Math.min(sizeGanttTable, sizeMainSqls)][3];
		
		final GanttDrawingPartHelper partHelper = new GanttDrawingPartHelper();
		
		double countOfSqls = database.getSqlsTempDetail().getCountSql();
		double sumOfRange = database.getSqlsTempDetail().get_sum();
		
		// Desc sorting
		HashMap<String, HashMap<String, Object>> sortedSessionMap =
			Utils.sortHashMapByValues(database.getSqlsTempDetail().getMainSqls(),COUNT);
		
		Map<String,String> arraySqlIdType50 = new HashMap<String, String>();
		Map<String,String> arraySqlIdText50 = new HashMap<String, String>();
		
		for (Entry<String, HashMap<String, Object>> me : sortedSessionMap.entrySet()) {	
			
			data[i][0] = createDrawingState(partHelper,
					me,countOfSqls,sumOfRange);
			
			data[i][1] = me.getKey();
			//data[i][2] = UNKNOWN;
			// double value = Double.parseDouble((String) me.getValue().get(SQL_TYPE));
			double value = 0.0;
			data[i][2] = Options.getInstance().getResource(
					Options.getInstance().getResource(String.valueOf((int) value)));

			/** Load sqlid */
			arraySqlIdType50.put(me.getKey(),
						database.getSqlType(me.getKey()));
			arraySqlIdText50.put(me.getKey(),
						database.getSqlText(me.getKey()));
			
			/** Exit when rows > 100 */
			if (i+1==Math.min(sizeGanttTable, sizeMainSqls)){
				break;
			}
			i++;
			ii++;
		}
		
		/** Set CommandType, SqlText for sqlid */
		ii = 0;
		for (Entry<String, HashMap<String, Object>> me : sortedSessionMap.entrySet()) {
						
			data[ii][1] = createDrawingStateSqlId(partHelper,me,arraySqlIdText50);
			
			if (!arraySqlIdType50.get(me.getKey()).toString().equals("")){
				data[ii][2] = arraySqlIdType50.get(me.getKey());
			}
			
			/** Save clipboard content */
			clipBoardContent.append(
					me.getKey()+":::"+ arraySqlIdText50.get(me.getKey()).trim()+"\n");
			
			/** Save arraySqlIdText50 for SQL Text tab*/
			// if (arraySqlIdType50.get(me.getKey()) != "UNKNOWN") { arraySqlIdText50SQLTextTab.put(ii, me.getKey()); }
			arraySqlIdText50SQLTextTab.put(ii, me.getKey());
			
			/** Exit when rows > 500 */
			if (ii+1==Math.min(sizeGanttTable, sizeMainSqls)){
				break;
			}
			
			ii++;
		}
		
		/** Set clipboard content */
		Utils.setClipBoardContent(clipBoardContent.toString());
		
		percentPrev = 0;
		
		return data;
	}
	
	/**
	 * Creates the drawing state for Sqls and Sessions.
	 *
	 * @param helper the helper
	 * @param me the me
	 * @param countOfSqls the sum of range
	 * 
	 * @return the drawing state
	 */
		private DrawingState createDrawingState( GanttDrawingPartHelper helper,
												 Entry<String, HashMap<String,Object>> me,
												 double countOfSqls,
												 double sumOfRange ) {
					
			BasicDrawingState state = helper.createDrawingState();
			ListDrawingPart part = helper.createDrawingPart(false);
			ListDrawingPart textLayer = helper.createDrawingPart(true);	
		
			double countPerSqlID = (Double)me.getValue().get(COUNT);
			double percent = Utils.round(countPerSqlID/countOfSqls*100,2);
			
			/* < 30  => 2 
			 * 30-70  => 1
			 * > 70    => 0 */
			if (percentPrev == 0){
				if (percent > 70){
					scaleToggle = 0;
				} else if (percent <70 && percent >30){
					scaleToggle = 1;
				} else if (percent < 30){
					scaleToggle = 2;
				}
			}
			
			if (percent<0.6){
				 // Show only percent
			    String localizedText = ""+percent+"%";
			    helper.createActivityEntry(new StringBuffer(localizedText), 
			    		new Date(0), new Date(100), 
			    		BasicPainterModule.BASIC_STRING_PAINTER, TEXT_PAINTER, textLayer);
			    
				state.addDrawingPart(part);
				state.addDrawingPart(textLayer);
			return state;
	    	}
			
				// Save to local map, sort by values 
				HashMap tempKeyMap = new HashMap<String,Double>();
			
				Iterator iterEvent = this.database.getSqlsTempDetail().getEventList().iterator();
				while (iterEvent.hasNext()) {
					String eventName = (String) iterEvent.next();
					Double eventValue = (Double)me.getValue().get(eventName);
					if (eventValue != null){
						tempKeyMap.put(eventName, me.getValue().get(eventName));
					}
				}
						
				HashMap sortedByKeyMap = new HashMap<String,Double>();
				
				//Sort values on desc order
				sortedByKeyMap = Utils.sortHashMapByValuesDesc(tempKeyMap);			
				
				// Calculate sum of event by SqlId
				Set keySetsorted = sortedByKeyMap.keySet();
				
				long start = 0;
				scale = Utils.getScale(scaleToggle, percent);
				
				// Create activites for row on gantt
				Set keySetsorted1 = sortedByKeyMap.keySet();
			     Iterator jj = keySetsorted.iterator();
			      while (jj.hasNext()) {
			    	
			    	String key = (String) jj.next();
					double value = (Double)sortedByKeyMap.get(key);	

					// Show only not zero activites.
					if (value != 0 ){
						
						double currentGroupPercentNotScale = (value/countPerSqlID)*percent;
						double currentGroupPercent = currentGroupPercentNotScale*scale;
						
						if (currentGroupPercent<1.0 && currentGroupPercent>=0.6){
							currentGroupPercent = Utils.round(currentGroupPercent,0);
						}
						
						long currGroupPercentL = (long)Utils.round(currentGroupPercent,0);	
						
						// Set tooltip
						final StringBuffer o = new StringBuffer();
						{
							o.append("<HTML>");
							o.append("<b>" + key + " " +
									Utils.round(currentGroupPercentNotScale,2) + "%" + "</b>");
							o.append("</HTML>");		
						}
						
						// Exit when prev. egantt < than current egantt graph
						if (percentPrev != 0 && 
								(start + currGroupPercentL) > percentPrev ){
							currGroupPercentL = percentPrev - start;
							helper.createActivityEntry(
									o,
									new Date(start), 
									new Date(start+currGroupPercentL),
									key,//getGradientContext(key), 
									part);
							 start = start + currGroupPercentL;
							 break;
						}
						
						// If row only one
						if (currentGroupPercent == 100){
						helper.createActivityEntry(
								o,
								new Date(start), 
								new Date(currGroupPercentL),
								key,//getGradientContext(key), 
								part);
						} else {
						 helper.createActivityEntry(
								o,
								new Date(start), 
								new Date(start+currGroupPercentL),
								key,//getGradientContext(key), 
								part);
						 start = start + currGroupPercentL;
						}
					}
				}
			      
			    // Show percent
			    String localizedText = ""+percent+"%";
			    helper.createActivityEntry(new StringBuffer(localizedText), 
			    		new Date(start), new Date(100),
			    		BasicPainterModule.BASIC_STRING_PAINTER, TEXT_PAINTER, textLayer);
			 
			    state.addDrawingPart(part);
				state.addDrawingPart(textLayer);

				percentPrev = start;
				
			return state;
			
		}
	
	/**
	 * Creates the drawing state for SqlId.
	 * 
	 * @param helper  the helper
	 * @param me  the me
	 * @param arraySqlIdText50
	 * @return the drawing state
	 */
	private DrawingState createDrawingStateSqlId(GanttDrawingPartHelper helper,
			Entry<String, HashMap<String, Object>> me,
			Map<String, String> arraySqlIdText50) {

		BasicDrawingState state = helper.createDrawingState();
		ListDrawingPart part = helper.createDrawingPart(false);
		ListDrawingPart textLayer = helper.createDrawingPart(true);

		String key = "";
		String value = "";

		if (!arraySqlIdText50.get(me.getKey()).toString().equals("")) {
			key = me.getKey().trim();
			value = arraySqlIdText50.get(me.getKey()).trim();
		} else {
			key = me.getKey().trim();
			value = "No data";
		}

		// Show sqlid
		helper.createActivityEntry(new StringBuffer(key), new Date(5),
				new Date(95), BasicPainterModule.BASIC_STRING_PAINTER,
				TEXT_PAINTER, textLayer);

		// Set tooltip (sql_text)
		final StringBuffer o = new StringBuffer();
		o.append(Utils.formatSqlQueryShort(value));
		
		helper.createActivityEntry(o, new Date(0), new Date(100),
				BasicPainterModule.BASIC_STRING_PAINTER, part);

		state.addDrawingPart(part);
		state.addDrawingPart(textLayer);
        state.setTextValue(key);

		return state;

	}
	
	/**
	 * @return the topSqlsSqlText
	 */
	public int getTopSqlsSqlText() {
		return topSqlsSqlText;
	}

	/**
	 * @param topSqlsSqlText the topSqlsSqlText to set
	 */
	public void setTopSqlsSqlText(int topSqlsSqlText) {
		this.topSqlsSqlText = topSqlsSqlText;
	}
	
}
