/*
 *-------------------
 * The EventColors.java is part of ASH Viewer
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
package org.ash.util;

import java.awt.Color;
import java.util.HashMap;

public class EventColors {
	
	/** The store labels of wait event and Color for each (details)*/
	private	static HashMap<String, Color> seriesNameColor;
	
	/**
	 * Constructor
	 */
	public EventColors(){
		/** Init */
		this.seriesNameColor = new HashMap<String, Color>();
		
		try {

		// dcvetkov
		// https://www.w3schools.com/colors/colors_rgb.asp

		this.seriesNameColor.put(Options.getInstance().getResource("CPULabel.text"),new Color(0,204,0));		// CPU + CPU Wait
		this.seriesNameColor.put(Options.getInstance().getResource("IOLabel.text"), new Color(0,74,231));		// User IO
		this.seriesNameColor.put(Options.getInstance().getResource("LockLabel.text"), new Color(192,40,0));		// Application
		this.seriesNameColor.put(Options.getInstance().getResource("LWLockLabel.text"),new Color(139,26,0));		// Concurrency
		this.seriesNameColor.put(Options.getInstance().getResource("BufferPinLabel.text"), new Color(0,161,230));	// Read by other session
		this.seriesNameColor.put(Options.getInstance().getResource("ActivityLabel.text"), new Color(255,165,0));
		this.seriesNameColor.put(Options.getInstance().getResource("ExtensionLabel.text"), new Color(0,123,20));
		this.seriesNameColor.put(Options.getInstance().getResource("ClientLabel.text"), new Color(159,147,113));	// Network
		this.seriesNameColor.put(Options.getInstance().getResource("IPCLabel.text"), new Color(240,110,170));		// Other
		this.seriesNameColor.put(Options.getInstance().getResource("TimeoutLabel.text"), new Color(84,56,28));

		/** CPU */
		this.seriesNameColor.put("CPU",new Color(0,204,0));

                // this.seriesNameColor.put("Activity", new Color(1,254,1));
                this.seriesNameColor.put("AddinShmemInitLock", new Color(2,253,254));
                this.seriesNameColor.put("advisory", new Color(3,252,2));
                this.seriesNameColor.put("async", new Color(4,251,253));
                this.seriesNameColor.put("AsyncCtlLock", new Color(5,250,3));
                this.seriesNameColor.put("AsyncQueueLock", new Color(6,249,252));
                this.seriesNameColor.put("AutoFileLock", new Color(7,248,4));
                this.seriesNameColor.put("AutovacuumLock", new Color(8,247,251));
                this.seriesNameColor.put("AutoVacuumMain", new Color(9,246,5));
                this.seriesNameColor.put("AutovacuumScheduleLock", new Color(10,245,250));
                this.seriesNameColor.put("BackendRandomLock", new Color(11,244,6));
                this.seriesNameColor.put("BackgroundWorkerLock", new Color(12,243,249));
                this.seriesNameColor.put("BgWorkerStartup", new Color(13,242,7));
                this.seriesNameColor.put("BgWriterHibernate", new Color(14,241,248));
                this.seriesNameColor.put("BgWriterMain", new Color(15,240,8));
                this.seriesNameColor.put("BtreePage", new Color(16,239,247));
                this.seriesNameColor.put("BtreeVacuumLock", new Color(17,238,9));
                this.seriesNameColor.put("buffer_content", new Color(18,237,246));
                this.seriesNameColor.put("buffer_io", new Color(19,236,10));
                this.seriesNameColor.put("buffer_mapping", new Color(220,35,245));
                // this.seriesNameColor.put("BufferPin", new Color(21,234,11));
                this.seriesNameColor.put("BufFileWrite", new Color(22,233,244));
                this.seriesNameColor.put("CheckpointerCommLock", new Color(23,232,12));
                this.seriesNameColor.put("CheckpointerMain", new Color(24,231,243));
                this.seriesNameColor.put("CheckpointLock", new Color(25,230,13));
                //this.seriesNameColor.put("Client", new Color(26,229,242));
                this.seriesNameColor.put("ClientRead", new Color(159,147,113));
                this.seriesNameColor.put("ClientWrite", new Color(27,228,14));
                this.seriesNameColor.put("clog", new Color(28,227,241));
                this.seriesNameColor.put("CLogControlLock", new Color(29,226,15));
                this.seriesNameColor.put("CLogTruncationLock", new Color(30,225,240));
                this.seriesNameColor.put("commit_timestamp", new Color(31,224,16));
                this.seriesNameColor.put("CommitTsControlLock", new Color(32,223,239));
                this.seriesNameColor.put("CommitTsLock", new Color(33,222,17));
                this.seriesNameColor.put("ControlFileLock", new Color(34,221,238));
                this.seriesNameColor.put("ControlFileRead", new Color(35,220,18));
                this.seriesNameColor.put("ControlFileSync", new Color(36,219,237));
                this.seriesNameColor.put("ControlFileSyncUpdate", new Color(37,218,19));
                this.seriesNameColor.put("ControlFileWrite", new Color(38,217,236));
                this.seriesNameColor.put("ControlFileWriteUpdate", new Color(39,216,20));
                this.seriesNameColor.put("CopyFileRead", new Color(40,215,235));
                this.seriesNameColor.put("CopyFileWrite", new Color(41,214,21));
                this.seriesNameColor.put("DataFileExtend", new Color(42,213,234));
                this.seriesNameColor.put("DataFileFlush", new Color(43,212,22));
                this.seriesNameColor.put("DataFileImmediateSync", new Color(44,211,233));
                this.seriesNameColor.put("DataFilePrefetch", new Color(45,210,23));
                this.seriesNameColor.put("DataFileRead", new Color(0,74,231));
                this.seriesNameColor.put("DataFileSync", new Color(47,208,24));
                this.seriesNameColor.put("DataFileTruncate", new Color(48,207,231));
                this.seriesNameColor.put("DataFileWrite", new Color(49,206,25));
                this.seriesNameColor.put("DSMFillZeroWrite", new Color(50,205,230));
                this.seriesNameColor.put("DynamicSharedMemoryControlLock", new Color(51,204,26));
                this.seriesNameColor.put("ExecuteGather", new Color(52,203,229));
                this.seriesNameColor.put("extend", new Color(53,202,27));
                // this.seriesNameColor.put("Extension", new Color(54,201,228));
                // this.seriesNameColor.put("IO", new Color(55,200,28));
                //this.seriesNameColor.put("IPC", new Color(56,199,227));
                this.seriesNameColor.put("LibPQWalReceiverConnect", new Color(57,198,29));
                this.seriesNameColor.put("LibPQWalReceiverReceive", new Color(58,197,226));
                // this.seriesNameColor.put("Lock", new Color(59,196,30));
                this.seriesNameColor.put("LockFileAddToDataDirRead", new Color(60,195,225));
                this.seriesNameColor.put("LockFileAddToDataDirSync", new Color(61,194,31));
                this.seriesNameColor.put("LockFileAddToDataDirWrite", new Color(62,193,224));
                this.seriesNameColor.put("LockFileCreateRead", new Color(63,192,32));
                this.seriesNameColor.put("LockFileCreateSync", new Color(64,191,223));
                this.seriesNameColor.put("LockFileCreateWrite", new Color(65,190,33));
                this.seriesNameColor.put("LockFileReCheckDataDirRead", new Color(66,189,222));
                this.seriesNameColor.put("lock_manager", new Color(167,108,34));
                this.seriesNameColor.put("LogicalApplyMain", new Color(68,187,221));
                this.seriesNameColor.put("LogicalLauncherMain", new Color(69,186,35));
                this.seriesNameColor.put("LogicalRepWorkerLock", new Color(70,185,220));
                this.seriesNameColor.put("LogicalRewriteCheckpointSync", new Color(71,184,36));
                this.seriesNameColor.put("LogicalRewriteMappingSync", new Color(72,183,219));
                this.seriesNameColor.put("LogicalRewriteMappingWrite", new Color(73,182,37));
                this.seriesNameColor.put("LogicalRewriteSync", new Color(74,181,218));
                this.seriesNameColor.put("LogicalRewriteWrite", new Color(75,180,38));
                this.seriesNameColor.put("LogicalSyncData", new Color(76,179,217));
                this.seriesNameColor.put("LogicalSyncStateChange", new Color(77,178,39));
                this.seriesNameColor.put("MessageQueueInternal", new Color(78,177,216));
                this.seriesNameColor.put("MessageQueuePutMessage", new Color(79,176,40));
                this.seriesNameColor.put("MessageQueueReceive", new Color(80,175,215));
                this.seriesNameColor.put("MessageQueueSend", new Color(81,174,41));
                this.seriesNameColor.put("MultiXactGenLock", new Color(82,173,214));
                this.seriesNameColor.put("multixact_member", new Color(83,172,42));
                this.seriesNameColor.put("MultiXactMemberControlLock", new Color(84,171,213));
                this.seriesNameColor.put("multixact_offset", new Color(85,170,43));
                this.seriesNameColor.put("MultiXactOffsetControlLock", new Color(86,169,212));
                this.seriesNameColor.put("MultiXactTruncationLock", new Color(87,168,44));
                this.seriesNameColor.put("object", new Color(88,167,211));
                this.seriesNameColor.put("OidGenLock", new Color(89,166,45));
                this.seriesNameColor.put("oldserxid", new Color(90,165,210));
                this.seriesNameColor.put("OldSerXidLock", new Color(91,164,46));
                this.seriesNameColor.put("OldSnapshotTimeMapLock", new Color(92,163,209));
                this.seriesNameColor.put("page", new Color(93,162,47));
                this.seriesNameColor.put("ParallelBitmapScan", new Color(94,161,208));
                this.seriesNameColor.put("ParallelFinish", new Color(95,160,48));
                this.seriesNameColor.put("parallel_query_dsa", new Color(96,159,207));
                this.seriesNameColor.put("PgSleep", new Color(97,158,49));
                this.seriesNameColor.put("PgStatMain", new Color(98,157,206));
                this.seriesNameColor.put("predicate_lock_manager", new Color(99,156,50));
                this.seriesNameColor.put("proc", new Color(100,155,205));
                this.seriesNameColor.put("ProcArrayGroupUpdate", new Color(101,154,51));
                this.seriesNameColor.put("ProcArrayLock", new Color(102,153,204));
                this.seriesNameColor.put("RecoveryApplyDelay", new Color(103,152,52));
                this.seriesNameColor.put("RecoveryWalAll", new Color(104,151,203));
                this.seriesNameColor.put("RecoveryWalStream", new Color(105,150,53));
                this.seriesNameColor.put("relation", new Color(99,138,215));
                this.seriesNameColor.put("RelationMappingLock", new Color(106,149,202));
                this.seriesNameColor.put("RelationMapRead", new Color(107,148,54));
                this.seriesNameColor.put("RelationMapSync", new Color(108,147,201));
                this.seriesNameColor.put("RelationMapWrite", new Color(109,146,55));
                this.seriesNameColor.put("RelCacheInitLock", new Color(110,145,200));
                this.seriesNameColor.put("ReorderBufferRead", new Color(111,144,56));
                this.seriesNameColor.put("ReorderBufferWrite", new Color(112,143,199));
                this.seriesNameColor.put("ReorderLogicalMappingRead", new Color(113,142,57));
                this.seriesNameColor.put("replication_origin", new Color(114,141,198));
                this.seriesNameColor.put("ReplicationOriginDrop", new Color(115,140,58));
                this.seriesNameColor.put("ReplicationOriginLock", new Color(116,139,197));
                this.seriesNameColor.put("ReplicationSlotAllocationLock", new Color(117,138,59));
                this.seriesNameColor.put("ReplicationSlotControlLock", new Color(118,137,196));
                this.seriesNameColor.put("ReplicationSlotDrop", new Color(119,136,60));
                this.seriesNameColor.put("replication_slot_io", new Color(120,135,195));
                this.seriesNameColor.put("ReplicationSlotRead", new Color(121,134,61));
                this.seriesNameColor.put("ReplicationSlotRestoreSync", new Color(122,133,194));
                this.seriesNameColor.put("ReplicationSlotSync", new Color(123,132,62));
                this.seriesNameColor.put("ReplicationSlotWrite", new Color(124,131,193));
                this.seriesNameColor.put("SafeSnapshot", new Color(125,130,63));
                this.seriesNameColor.put("SerializableFinishedListLock", new Color(126,129,192));
                this.seriesNameColor.put("SerializablePredicateLockListLock", new Color(127,128,64));
                this.seriesNameColor.put("SerializableXactHashLock", new Color(128,127,191));
                this.seriesNameColor.put("ShmemIndexLock", new Color(129,126,65));
                this.seriesNameColor.put("SInvalReadLock", new Color(130,125,190));
                this.seriesNameColor.put("SInvalWriteLock", new Color(131,124,66));
                this.seriesNameColor.put("SLRUFlushSync", new Color(132,123,189));
                this.seriesNameColor.put("SLRURead", new Color(133,122,67));
                this.seriesNameColor.put("SLRUSync", new Color(134,121,188));
                this.seriesNameColor.put("SLRUWrite", new Color(135,120,68));
                this.seriesNameColor.put("SnapbuildRead", new Color(136,119,187));
                this.seriesNameColor.put("SnapbuildSync", new Color(137,118,69));
                this.seriesNameColor.put("SnapbuildWrite", new Color(138,117,186));
                this.seriesNameColor.put("speculative", new Color(139,116,70));
                this.seriesNameColor.put("SSLOpenServer", new Color(140,115,185));
                this.seriesNameColor.put("subtrans", new Color(141,114,71));
                this.seriesNameColor.put("SubtransControlLock", new Color(142,113,184));
                this.seriesNameColor.put("SyncRep", new Color(143,112,72));
                this.seriesNameColor.put("SyncRepLock", new Color(144,111,183));
                this.seriesNameColor.put("SyncScanLock", new Color(145,110,73));
                this.seriesNameColor.put("SysLoggerMain", new Color(146,109,182));
                this.seriesNameColor.put("TablespaceCreateLock", new Color(147,108,74));
                this.seriesNameColor.put("tbm", new Color(148,107,181));
                this.seriesNameColor.put("TimelineHistoryFileSync", new Color(149,106,75));
                this.seriesNameColor.put("TimelineHistoryFileWrite", new Color(150,105,180));
                this.seriesNameColor.put("TimelineHistoryRead", new Color(151,104,76));
                this.seriesNameColor.put("TimelineHistorySync", new Color(152,103,179));
                this.seriesNameColor.put("TimelineHistoryWrite", new Color(153,102,77));
                // this.seriesNameColor.put("Timeout", new Color(154,101,178));
                this.seriesNameColor.put("transactionid", new Color(192,40,0));
                this.seriesNameColor.put("tuple", new Color(156,99,177));
                this.seriesNameColor.put("TwophaseFileRead", new Color(157,98,79));
                this.seriesNameColor.put("TwophaseFileSync", new Color(158,97,176));
                this.seriesNameColor.put("TwophaseFileWrite", new Color(159,96,80));
                this.seriesNameColor.put("TwoPhaseStateLock", new Color(160,95,175));
                this.seriesNameColor.put("userlock", new Color(161,94,81));
                this.seriesNameColor.put("virtualxid", new Color(162,93,174));
                this.seriesNameColor.put("WALBootstrapSync", new Color(163,92,82));
                this.seriesNameColor.put("WALBootstrapWrite", new Color(164,91,173));
                this.seriesNameColor.put("WALBufMappingLock", new Color(165,90,83));
                this.seriesNameColor.put("WALCopyRead", new Color(166,89,172));
                this.seriesNameColor.put("WALCopySync", new Color(167,88,84));
                this.seriesNameColor.put("WALCopyWrite", new Color(168,87,171));
                this.seriesNameColor.put("WALInitSync", new Color(169,86,85));
                this.seriesNameColor.put("WALInitWrite", new Color(170,85,170));
                this.seriesNameColor.put("wal_insert", new Color(46,184,86));
                this.seriesNameColor.put("WALRead", new Color(172,83,169));
                this.seriesNameColor.put("WalReceiverMain", new Color(173,82,87));
                this.seriesNameColor.put("WalReceiverWaitStart", new Color(174,81,168));
                this.seriesNameColor.put("WalSenderMain", new Color(175,80,88));
                this.seriesNameColor.put("WALSenderTimelineHistoryRead", new Color(176,79,167));
                this.seriesNameColor.put("WalSenderWaitForWAL", new Color(177,78,89));
                this.seriesNameColor.put("WalSenderWriteData", new Color(178,77,166));
                this.seriesNameColor.put("WALSyncMethodAssign", new Color(179,76,90));
                this.seriesNameColor.put("WALWrite", new Color(180,75,165));
                this.seriesNameColor.put("WALWriteLock", new Color(139,26,0));
                this.seriesNameColor.put("WalWriterMain", new Color(182,73,164));
                this.seriesNameColor.put("XidGenLock", new Color(23,72,92));



		} catch (Exception e){
     		e.printStackTrace();
     	}
	}
	
	/**
	 * Get Color for event name
	 * @param eventName
	 * @return new Color(r,g,b)
	 */
	public static Color getColor(String eventName){
		if (seriesNameColor.containsKey(eventName)){
			return seriesNameColor.get(eventName);
		} else {
			return new Color(100,100,100);
		}
	}
	

}
