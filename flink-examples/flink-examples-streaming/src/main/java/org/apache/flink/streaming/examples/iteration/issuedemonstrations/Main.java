package org.apache.flink.streaming.examples.iteration.issuedemonstrations;

import org.apache.flink.streaming.examples.iteration.issuedemonstrations.SaveAndReadPersistentStateExample.OverwriteMode;
import org.apache.flink.streaming.examples.iteration.issuedemonstrations.SaveAndReadPersistentStateExample.StateAccessMode;
import org.apache.flink.streaming.examples.iteration.issuedemonstrations.SaveAndReadPersistentStateExample.StateMode;
import org.apache.flink.streaming.examples.iteration.issuedemonstrations.lostmessagestest.LostMessagesExample;

public class Main {

	public static void main(String args[]) throws Exception {
		if(args.length == 0) {
			System.err.println("specify the prgram you want to run");
			return;
		}
		if(args[0].equalsIgnoreCase("persistentState")) {
			if(args.length < 5) {
				System.err.println("missing arguments");
				return;
			}
			int sleepTimePerElement = Integer.parseInt(args[1]);
			StateMode stateMode = StateMode.parse(args[2]);
			OverwriteMode overwriteMode = OverwriteMode.parse(args[3]);
			StateAccessMode stateAccessMode = StateAccessMode.parse(args[4]);
			SaveAndReadPersistentStateExample example = new SaveAndReadPersistentStateExample(sleepTimePerElement,
				stateMode, overwriteMode, stateAccessMode);
			example.run();
		} else if(args[0].equalsIgnoreCase("watermarks")) {
			WatermarksExample example = new WatermarksExample();
			example.run();
		} else if (args[0].equalsIgnoreCase("lostMessages")) {
			LostMessagesExample example = new LostMessagesExample();
			example.run();
		}


	}

}
