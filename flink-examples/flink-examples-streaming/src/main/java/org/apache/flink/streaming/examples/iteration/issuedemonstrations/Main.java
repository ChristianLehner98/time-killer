package org.apache.flink.streaming.examples.iteration.issuedemonstrations;

import org.apache.flink.streaming.examples.iteration.issuedemonstrations.lostmessagestest.LostMessagesExample;

public class Main {

	public static void main(String args[]) throws Exception {
		if(args.length == 0) {
			System.err.println("specify the prgram you want to run");
			return;
		}
		if(args[0].equalsIgnoreCase("persistentState")) {
			SaveAndReadPersistentStateExample example = new SaveAndReadPersistentStateExample(1000);
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
