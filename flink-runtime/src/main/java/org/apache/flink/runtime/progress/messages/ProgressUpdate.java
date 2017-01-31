package org.apache.flink.runtime.progress.messages;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProgressUpdate {
    private CountMap counts;
    private TerminationUpdates doneUpdates;

    public ProgressUpdate(CountMap counts, TerminationUpdates done) {
        this.counts = counts;
        this.doneUpdates = done;
    }

    public CountMap getCounts() {
        return counts;
    }
    public TerminationUpdates getDone() {
        return doneUpdates;
    }
}
