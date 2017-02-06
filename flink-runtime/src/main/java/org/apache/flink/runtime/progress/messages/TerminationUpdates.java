package org.apache.flink.runtime.progress.messages;

import java.util.*;

public class TerminationUpdates {
    private int operatorId;
    private int numberOfParallelInstances;
    private Map<List<Long>, Set<Integer>> updates = new HashMap<>();

    public TerminationUpdates(int operatorId, int numberOfParallelInstances) {
        this.operatorId = operatorId;
        this.numberOfParallelInstances = numberOfParallelInstances;
    }

    private void updateAll(List<Long> context, Set<Integer> instances) {
        for(Integer instance : instances) {
            update(context, instance);
        }
    }

    public void update(List<Long> context, int instance) {
        Set<Integer> current = updates.get(context);
        if(current == null) {
            current = new HashSet<>();
            updates.put(context, current);
        }
        current.add(instance);
    }

    public void tryMerge(TerminationUpdates other) {
        if (operatorId != other.operatorId) return;

        for(Map.Entry<List<Long>, Set<Integer>> entry : other.updates.entrySet()) {
            updateAll(entry.getKey(), entry.getValue());
        }
    }

    public Set<List<Long>> isDoneOnAllInstances() {
        Set<List<Long>> result = new HashSet<>();
        Iterator<Map.Entry<List<Long>,Set<Integer>>> entries = updates.entrySet().iterator();
        while(entries.hasNext()) {
            Map.Entry<List<Long>,Set<Integer>> entry = entries.next();
            if(entry.getValue().size() == numberOfParallelInstances) {
                result.add(entry.getKey());
                entries.remove();
            }
        }
        return result;
    }

    public void clear() {
        updates = new HashMap<>();
    }

    public int getOperatorId() {
    	return operatorId;
	}
}
