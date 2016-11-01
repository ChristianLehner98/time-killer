/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.transformations;

import com.google.common.collect.Lists;
import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import java.util.Collection;
import java.util.List;

/**
 * This transformation represents an exit from an iteration scope. It is there to aid the proper
 * job graph construction and coordinate the level of encapsulation in the dataflow graph.

 */
@Internal
public class ScopeTransformation<T> extends StreamTransformation<T> {
	
	private final StreamTransformation<T> input;
	private final SCOPE_TYPE scopeType;

	public enum SCOPE_TYPE {INGRESS, EGRESS}

	public ScopeTransformation(StreamTransformation<T> input, SCOPE_TYPE type) {
		super("Scope", input.getOutputType(), input.getParallelism());
		this.input = input;
		this.scopeType = type;
	}

	/**
	 * Returns the input {@code StreamTransformation}.
	 */
	public StreamTransformation<T> getInput() {
		return input;
	}

	@Override
	public Collection<StreamTransformation<?>> getTransitivePredecessors() {
		List<StreamTransformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}

	@Override
	public void setScopeLevel(int scopeLevel) {
	}

	@Override
	public int getScopeLevel() {
		return scopeType==SCOPE_TYPE.INGRESS ? input.getScopeLevel() + 1 : input.getScopeLevel() - 1;
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
	}

}


