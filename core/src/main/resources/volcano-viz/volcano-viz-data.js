/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// 
var data = {

	allNodes: {
		"node1": {
			label: "TableScan-1", // node label being displayed 
			isSubset: false, // if the node is a RelSubset
			explanation: "table=t1", // additional explanation of properties
			finalCost: "100 cpu, 100io" // final cost (in string) of the node at the end of optimization
		},
		"node2": {
			label: "Filter-2",
			isSubset: false,
			explanation: "condition=c",
			finalCost: "200cpu, 100io"
		},
		"node3": {
			label: "TableSink-3",
			isSubset: false,
			explanation: "table=t2",
			finalCost: "20cpu, 20io"
		},
		"node4": {
			label: "IndexTableScan-4",
			isSubset: false,
			explanation: "table=t1, condition=c",
			finalCost: "10cpu, 10io"
		},
	},

	ruleMatchSequence: [
		"INITIAL",
		"IndexTableScanRule#1",
		"FINAL"
	],

	ruleMatchInfoMap: {
		"INITIAL": {
			setLabels: {
				"set1": "set1",
				"set2": "set2",
				"set3": "set3",
			},
			setOriginalRel: {
				"set1": "node1",
				"set2": "node2",
				"set3": "node3",
			},
			nodesInSet: {
				"set1": ["node1"],
				"set2": ["node2"],
				"set3": ["node3"]
			}, 
			nodeInputs: {
				"node2": ["node1"],
				"node3": ["node2"]
			},
			matchedNodes: [],
			newNodes: []
		},
		"IndexTableScanRule#1": {
			setLabels: {
				"set1": "set1",
				"set2": "set2",
				"set3": "set3",
			},
			setOriginalRel: {
				"set1": "node1",
				"set2": "node2",
				"set3": "node3",
			},
			nodesInSet: {
				"set1": ["node1"],
				"set2": ["node2", "node4"],
				"set3": ["node3"]
			}, 
			nodeInputs: {
				"node2": ["node1"],
				"node3": ["node2", "node4"]
			},
			matchedNodes: ["node1", "node2"],
			newNodes: ["node4"]
		},
		"FINAL": {
			setLabels: {
				"set1": "set1",
				"set2": "set2",
				"set3": "set3",
			},
			setOriginalRel: {
				"set1": "node1",
				"set2": "node2",
				"set3": "node3",
			},
			nodesInSet: {
				"set1": ["node1"],
				"set2": ["node2", "node4"],
				"set3": ["node3"]
			}, 
			nodeInputs: {
				"node2": ["node1"],
				"node3": ["node2", "node4"]
			},
			// for the FINAL rule match, the matchedNodes represent the final selected plan
			matchedNodes: ["node4", "node3"],
			newNodes: []
		}
	},

	ruleMatchTvrInfoMap: {
		"INITIAL": [{ 
			"tvrId": "tvr1", 
			"tvrSets": { "SetSnapshot@(MAX)": ["set1"] },
			"tvrPropertyLinks": {}
		}, { 
			"tvrId": "tvr2", 
			"tvrSets": { "SetSnapshot@(MAX)": ["set2"] },
			"tvrPropertyLinks": {}
		}, { 
			"tvrId": "tvr3", 
			"tvrSets": { "SetSnapshot@(MAX)": ["set3"] },
			"tvrPropertyLinks": {}
		}],
		"IndexTableScanRule#1": [{ 
			"tvrId": "tvr1", 
			"tvrSets": { "SetSnapshot@(MAX)": ["set1"] },
			"tvrPropertyLinks": { "TvrPropertyFoo": ["tvr2"] }
		}, { 
			"tvrId": "tvr2", 
			"tvrSets": { "SetSnapshot@(MAX)": ["set2"] },
			"tvrPropertyLinks": { "TvrPropertyBar": ["tvr3", "tvr1"] }
		}, { 
			"tvrId": "tvr3",
			"tvrSets": { "SetSnapshot@(MAX)": ["set3"] },
			"tvrPropertyLinks": {}
		}],
		"FINAL": [{ 
			"tvrId": "tvr1", 
			"tvrSets": { "SetSnapshot@(MAX)": ["set1"] },
			"tvrPropertyLinks": { "TvrPropertyFoo": ["tvr2"] }
		}, { 
			"tvrId": "tvr2", 
			"tvrSets": { "SetSnapshot@(MAX)": ["set2"] },
			"tvrPropertyLinks": { "TvrPropertyBar": ["tvr3"] }
		}, { 
			"tvrId": "tvr3", 
			"tvrSets": { "SetSnapshot@(MAX)": ["set3"] },
			"tvrPropertyLinks": {}
		}],
	},

	nodeAddedInRule: {
		"node1": "INITIAL",
		"node2": "INITIAL",
		"node3": "INITIAL",
		"node4": "IndexTableScanRule#1"
	}

}
