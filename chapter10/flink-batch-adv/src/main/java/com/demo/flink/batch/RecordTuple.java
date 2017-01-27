package com.demo.flink.batch;

import org.apache.flink.api.java.tuple.Tuple8;

public class RecordTuple extends Tuple8<String, String, Integer, String, Integer, Integer, Integer, Integer> {

	private static final long serialVersionUID = 1L;

	public RecordTuple() {
		super();
	}

	public RecordTuple(String value0, String value1, Integer value2, String value3, Integer value4, Integer value5,
			Integer value6, Integer value7) {
		super(value0, value1, value2, value3, value4, value5, value6, value7);
	}

}
