package com.demo.chapter05;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

public class EventDeserializationSchema implements DeserializationSchema<TemperatureEvent> {

	public TypeInformation<TemperatureEvent> getProducedType() {
		return TypeExtractor.getForClass(TemperatureEvent.class);
	}

	public TemperatureEvent deserialize(byte[] arg0) throws IOException {
		String str = new String(arg0, StandardCharsets.UTF_8);

		String[] parts = str.split("=");
		return new TemperatureEvent(parts[0], Double.parseDouble(parts[1]));
	}

	public boolean isEndOfStream(TemperatureEvent arg0) {
		return false;
	}

}
