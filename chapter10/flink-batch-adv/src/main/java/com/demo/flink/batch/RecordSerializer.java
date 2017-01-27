package com.demo.flink.batch;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class RecordSerializer extends Serializer<Record> {

	@Override
	public Record read(Kryo kryo, Input input, Class<Record> type) {

		return new Record(input.readString(), input.readString(), input.read(), input.readString(), input.read(),
				input.read(), input.read(), input.read());
	}

	@Override
	public void write(Kryo kryo, Output output, Record object) {
		output.writeString(object.getPlayerName());
		output.writeString(object.getCountry());
		output.writeInt(object.getYear());
		output.writeString(object.getGame());
		output.writeInt(object.getGold());
		output.writeInt(object.getSilver());
		output.writeInt(object.getBronze());
		output.writeInt(object.getTotal());

	}

}
