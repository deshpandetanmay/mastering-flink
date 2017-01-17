package com.demo.flink.streaming;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Custom Watermark Emitter. 
 * @author TDeshpande
 *
 */
public class CustomWatermarkEmitter implements AssignerWithPunctuatedWatermarks<String> {

	
	private static final long serialVersionUID = 1L;

	@Override
	public long extractTimestamp(String arg0, long arg1) {
		if (null != arg0 && arg0.contains(",")) {
			String parts[] = arg0.split(",");
			return Long.parseLong(parts[0]);
		}

		return 0;
	}

	@Override
	public Watermark checkAndGetNextWatermark(String arg0, long arg1) {
		if (null != arg0 && arg0.contains(",")) {
			String parts[] = arg0.split(",");
			return new Watermark(Long.parseLong(parts[0]));
		}
		return null;
	}

}
