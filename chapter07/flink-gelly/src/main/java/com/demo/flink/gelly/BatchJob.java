package com.demo.flink.gelly;

import org.apache.flink.api.java.DataSet;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;

public class BatchJob {

	final static String srcId = "s15";

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Create graph by reading from CSV files
		DataSet<Tuple2<String, Double>> airportVertices = env
				.readCsvFile("D://work//Mastering Flink//Chapter 7//data//nodes.csv").types(String.class, Double.class);

		DataSet<Tuple3<String, String, Double>> airportEdges = env
				.readCsvFile("D://work//Mastering Flink//Chapter 7//data//edges.csv")
				.types(String.class, String.class, Double.class);

		Graph<String, Double, Double> graph = Graph.fromTupleDataSet(airportVertices, airportEdges, env);

		// Find out no. of airports and routes
		System.out.println("No. of Routes in Graph:" + graph.numberOfEdges());
		System.out.println("No. of Airports in Graph:" + graph.numberOfVertices());

		// define the maximum number of iterations
		int maxIterations = 10;

		// Execute the vertex-centric iteration
		Graph<String, Double, Double> result = graph.runVertexCentricIteration(new SSSPComputeFunction(),
				new SSSPCombiner(), maxIterations);

		// Extract the vertices as the result
		DataSet<Vertex<String, Double>> singleSourceShortestPaths = result.getVertices();
		
		singleSourceShortestPaths.print();
		
		

	}

	final static class SSSPComputeFunction extends ComputeFunction<String, Double, Double, Double> {

		@Override
		public void compute(Vertex<String, Double> vertex, MessageIterator<Double> messages) throws Exception {
			double minDistance = (vertex.getId().equals(srcId)) ? 0d : Double.POSITIVE_INFINITY;
			for (Double msg : messages) {
				minDistance = Math.min(minDistance, msg);
			}

			if (minDistance < vertex.getValue()) {
				setNewVertexValue(minDistance);
				for (Edge<String, Double> e : getEdges()) {
					sendMessageTo(e.getTarget(), minDistance + e.getValue());
				}
			}

		}

	}

	final static class SSSPCombiner extends MessageCombiner<String, Double> {

		public void combineMessages(MessageIterator<Double> messages) {

			double minMessage = Double.POSITIVE_INFINITY;
			for (Double msg : messages) {
				minMessage = Math.min(minMessage, msg);
			}
			sendCombinedMessage(minMessage);
		}

	}

}
