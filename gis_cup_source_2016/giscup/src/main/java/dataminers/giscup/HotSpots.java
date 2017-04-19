package dataminers.giscup;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class HotSpots implements Serializable {
	private static final long serialVersionUID = 1L;
	public static JavaSparkContext sc;

	static Integer numPartitions;

	static SparkConf conf;

	static PriorityQueue<String> topZscore;
	static double standardDeviation;
	static double averageCost;
	static int[][][] space_time_cube;
	public static final int topK = 50;

	public static void main(String[] args) {

		conf = new SparkConf().setAppName("GISCUP-2016");
		sc = new JavaSparkContext(conf);
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		try {
			numPartitions = 200;
			System.out.println("X= " + MathUtility.X_LATITUDE + " Y= " + MathUtility.Y_LONGITUDE);
			getTopkHotSpots(sc, topK, args);
		} catch (IOException e) {
			e.printStackTrace();
		}
		sc.stop();
	}

	private static JavaPairRDD<Cell, Integer> collectData(JavaRDD<String> textFile) {
		JavaPairRDD<Cell, Integer> pairs = textFile.mapToPair(line -> {
		if(isPointValid(line)) {
				Cell cell = createCell(line);
				return new Tuple2<Cell, Integer>(cell, 1);
			}
			return new Tuple2<Cell, Integer>(null, 1);
		});

		JavaPairRDD<Cell, Integer> data = pairs.reduceByKey((a, b) -> a + b);
		return data;
	}

	public static void computeOrdisValue() {
		double ordisNumerator, ordisDenominator, ordisScore;
		// Sorts in ascending order
		Comparator<String> comparator = new Comparator<String>() {
			public int compare(String a, String b) {
				return -1 * Double.compare(Double.parseDouble(a.split(",")[0]), Double.parseDouble(b.split(",")[0]));
			}
		};

		topZscore = new PriorityQueue<String>(topK, comparator);

		for (int z = 0; z < space_time_cube.length; z++) {
			for (int i = 0; i < space_time_cube[0].length; i++) {
				for (int j = 0; j < space_time_cube[0][0].length; j++) {
					ordisNumerator = MathUtility.getOrdisNumerator(space_time_cube, z, i, j);
					ordisDenominator = MathUtility.getOrdisDenominator(space_time_cube, standardDeviation, z, i, j);
					ordisScore = ordisNumerator / ordisDenominator;
					topZscore.offer(ordisScore + "," + z + "," + i + "," + j);
				}
			}
		}
	}

	public static void createSpaceTimeCube(List<Tuple2<Cell, Integer>> data) {
		space_time_cube = new int[MathUtility.Z_DAYS][MathUtility.X_LATITUDE + 2][MathUtility.Y_LONGITUDE + 2];

		for (int i = 0; i < data.size(); i++) {
			if (data.get(i)._1 != null) {
				Cell temp = data.get(i)._1;
				space_time_cube[temp.getZ() - 1][temp.getX()][temp.getY()] += data.get(i)._2;
			}
		}
	}

	public static void getTopkHotSpots(JavaSparkContext sc, int topK, String[] args) throws IOException {

		JavaRDD<String> textFile = sc.textFile(args[0], numPartitions);

		List<Tuple2<Cell, Integer>> data = collectData(textFile).collect();

		createSpaceTimeCube(data);

		averageCost = MathUtility.averageAttributeCost(space_time_cube);
		System.out.println("mean: " + averageCost);
		standardDeviation = MathUtility.standardDeviation(space_time_cube, averageCost);
		System.out.println("standardDeviation: " + standardDeviation);
		computeOrdisValue();
		saveResults(topK, args[1]);

	}

	private static void saveResults(int topK, String path) throws IOException {
		FileWriter fileWriter = new FileWriter(new File(path));
		String entry = "";
		StringBuilder sb = new StringBuilder();
		int recordCount = 0;

		while (recordCount < topK) {
			entry = topZscore.poll();
			String[] tokens = entry.split(",");
			sb.append((Integer.parseInt(tokens[2]) + 4050)/100.00);
			sb.append(',');
			sb.append(-1 * ((Integer.parseInt(tokens[3]) + 7370))/100.00);
			sb.append(',');
			sb.append(tokens[1]);
			sb.append(',');
			sb.append(tokens[0]);
			sb.append('\n');
			recordCount++;
		}

		fileWriter.write(sb.toString());
		fileWriter.close();
	}

	private static Cell createCell(final String line) {
		final String[] fields = line.split(",");

		if (!fields[5].equals("pickup_longitude")) {
			final int x = Math.abs((int) Math.floor(100.0 * (Double.parseDouble(fields[6]) - MathUtility.LATITUDE_MIN)));
			final int y = Math.abs((int) Math.floor(100.0 * (Double.parseDouble(fields[5]) - MathUtility.LONGITUDE_MAX)));
			final int z = Integer.parseInt(fields[1].split(" ")[0].split("-")[2]);
			return new Cell(x, y, z);
		}
		return null;
	}

	private static boolean isPointValid(String line) {
		String[] row = line.split(",");
		if (!row[5].equals("pickup_longitude")) {
			Double y = Double.parseDouble(row[5]);
			Double x = Double.parseDouble(row[6]);
			if(x >= MathUtility.LATITUDE_MIN && x <= MathUtility.LATITUDE_MAX && y >= MathUtility.LONGITUDE_MIN && y <= MathUtility.LONGITUDE_MAX){
				return true;
			}
		}
		return false;
	}
}
