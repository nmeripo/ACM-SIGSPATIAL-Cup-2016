package dataminers.giscup;


public class MathUtility {
	public static final double LONGITUDE_MIN = -74.25;
	public static final double LONGITUDE_MAX = -73.7;
	public static final double LATITUDE_MIN = 40.5;
	public static final double LATITUDE_MAX = 40.9;
	public static final double STEP = 0.01;
	public static final int Z_DAYS = 31;
	public static final int Y_LONGITUDE = (int) -((LONGITUDE_MIN - LONGITUDE_MAX) / STEP);
	public static final int X_LATITUDE = (int) ((LATITUDE_MAX - LATITUDE_MIN) / STEP);
	public static final int totalNumberOfCells = Z_DAYS * (X_LATITUDE + 2) * (Y_LONGITUDE + 2);

	public static double averageAttributeCost(int[][][] space_time_cube) {
		return (double) totalAttributeCost(space_time_cube) / totalNumberOfCells;
	}

	private static int totalAttributeCost(int[][][] space_time_cube) {
		int totalAttributeCost = 0;
		for (int z = 0; z < space_time_cube.length; z++) {
			for (int i = 0; i < space_time_cube[0].length; i++) {
				for (int j = 0; j < space_time_cube[0][0].length; j++) {
					totalAttributeCost += space_time_cube[z][i][j];
				}
			}
		}
		return totalAttributeCost;
	}

	public static double standardDeviation(int[][][] spaceTimeCube, double averageCost) {
		double standardDeviation = 0.0;
		for (int i = 0; i < spaceTimeCube.length; i++) {
			for (int j = 0; j < spaceTimeCube[0].length; j++) {
				for (int k = 0; k < spaceTimeCube[0][0].length; k++) {
					standardDeviation += Math.pow((double) spaceTimeCube[i][j][k], 2);
				}
			}
		}
		standardDeviation /= totalNumberOfCells;
		standardDeviation -= Math.pow(averageCost, 2);
		standardDeviation = Math.sqrt(standardDeviation);
		return standardDeviation;
	}

	public static double getOrdisNumerator(int[][][] space_time_cube, int time, int row, int col) {
		int totalNeighbors = 0;
		double ordisNumerator = 0.00;
		
		for (int k = time - 1; k <= time + 1; k++) {
			for (int i = row - 1; i <= row + 1; i++) {
				for (int j = col - 1; j <= col + 1; j++) {
					if (!(k < 0 || k >= (space_time_cube).length || i < 0 || i >= (space_time_cube)[0].length || j < 0 || 
							j >= (space_time_cube)[0][0].length)) {
						totalNeighbors++;
						ordisNumerator += (space_time_cube)[k][i][j];
					}
				}
			}
		}
		
		ordisNumerator -= (averageAttributeCost(space_time_cube) * (double) totalNeighbors);
		return ordisNumerator;
	}

	public static double getOrdisDenominator(int[][][] space_time_cube, double coVar, int time, int row, int col) {
		int count = 0;
		double ordisDenominator = 0.00;
		
		for (int k = time - 1; k <= time + 1; k++) {
			for (int i = row - 1; i <= row + 1; i++) {
				for (int j = col - 1; j <= col + 1; j++) {
					if (!(k < 0 || k >= space_time_cube.length || i < 0 || i >= space_time_cube[0].length || j < 0 || j >= space_time_cube[0][0].length)) {
						count++;
					}
				}
			}
		}
		
		ordisDenominator = (double) totalNumberOfCells * (double) count;
		ordisDenominator -= Math.pow((double) count, 2);
		ordisDenominator /= (double) (totalNumberOfCells - 1);
		ordisDenominator = Math.sqrt(ordisDenominator);
		ordisDenominator *= coVar;
		return ordisDenominator;
	}
}
