package strata.aigen;

public class AutoScheduleGenerator {

	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("USAGE: java AutoScheduleGenerator <numAsyncOp> <numNode> <schedDir (e.g. /home/kroud2/bhkim/strata/scheduleGen/)>");
			System.exit(1);
		}
		int numAsyncOp = Integer.parseInt(args[0]);
		int numNode = Integer.parseInt(args[1]);
		String schedDir = args[2];
		System.out.println("number of async op using=" + numAsyncOp);
		System.out.println("number of nodes using=" + numNode);
		System.out.println("directory to store schedule files=" + schedDir);

		//stats BEGIN
		int curExploringPathLength = -1;
		int shortestSchedLength = 10000;
		int longestSchedLength = -1;
		int cumSchedLength = -1;
		int cumSchedCounts = 0;
		//stats END
		
		ScheduleGenerator sgen = new ScheduleGenerator(numAsyncOp, numNode, schedDir);
		sgen.generateSchedule();
		sgen.findInitialPaths();
		
		//stats BEGIN
		curExploringPathLength = sgen.currentExploringPath.size();
		if (curExploringPathLength < shortestSchedLength) {
			shortestSchedLength = curExploringPathLength;
		}
		if (curExploringPathLength > longestSchedLength) {
			longestSchedLength = curExploringPathLength;
		}
		cumSchedLength += curExploringPathLength;
		cumSchedCounts++;
		//stats END
				
		
		while(!sgen.initialPaths.isEmpty()) {
			sgen.currentInitPath = sgen.initialPaths.removeFirst();
			sgen.resetTest();
			sgen.replayInitPath();
			sgen.generateSchedule();
			// stats BEGIN
			curExploringPathLength = sgen.currentExploringPath.size();
			if (curExploringPathLength < shortestSchedLength) {
				shortestSchedLength = curExploringPathLength;
			}
			if (curExploringPathLength > longestSchedLength) {
				longestSchedLength = curExploringPathLength;
			}
			cumSchedLength += curExploringPathLength;
			cumSchedCounts++;
			// stats END
			sgen.findInitialPaths();
			if (sgen.uniqSchedCnt % 10000 == 0) {
				System.out.println("count=" + sgen.uniqSchedCnt);
			}
		}
		System.out.println("shortestSchedLength=" + shortestSchedLength);
		System.out.println("longestSchedLength=" + longestSchedLength);
		System.out.println("cumSchedLength=" + cumSchedLength);
		System.out.println("cumSchedCounts=" + cumSchedCounts);
		System.out.println("averageSchedLength=" + (cumSchedLength / cumSchedCounts));
		System.out.println("Number of generated schedule (unique) = " + sgen.uniqSchedCnt);
		System.out.println("Number of duplicated schedule (abandoned) = " + sgen.dupSchedCnt);
	}
	
}
