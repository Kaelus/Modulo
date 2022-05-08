package strata.conex.zookeeper;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Scanner;

import strata.conex.SystemController;
import strata.conex.TestingEngine;

public class ZooKeeperSystemControllerUnitTest {

	protected static SystemController controller; 
	
	private static void testWriteData() {
		
		String path = "/testDivergenceResync";
		int base = 0;
		int numAOP = 3;
		String[] keys = new String[numAOP];
				
		controller.prepareTestingEnvironment();
		controller.startEnsemble();
		controller.bootstrapClients();

		for (int i = 0; i < numAOP; i++) {
			keys[i] = path + i;
		}
		controller.createData(0,  TestingEngine.probingKey, 777);
		
		for (int i = 0; i < numAOP; i++) {
   			controller.createData(0,  keys[i], i+base);
   		}
		
		base = 1000;
		for (int i = 0 ; i < numAOP; i++) {
			controller.writeData(0, keys[i], base + i);
		}
				
		byte[][] valBytes = new byte[numAOP][];
		for (int i = 0 ; i < numAOP; i++) {
			valBytes[i] = controller.readData(0, keys[i]);
			String valStr = "NONE";
			try {
				valStr = new String(valBytes[i], "UTF-8");
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (!valStr.equals("" + (base + i))) {
				System.err.println("[TEST FAILED] writeData test failed. valStr=" + valStr + " while base+i=" + (base+i));
				controller.takedownClients();
				controller.stopEnsemble();
				controller.resetTest();
				return;
			}
		}
		System.out.println("[TEST PASSED] writeData test passed");
		controller.takedownClients();
		controller.stopEnsemble();
		controller.resetTest();
	}

	private static void testReadData() {
		
		String path = "/testDivergenceResync";
		int base = 0;
		int numAOP = 3;
		String[] keys = new String[numAOP];
		
		controller.prepareTestingEnvironment();
		controller.startEnsemble();
		controller.bootstrapClients();
		
		for (int i = 0; i < numAOP; i++) {
			keys[i] = path + i;
		}
		controller.createData(0,  TestingEngine.probingKey, 777);
		
		for (int i = 0; i < numAOP; i++) {
   			controller.createData(0,  keys[i], i+base);
   		}
		
		byte[][] initValBytes = new byte[numAOP][];
		for (int i = 0 ; i < numAOP; i++) {
			initValBytes[i] = controller.readData(0, keys[i]);
		}
		
		controller.stopEnsemble();
		controller.startNode(0);
		controller.startNode(1);
		
		byte[][] againValBytes = new byte[numAOP][];
		for (int i = 0 ; i < numAOP; i++) {
			againValBytes[i] = controller.readData(0, keys[i]);
			if (!Arrays.equals(initValBytes[i],againValBytes[i])) {
				System.err.println("[TEST FAILED] readData test failed");
				controller.takedownClients();
				controller.stopEnsemble();
				controller.resetTest();
				return;
			}
		}
		System.out.println("[TEST PASSED] testReadData passed");	
		controller.takedownClients();
		controller.stopEnsemble();
		controller.resetTest();
	}

	private static void testCreateData() {
		String path = "/testDivergenceResync";
		int base = 0;
		int numAOP = 3;
		String[] keys = new String[numAOP];
		
		controller.prepareTestingEnvironment();
		controller.startEnsemble();
		controller.bootstrapClients();

		for (int i = 0; i < numAOP; i++) {
			keys[i] = path + i;
		}
		controller.createData(0,  TestingEngine.probingKey, 777);
		
		for (int i = 0; i < numAOP; i++) {
   			controller.createData(0,  keys[i], i+base);
   		}
		byte[] valBytes = null;
		for (int i = 0; i < numAOP; i++) {
			valBytes = controller.readData(0, keys[i]);
			String valStr = "NONE";
			try {
				valStr = new String(valBytes, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			if (valBytes == null || !valStr.equals("" + (i + base))) {
				System.err.println("[TEST FAILED] createData test failed");
				controller.takedownClients();
				controller.stopEnsemble();
				controller.resetTest();
				return;
			}
		}
		System.out.println("[TEST PASSED] testCreatData passed");
		
		controller.takedownClients();
		controller.stopEnsemble();
		controller.resetTest();
	}
	

	private static void testSortTargetState() {
		((ZooKeeperSystemController)controller).actualLeader = 2;
		int[] unsortedTargetState = new int[] {1,1,0};
		int[] expectedSortedTargetState1 = new int[] {1,0,1};
		int[] expectedSortedTargetState2 = new int[] {0,1,1};
		int[] sortedTargetState;
		
		// test basic case
		((ZooKeeperSystemController)controller).isNodeOnline[0] = true;
		((ZooKeeperSystemController)controller).isNodeOnline[1] = true;
		((ZooKeeperSystemController)controller).isNodeOnline[2] = true;
		sortedTargetState = controller.sortTargetState(unsortedTargetState.clone());
		if (Arrays.equals(sortedTargetState, expectedSortedTargetState1) 
				|| Arrays.equals(sortedTargetState, expectedSortedTargetState2)) {
			System.out.println("test passed");
		} else {
			System.out.println("test failed. output sortedTargetState was " + Arrays.toString(sortedTargetState));
			System.out.println("We expected:");
			System.out.println("  " + Arrays.toString(expectedSortedTargetState1));
			System.out.println("  " + Arrays.toString(expectedSortedTargetState2));
		}

		// test basic case
		((ZooKeeperSystemController)controller).isNodeOnline[0] = false;
		((ZooKeeperSystemController)controller).isNodeOnline[1] = true;
		((ZooKeeperSystemController)controller).isNodeOnline[2] = true;
		sortedTargetState = controller.sortTargetState(unsortedTargetState.clone());
		if (Arrays.equals(sortedTargetState, expectedSortedTargetState2)) {
			System.out.println("test passed");
		} else {
			System.out.println("test failed. output sortedTargetState was " + Arrays.toString(sortedTargetState));
			System.out.println("We expected:");
			System.out.println("  " + Arrays.toString(expectedSortedTargetState2));
		}

		
		// test if it considers node online status
		((ZooKeeperSystemController)controller).isNodeOnline[0] = true;
		((ZooKeeperSystemController)controller).isNodeOnline[1] = false;
		((ZooKeeperSystemController)controller).isNodeOnline[2] = true;
		sortedTargetState = controller.sortTargetState(unsortedTargetState.clone());
		if (Arrays.equals(sortedTargetState, expectedSortedTargetState1)) {
			System.out.println("test passed");
		} else {
			System.out.println("test failed. output sortedTargetState was " + Arrays.toString(sortedTargetState));
			System.out.println("We expected:");
			System.out.println("  " + Arrays.toString(expectedSortedTargetState1));
		}
	

		// test for larger state change
		unsortedTargetState[1] = 3;
		expectedSortedTargetState1[1] = 0;
		expectedSortedTargetState1[2] = 3;
		sortedTargetState = controller.sortTargetState(unsortedTargetState.clone());
		if (Arrays.equals(sortedTargetState, expectedSortedTargetState1)) { 
			System.out.println("test passed");
		} else {
			System.out.println("test failed. output sortedTargetState was " + Arrays.toString(sortedTargetState));
			System.out.println("We expected:");
			System.out.println("  " + Arrays.toString(expectedSortedTargetState1));
		}
		
		// test if it can raise the flag for the unexpected target state considering the node online status
		//unsortedTargetState[2] = 2;
		//sortedTargetState = controller.sortTargetState(unsortedTargetState.clone());
		//if (sortedTargetState == null) {
		//	System.out.println("test passed");
		//} else {
		//	System.out.println("test failed. should have returned null.");
		//}
	}
	
	public static void main(String[] args) {
		int numNode = 3;
		String workingDir = "/home/ben/project/vcon/zk-sys-ctr-test"; // HARD-CODED
		int n = -1;
		int numTestOptions = 4;
		
		controller = new ZooKeeperSystemController(numNode, workingDir);
		File workingDirFile = new File(workingDir);
		if (!workingDirFile.exists()) {
			if (!workingDirFile.mkdir()) {
				System.err.println("Unable to mkdir " + workingDirFile);
	            System.exit(1);
	        }
		}
		
		Scanner reader = new Scanner(System.in);
		while(true){
			System.out.println("Enter a number (0 - display options ; 1 - test createData ; 2 - test readData ; 3 - writeData ;"
					+ " 4 - test sortTargetState ; 100 - terminate):");
			
    		n = reader.nextInt(); // Scans the next token of the input as an int.
    		if (n == 100) {
    			break;
    		} else if (n > numTestOptions || n < 0) {
    			System.err.println("You should input number 0 to " + numTestOptions);
    		} else if (n == 0) {
    			continue;
    		} else {
				switch (n) {
				case 1:
					testCreateData();
					break;
				case 2:
					testReadData();
					break;
				case 3:
					testWriteData();
					break;
				case 4:
					testSortTargetState();
					break;
				default:
					break;
				}
    		}
		}
		reader.close();
	}

}
