package strata.utils;

//Java program to print all combination of size r in an array of size n
import java.util.LinkedList;

public class Permutation {

	public static LinkedList<LinkedList<Object>> vconCumCombiList = new LinkedList<LinkedList<Object>>();
	static int counter = 0;

	static void vconCombinationUtil(LinkedList<Object> vconList,
			LinkedList<Object> data, int start, int end, int index, int r) {
		// Current combination is ready to be printed, print it
		if (index == r) {
			// for (int j = 0; j < r; j++) {
			// System.out.print(data.get(j).toString() + " ");
			// }
			counter++;
			vconCumCombiList.add((LinkedList<Object>) data.clone());
			// System.out.println("");
			return;
		}

		// replace index with all possible elements. The condition
		// "end-i+1 >= r-index" makes sure that including one element
		// at index will make a combination with remaining elements
		// at remaining positions
		for (int i = start; i <= end && end - i + 1 >= r - index; i++) {
			if (data.size() - 1 < index) {
				data.add(vconList.get(i));
			} else {
				data.set(index, vconList.get(i));
			}
			vconCombinationUtil(vconList, data, i + 1, end, index + 1, r);
		}
	}

	public static void getVConCombination(LinkedList<Object> vconList, int r, int n) {
		vconCumCombiList.clear();

		// A temporary array to store all combination one by one
		LinkedList<Object> data = new LinkedList<Object>();

		// Print all combination using temporary array 'data[]'
		vconCombinationUtil(vconList, data, 0, n - 1, 0, r);
		// System.out.println("There were as many combinations as=" + counter);

		// System.out.println("size of vconCumCombiList=" +
		// vconCumCombiList.size());
		// for (int i = 0; i < vconCumCombiList.size(); i++) {
		// LinkedList<Object> combi = vconCumCombiList.get(i);
		// System.out.print("[");
		// for (int j = 0; j < combi.size(); j++) {
		// System.out.print(combi.get(j).toString() + " && ");
		// }
		// System.out.println("]");
		// }
	}

	/* Driver function to check for above function */
	public static void main(String[] args) {
		int arr[] = { 1, 2, 3, 4, 5 };
		int r = 3;
		int n = arr.length;
		
		LinkedList<Object> vconList = new LinkedList<Object>();
		vconList.add(1);
		vconList.add(2);
		vconList.add(3);
		vconList.add(4);
		vconList.add(5);
		getVConCombination(vconList, r, vconList.size());
		System.out.println("Combination List Begin:");
    	for (LinkedList<Object> vconCombi : Permutation.vconCumCombiList) {
    		System.out.println(">> Combination Begin:");
    		for (Object vconCombiElem: vconCombi) {
    			System.out.println(vconCombiElem.toString());
    		}
    		System.out.println(">> Combination End");
        }
    	System.out.println("Combination List End");
	}
	
}
