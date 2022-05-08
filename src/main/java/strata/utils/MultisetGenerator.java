package strata.utils;

import java.util.ArrayList;
import java.util.List;


/**
 * A multiset (or bag) is a generalization of the concept of a set that,
 * unlike a set, allows multiple instances of the multiset's elements. 
 * 
 * For example, {a, a, b} and {a, b} are different multisets although 
 * they are the same set. However, order does not matter, so {a, a, b} 
 * and {a, b, a} are the same multiset.
 * 
 * The number of times an element belongs to the multiset is the 
 * multiplicity of that member. 
 * The total number of elements in a multiset, including repeated 
 * memberships, is the cardinality of the multiset. 
 * 
 * For example, in the multiset {a, a, b, b, b, c} the multiplicities of 
 * the members a, b, and c are respectively 2, 3, and 1, and the 
 * cardinality of the multiset is 6.
 *  
 * Source: https://en.wikipedia.org/wiki/Multiset
 * 
 * Given the number of items and the cardinality of the desired multisets, 
 * this class allows to generate all possible multisets.
 * 
 * @author Matteo Nardelli
 *
 */
public class MultisetGenerator {
	
	/**
	 * Computes all possible multisets of <b>numItems</b> items
	 * with cardinality <b>cardinality</b>.
	 * 
	 * @param 	numItems
	 * @param 	cardinality
	 * @return	all possible multisets
	 */
	public static List<int[]> generateMultisets(int numItems, int cardinality){
		
		List<int[]> multisets = new ArrayList<int[]>();

		/* Initialize indexes and boundaries */
		int j = cardinality;
		int j_1 = cardinality;
		int q = cardinality;
		int maxItem = numItems - 1;
		
		/* Initialize the first multiset as [0 ... 0] */
		int[] a = new int[cardinality];
		for (int i = 0; i < cardinality; i++){
			a[i] = 0;
		}

		/* Generate multisets */
		while (true){
						
			/* Emit the computed multiset */
			multisets.add(a.clone());
			
			/* Compute next multiset */
			j = cardinality - 1;
			

			/* Find element to update (increment) */
			while (j > -1 && a[j] == maxItem){
				j--;
			}
			if (j < 0)
				break;
			
			/* Update the multiset elements */
			j_1 = j;
			
			while (j_1 <= cardinality - 1){
				
				a[j_1] = a[j_1] + 1;
				q = j_1;

				while (q < cardinality - 1){
					a[q + 1] = a[q]; 
					q += 1;
				}
				
				q += 1;
				j_1 = q;
			
			}
		}

		return multisets;
	}
	
	public static void main(String[] args){
		
		System.out.println("Multiset Generator");

		/* DEMO 1: Just print all generated multisets */
		System.out.println("Generate multisets of cardinality 2 on 3 elements");
		List<int[]> mss = generateMultisets(3, 2);
		for (int[] ms : mss){
			for (int i = 0; i < ms.length; i++)
				System.out.print(ms[i]+" ");
			System.out.println();
		}


		/* DEMO 2: Use the computed multisets as indexes to generate 
		 * multisets of objects */
		System.out.println("\nGenerate multisets of cardinality 2 on 4 objects (strings)");
		mss = generateMultisets(4, 2);
		String[] strings = {"a", "b", "c", "d"};
		
		for (int[] ms : mss){
			for (int i = 0; i < ms.length; i++)
				System.out.print(strings[ms[i]]+" ");
			System.out.println();
		}
		
		/* DEMO BK 1: Just print all generated multisets */
		System.out.println("Generate multisets of cardinality 3 on 7 elements");
		mss = generateMultisets(7, 3);
		System.out.println("Total number of multiset=" + mss.size());
		for (int[] ms : mss){
			for (int i = 0; i < ms.length; i++)
				System.out.print(ms[i]+" ");
			System.out.println();
		}
	}
	
}
