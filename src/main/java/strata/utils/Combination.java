package strata.utils;

import java.util.*;


public class Combination {
  public static List<List<Integer>> combinations(List<Integer> list, int maxLength) {
    return combinations(list, maxLength, new ArrayList(), new ArrayList());
  }

  private static List<List<Integer>> combinations(List<Integer> list, int length, List<Integer> current, List<List<Integer>> result) {
    if (length == 0) {
      List<List<Integer>> newResult =  new ArrayList<>(result);
      newResult.add(current);
      return newResult;
    }

    List<List<List<Integer>>> res3 = new ArrayList<>();
    for (Integer i : list) {
      List<Integer> newCurrent = new ArrayList<>(current);
      newCurrent.add(i);
      res3.add(combinations(list, length - 1, newCurrent, result));
    }

    List<List<Integer>> res2 = new ArrayList<>();
    for (List<List<Integer>> lst : res3) {
      res2.addAll(lst);
    }
    return res2;
  }

  public static void printCombinations(List<Integer> list, int maxLength) {
    List<List<Integer>> combs = combinations(list, maxLength);
    for (List<Integer> lst : combs) {
      String line = "";
      for (Integer i : lst) {
        line += i;
      }
      System.out.println(line);
    }
  }
 
  public static void main(String[] args) {
    List<Integer> l = Arrays.asList(0, 1, 2);
    printCombinations(l, 5);
  }
}