package com.cloudera.bigdata.analysis.core; /**
 * Created by Michelle on 12/20/14.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Frequence {
    public static void main(String args[]) {
        // create array list object
        List arrlist = new ArrayList();

        // populate the list
        arrlist.add("A");
        arrlist.add("B");
        arrlist.add("C");
        arrlist.add("C");
        arrlist.add("C");

        // check frequensy of 'C'
        int freq = Collections.frequency(arrlist, "B");

        System.out.println("Frequency of 'B' is: "+freq);
    }
}
