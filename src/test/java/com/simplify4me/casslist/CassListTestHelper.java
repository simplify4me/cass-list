package com.simplify4me.casslist;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 *
 */

public class CassListTestHelper {

    static Set<String> writeABunchOfValues(CassList cassList, int numValues) throws Exception {
        Set<String> set = new HashSet<String>();
       Random random = new Random();
        for (int index = 0; index < numValues; index++) {
            final String value = String.valueOf(index);
            set.add(value);
            cassList.add(value);
            Thread.sleep(random.nextInt(1000));
        }
        System.out.println("done writing");
        return set;
    }
}
