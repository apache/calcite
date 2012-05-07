package net.hydromatic.optiq;

import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: jhyde
 * Date: 4/23/12
 * Time: 11:29 AM
 * To change this template use File | Settings | File Templates.
 */
public class Demo {
    public static void main(String[] args) {
        ArrayList<String> names = new ArrayList<String>();
        names.add("John");
        names.add("Paul");
        names.add("George");
        names.add("Ringo");

        Iterable<String> nameIterable = names;

        for (String name : nameIterable) {
            System.out.println(name);
        }
    }
}
