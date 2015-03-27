/*
 * The MIT License
 *
 * Copyright 2015 Matteo Lissandrini <ml@disi.unitn.eu>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.unitn.disi.db.exemplar.core.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class CollectionUtils {

    public static <A, B> String mapToString(Map<A, B> m) {
        StringBuilder sb = new StringBuilder();
        Set<A> keys = m.keySet();
        sb.append("{");
        for (A key : keys) {
            sb.append("(").append(key).append(",").append(m.get(key)).append(")");
        }
        sb.append("}");
        return sb.toString();
    }

    public static <T> int differenceSize(Set<T> set1, Set<T> set2) {
        int count = set1.size();
        for (T t : set1) {
            if (set2.contains(t)) {
                count--;
            }
        }
        return count;
    }

    public static <T> int intersectionSize(Set<T> checkCollection, Collection<T> inputCollection) {
        int count = 0;
        for (T t : inputCollection) {
            if (checkCollection.contains(t)) {
                count++;
            }
        }
        return count;
    }

    public static <T> boolean intersectionNotEmpty(Set<T> checkCollection, Collection<T> inputCollection) {
        for (T t : inputCollection) {
            if (checkCollection.contains(t)) {
                return true;
            }
        }
        return false;
    }

    public static <T> Set<T> intersect(Set<T> set1, Set<T> set2) {
        Set<T> a;
        Set<T> b;
        Set<T> intersection = new HashSet<>();
        if (set1.size() <= set2.size()) {
            a = set1;
            b = set2;
        } else {
            a = set2;
            b = set1;
        }
        for (T e : a) {
            if (b.contains(e)) {
                intersection.add(e);
            }
        }
        return intersection;
    }

    public static <T> List<T> intersect(List<T> list1, Set<T> set2) {
        List<T> intersection = new ArrayList<>(set2.size());

        for (T e : list1) {
            if (set2.contains(e)) {
                intersection.add(e);
            }
        }
        return intersection;
    }

    public static <T> List<T> intersect(List<T> list1, List<T> list2) {
        List<T> a;
        List<T> b;
        List<T> intersection;
        if (list1.size() <= list2.size()) {
            a = list1;
            b = list2;
            intersection = new ArrayList<>(list1.size());
        } else {
            a = list2;
            b = list1;
            intersection = new ArrayList<>(list2.size());
        }
        for (T e : a) {
            if (b.contains(e)) {
                intersection.add(e);
            }
        }
        return intersection;
    }

}
