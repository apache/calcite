/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test.schemata.pets;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.Vector;

/**
 * The main purpose of the schema is to test how ReflectiveSchema works with different
 * implementations of Collections. For ease of testing, each table is filled with the
 * same data.
 *
 * <p>Here is the SQL to create example table:
 *
 * <blockquote>
 * <pre>
 *
 * CREATE TABLE "cats_array_list" (
 *   "name" VARCHAR(10) NOT NULL,
 *   "age" INTEGER NOT NUL);
 * INSERT INTO "cats_array_list" VALUES ('Simba', 2);
 * INSERT INTO "cats_array_list" VALUES ('Luna', 1);
 * INSERT INTO "cats_array_list" VALUES ('Bella', 3);
 *
 * </pre>
 * </blockquote>
 *
 * <p>The same script applied for all tables with names:
 * <ul>
 *     <li>cats_array_list</li>
 *     <li>cats_linked_list</li>
 *     <li>cats_stack</li>
 *     <li>cats_vector</li>
 *     <li>cats_priority_queue</li>
 *     <li>cats_array_dequeue</li>
 *     <li>cats_hash_set</li>
 *     <li>cats_linked_hash_set</li>
 *     <li>cats_tree_set</li>
 * </ul>
 */
public class PetCollectionsSchema {

  @Override public String toString() {
    return "PetCollectionsSchema";
  }

  public final List<Pet> cats_array_list = getCatsArrayList();
  public final List<Pet> cats_linked_list = getCatsLinkedList();
  public final Stack<Pet> cats_stack = getCatsStack();
  public final Vector<Pet> cats_vector = getCatsVector();
  public final Queue<Pet> cats_priority_queue = getCatsPriorityQueue();
  public final Queue<Pet> cats_array_dequeue = getCatsArrayDeQueue();
  public final Set<Pet> cats_hash_set = getCatsHashSet();
  public final Set<Pet> cats_linked_hash_set = getCatsLinkedHashSet();
  public final Set<Pet> cats_tree_set = getCatsTreeSet();


  private List<Pet> getCatsArrayList() {
    final List<Pet> cats = new ArrayList<>();
    fillCats(cats);
    return cats;
  }

  private List<Pet> getCatsLinkedList() {
    final List<Pet> cats = new LinkedList<>();
    fillCats(cats);
    return cats;
  }

  private Stack<Pet> getCatsStack() {
    final Stack<Pet> cats = new Stack<>();
    fillCats(cats);
    return cats;
  }

  private Vector<Pet> getCatsVector() {
    final Vector<Pet> cats = new Vector<>();
    fillCats(cats);
    return cats;
  }

  private Queue<Pet> getCatsPriorityQueue() {
    final PriorityQueue<Pet> cats = new PriorityQueue<>();
    fillCats(cats);
    return cats;
  }

  private Queue<Pet> getCatsArrayDeQueue() {
    final ArrayDeque<Pet> cats = new ArrayDeque<>();
    fillCats(cats);
    return cats;
  }

  private Set<Pet> getCatsHashSet() {
    final HashSet<Pet> cats = new HashSet<>();
    fillCats(cats);
    return cats;
  }

  private Set<Pet> getCatsLinkedHashSet() {
    final LinkedHashSet<Pet> cats = new LinkedHashSet<>();
    fillCats(cats);
    return cats;
  }

  private Set<Pet> getCatsTreeSet() {
    final TreeSet<Pet> cats = new TreeSet<>();
    fillCats(cats);
    return cats;
  }

  private void fillCats(Collection<Pet> pets) {
    pets.add(new Pet("Simba", 2));
    pets.add(new Pet("Luna", 1));
    pets.add(new Pet("Bella", 3));
  }
}
