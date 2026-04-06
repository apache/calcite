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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import org.apache.commons.text.similarity.LevenshteinDistance;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Objects.requireNonNull;

/**
 * Helpers for {@link SqlNameMatcher}.
 */
public class SqlNameMatchers {

  private static final BaseMatcher CASE_SENSITIVE = new BaseMatcher(true);
  private static final BaseMatcher CASE_INSENSITIVE = new BaseMatcher(false);
  private static final LevenshteinDistance LEVENSHTEIN_DISTANCE =
      LevenshteinDistance.getDefaultInstance();
  private static final JaroWinklerSimilarity JARO_WINKLER_SIMILARITY =
      new JaroWinklerSimilarity();

  private SqlNameMatchers() {}

  /** Returns a name matcher with the given case sensitivity. */
  public static SqlNameMatcher withCaseSensitive(final boolean caseSensitive) {
    return caseSensitive ? CASE_SENSITIVE : CASE_INSENSITIVE;
  }

  /** Creates a name matcher that can suggest corrections to what the user
   * typed. It matches liberally (case-insensitively) and also records the last
   * match. */
  public static SqlNameMatcher liberal() {
    return new LiberalNameMatcher();
  }

  /** Suggestion for the first component of a multi-part object name that failed
   * to resolve. */
  static class NameSuggestion {
    final ImmutableList<String> prefixNames;
    final String name;
    final String suggestion;

    NameSuggestion(List<String> prefixNames, String name, String suggestion) {
      this.prefixNames = ImmutableList.copyOf(prefixNames);
      this.name = requireNonNull(name, "name");
      this.suggestion = requireNonNull(suggestion, "suggestion");
    }
  }

  /** Returns the best near-match suggestions for a name. */
  public static List<String> bestMatches(String name, Iterable<String> candidateNames) {
    return bestMatches(name, candidateNames, true);
  }

  private static List<String> bestMatches(String name, Iterable<String> candidateNames,
      boolean allowDigitOnlyDifference) {
    final String normalizedName = normalize(name);
    // Keep the same thresholds as Ruby's did_you_mean spell checker: Jaro-Winkler
    // broadens typo recall, and the length-scaled Levenshtein limit keeps hints conservative.
    final double similarityThreshold = normalizedName.length() > 3 ? 0.834D : 0.77D;
    final int distanceThreshold = (normalizedName.length() + 3) / 4;
    final List<MatchResult> matches = new ArrayList<>();
    int ordinal = 0;
    for (String candidateName : candidateNames) {
      if (name.equals(candidateName)) {
        ordinal++;
        continue;
      }
      final String normalizedCandidateName = normalize(candidateName);
      if (!isEligibleEditDistanceMatch(name, candidateName,
          normalizedName, normalizedCandidateName, allowDigitOnlyDifference)) {
        ordinal++;
        continue;
      }
      final int distance =
          LEVENSHTEIN_DISTANCE.apply(normalizedCandidateName, normalizedName);
      final double similarity =
          JARO_WINKLER_SIMILARITY.apply(normalizedCandidateName, normalizedName);
      if (distance > distanceThreshold
          && similarity < similarityThreshold) {
        ordinal++;
        continue;
      }
      matches.add(
          new MatchResult(candidateName, normalizedCandidateName,
              similarity, distance, ordinal));
      ordinal++;
    }
    if (matches.isEmpty()) {
      return ImmutableList.of();
    }
    matches.sort(Comparator
        .comparingDouble((MatchResult match) -> match.similarity)
        .reversed()
        .thenComparing(match -> match.candidateName)
        .thenComparingInt(match -> match.ordinal));

    int bestDistance = Integer.MAX_VALUE;
    for (MatchResult match : matches) {
      if (match.distance <= distanceThreshold) {
        bestDistance = Math.min(bestDistance, match.distance);
      }
    }
    if (bestDistance != Integer.MAX_VALUE) {
      final Set<String> corrections = new LinkedHashSet<>();
      for (MatchResult match : matches) {
        if (match.distance == bestDistance) {
          corrections.add(match.candidateName);
        }
      }
      return ImmutableList.copyOf(corrections);
    }
    for (MatchResult match : matches) {
      final int length =
          Math.min(normalizedName.length(), match.normalizedCandidateName.length());
      if (match.distance < length) {
        return ImmutableList.of(match.candidateName);
      }
    }
    return ImmutableList.of();
  }

  private static boolean isEligibleEditDistanceMatch(
      String name, String candidateName, String normalizedName,
      String normalizedCandidateName, boolean allowDigitOnlyDifference) {
    if (normalizedName.length() <= 1 || normalizedCandidateName.length() <= 1) {
      return false;
    }
    if (normalizedCandidateName.equals(normalizedName)) {
      return false;
    }
    if (normalizedCandidateName.length() == normalizedName.length() + 1
        && (normalizedCandidateName.startsWith(normalizedName)
            || normalizedCandidateName.endsWith(normalizedName))) {
      return false;
    }
    if (!name.equals(name.trim()) || !candidateName.equals(candidateName.trim())) {
      if (candidateName.trim().equalsIgnoreCase(name.trim())) {
        return false;
      }
    }
    if (!allowDigitOnlyDifference
        && hasOnlyDigitDifference(normalizedName, normalizedCandidateName)) {
      return false;
    }
    return digitCount(name) == digitCount(candidateName);
  }

  /** Returns the best near-match suggestion for a name. */
  public static @Nullable String bestMatch(String name, Iterable<String> candidateNames) {
    return bestMatch(name, candidateNames, true);
  }

  static @Nullable String bestObjectMatch(String name, Iterable<String> candidateNames) {
    return bestMatch(name, candidateNames, false);
  }

  private static @Nullable String bestMatch(String name, Iterable<String> candidateNames,
      boolean allowDigitOnlyDifference) {
    final List<String> matches = bestMatches(name, candidateNames,
        allowDigitOnlyDifference);
    return matches.isEmpty() ? null : matches.get(0);
  }

  /** Returns the best near-match suggestion for the first unresolved component
   * of a multi-part catalog object name. */
  static @Nullable NameSuggestion bestObjectName(
      SqlValidatorCatalogReader catalogReader, List<String> names) {
    final Iterable<List<String>> schemaPaths;
    if (names.size() > 1 && catalogReader.getSchemaPaths().size() > 1) {
      schemaPaths = Util.skip(catalogReader.getSchemaPaths());
    } else {
      schemaPaths = catalogReader.getSchemaPaths();
    }
    for (List<String> schemaPath : schemaPaths) {
      final @Nullable NameSuggestion suggestion =
          bestObjectName(catalogReader, schemaPath, names);
      if (suggestion != null) {
        return suggestion;
      }
    }
    final @Nullable NameSuggestion suggestion =
        bestObjectName(catalogReader, ImmutableList.of(), names);
    if (suggestion != null) {
      return suggestion;
    }
    if (names.size() > 1) {
      final Set<String> candidateNames =
          new LinkedHashSet<>(directObjectNames(catalogReader, ImmutableList.of()));
      for (List<String> schemaPath : catalogReader.getSchemaPaths()) {
        if (!schemaPath.isEmpty()) {
          candidateNames.add(Util.last(schemaPath));
        }
      }
      final @Nullable String firstSuggestion =
          bestObjectMatch(names.get(0), candidateNames);
      if (firstSuggestion != null) {
        return new NameSuggestion(ImmutableList.of(), names.get(0), firstSuggestion);
      }
    }
    return null;
  }

  private static @Nullable NameSuggestion bestObjectName(
      SqlValidatorCatalogReader catalogReader, List<String> schemaPath,
      List<String> names) {
    List<String> objectPath = ImmutableList.copyOf(schemaPath);
    List<String> prefixNames = ImmutableList.of();
    for (String name : names) {
      final List<String> candidateNames = directObjectNames(catalogReader, objectPath);
      final @Nullable String exactMatch = exactMatch(name, candidateNames);
      if (exactMatch != null) {
        objectPath =
            ImmutableList.<String>builder().addAll(objectPath).add(exactMatch).build();
        prefixNames =
            ImmutableList.<String>builder().addAll(prefixNames).add(exactMatch).build();
        continue;
      }
      final @Nullable String suggestion = bestObjectMatch(name, candidateNames);
      return suggestion == null ? null
          : new NameSuggestion(prefixNames, name, suggestion);
    }
    return null;
  }

  static List<String> directObjectNames(
      SqlValidatorCatalogReader catalogReader, List<String> prefixNames) {
    final List<String> names = new ArrayList<>();
    final int expectedDepth = prefixNames.size() + 1;
    for (SqlMoniker moniker : catalogReader.getAllSchemaObjectNames(prefixNames)) {
      switch (moniker.getType()) {
      case CATALOG:
      case SCHEMA:
      case TABLE:
      case VIEW:
        final List<String> objectNames = moniker.getFullyQualifiedNames();
        if (objectNames.size() == expectedDepth) {
          names.add(Util.last(objectNames));
        }
        break;
      default:
        break;
      }
    }
    return names;
  }

  private static @Nullable String exactMatch(String name, Iterable<String> candidateNames) {
    for (String candidateName : candidateNames) {
      if (candidateName.equals(name)) {
        return candidateName;
      }
    }
    return null;
  }

  private static String normalize(String name) {
    return name.toLowerCase(Locale.ROOT);
  }

  private static int digitCount(String name) {
    int digitCount = 0;
    for (int i = 0; i < name.length(); i++) {
      if (Character.isDigit(name.charAt(i))) {
        digitCount++;
      }
    }
    return digitCount;
  }

  private static boolean hasOnlyDigitDifference(String name, String candidateName) {
    if (name.length() != candidateName.length()) {
      return false;
    }
    boolean sawDigitDifference = false;
    for (int i = 0; i < name.length(); i++) {
      final char c1 = name.charAt(i);
      final char c2 = candidateName.charAt(i);
      if (c1 == c2) {
        continue;
      }
      if (!Character.isDigit(c1) || !Character.isDigit(c2)) {
        return false;
      }
      sawDigitDifference = true;
    }
    return sawDigitDifference;
  }

  /** Ranked candidate retained while sorting edit-distance suggestions. */
  private static class MatchResult {
    private final String candidateName;
    private final String normalizedCandidateName;
    private final double similarity;
    private final int distance;
    private final int ordinal;

    MatchResult(String candidateName, String normalizedCandidateName,
        double similarity, int distance, int ordinal) {
      this.candidateName = requireNonNull(candidateName, "candidateName");
      this.normalizedCandidateName =
          requireNonNull(normalizedCandidateName, "normalizedCandidateName");
      this.similarity = similarity;
      this.distance = distance;
      this.ordinal = ordinal;
    }
  }

  /** Partial implementation of {@link SqlNameMatcher}. */
  private static class BaseMatcher implements SqlNameMatcher {
    private final boolean caseSensitive;

    BaseMatcher(boolean caseSensitive) {
      this.caseSensitive = caseSensitive;
    }

    @Override public boolean isCaseSensitive() {
      return caseSensitive;
    }

    @Override public boolean matches(String string, String name) {
      return caseSensitive ? string.equals(name)
          : string.equalsIgnoreCase(name);
    }

    protected boolean listMatches(List<String> list0, List<String> list1) {
      if (list0.size() != list1.size()) {
        return false;
      }
      for (int i = 0; i < list0.size(); i++) {
        String s0 = list0.get(i);
        String s1 = list1.get(i);
        if (!matches(s0, s1)) {
          return false;
        }
      }
      return true;
    }

    @Override public <K extends List<String>, V> @Nullable V get(Map<K, V> map,
        List<String> prefixNames, List<String> names) {
      final List<String> key = concat(prefixNames, names);
      if (caseSensitive) {
        //noinspection SuspiciousMethodCalls
        return map.get(key);
      }
      for (Map.Entry<K, V> entry : map.entrySet()) {
        if (listMatches(key, entry.getKey())) {
          matched(prefixNames, entry.getKey());
          return entry.getValue();
        }
      }
      return null;
    }

    private static List<String> concat(List<String> prefixNames, List<String> names) {
      if (prefixNames.isEmpty()) {
        return names;
      } else {
        return ImmutableList.<String>builder().addAll(prefixNames).addAll(names)
            .build();
      }
    }

    protected void matched(List<String> prefixNames, List<String> names) {
    }

    protected List<String> bestMatch() {
      throw new UnsupportedOperationException();
    }

    @Override public String bestString() {
      return SqlIdentifier.getString(bestMatch());
    }

    @Override public @Nullable RelDataTypeField field(RelDataType rowType, String fieldName) {
      return rowType.getField(fieldName, caseSensitive, false);
    }

    @Override public int frequency(Iterable<String> names, String name) {
      int n = 0;
      for (String s : names) {
        if (matches(s, name)) {
          ++n;
        }
      }
      return n;
    }

    @Override public Set<String> createSet() {
      return isCaseSensitive()
          ? new LinkedHashSet<>()
          : new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    }
  }

  /** Matcher that remembers the requests that were made of it. */
  private static class LiberalNameMatcher extends BaseMatcher {
    @Nullable List<String> matchedNames;

    LiberalNameMatcher() {
      super(false);
    }

    @Override protected boolean listMatches(List<String> list0,
        List<String> list1) {
      final boolean b = super.listMatches(list0, list1);
      if (b) {
        matchedNames = ImmutableList.copyOf(list1);
      }
      return b;
    }

    @Override protected void matched(List<String> prefixNames,
        List<String> names) {
      matchedNames =
          ImmutableList.copyOf(
              Util.startsWith(names, prefixNames)
                  ? Util.skip(names, prefixNames.size())
                  : names);
    }

    @Override public List<String> bestMatch() {
      return requireNonNull(matchedNames, "matchedNames");
    }
  }
}
