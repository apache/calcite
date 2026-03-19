# Plan: Tidy up SubPlanCommand

## Analysis of SubPlanCommand tidying options

### Current state

`SubPlanCommand` is a 230-line nested static class inside `QuidemTest.java`. Its `execute()` method handles three things in one blob:

1. **Token parsing** — a large if-else chain over a flat comma-separated string
2. **Factory/converter configuration** — building `SqlTestFactory` from config flags
3. **Rule application** — building a HepPlanner and firing rules

The arg string has **three different kinds of tokens** that are visually indistinguishable without knowing the rules:
- Lowercase config: `aggregateUnique=true`, `bloat=5`, `trim=true`
- CoreRules fields (ALL_CAPS): `AGGREGATE_PROJECT_MERGE`
- Other-class fields (Mixed.CAPS): `PruneEmptyRules.PROJECT_INSTANCE`
- Special pseudo-configs that *are really rule configs*: `functionsToReduce=AVG|SUM`, `throwIfNotUnique=false`, `withinDistinctOnly=true`

The last group is the real design smell: they look like global config options but actually configure a specific rule, and that coupling is invisible in the arg string.

---

### Idea 1: Top-level class

`SubPlanCommand` is already `static` — it has zero access to `QuidemTest` instance state. The only public dependency it calls back into is `getCoreRule(name)`, which is already `public static`. Moving it to its own file (`SubPlanCommand.java`) would:

- Make the file easier to navigate (QuidemTest is already ~1000 lines)
- Make `SubPlanCommand` testable in isolation
- Make it clearer that it's a first-class Quidem extension, not an implementation detail

`ExplainValidatedCommand` is much smaller (50 lines) and could stay nested or also be extracted.

**Verdict: worthwhile, low risk.** Purely mechanical move.

---

### Idea 2: Rule-parameterised syntax

The core idea: rule-specific config should live *inside* the rule token rather than as a global flag preceding it.

**`functionsToReduce=X` → `AggregateReduceFunctionsRule(functions=X)`**

Before:
```
!transform "functionsToReduce=AVG|SUM"
!transform "functionsToReduce=NONE"
```

After:
```
!transform "AggregateReduceFunctionsRule(functions=AVG|SUM)"
!transform "AggregateReduceFunctionsRule(functions=NONE)"
```

Benefits:
- The rule class is now **named** — a reader knows exactly which rule is being constructed
- `functionsToReduce` was a private language; `AggregateReduceFunctionsRule(functions=...)` mirrors the Java API (`Config.withFunctionsToReduce(...)`)
- `withinDistinctOnly=true` also configures `AggregateReduceFunctionsRule` — it could move here as `AggregateReduceFunctionsRule(withinDistinctOnly=true)`

**`throwIfNotUnique=false, AGGREGATE_EXPAND_WITHIN_DISTINCT` → `AGGREGATE_EXPAND_WITHIN_DISTINCT(throwIfNotUnique=false)`**

Before:
```
!transform "throwIfNotUnique=false, AGGREGATE_REDUCE_FUNCTIONS, AGGREGATE_EXPAND_WITHIN_DISTINCT"
```

After:
```
!transform "AGGREGATE_REDUCE_FUNCTIONS, AGGREGATE_EXPAND_WITHIN_DISTINCT(throwIfNotUnique=false)"
```

The connection between the flag and the rule it modifies is now **collocated**.

#### Parsing implications

Currently tokens are just `split(",")` then classify by first character. The new syntax requires recognising `NAME(key=value, ...)` within a token. This would need either:
- A simple regex like `(\w+(?:\.\w+)?)\(([^)]*)\)?` for the parameterised form
- Or split at `(` before the comma-split (but commas can appear inside parens — need a proper split)

The change to the parser is modest because parameterised tokens still start with a letter; only the handling of that one token changes.

#### What stays as global config

Tokens that affect the *converter* or *planner globally* (not a single rule) should remain at the top level:

| Token | Reason to keep global |
|---|---|
| `aggregateUnique=true` | Affects RelBuilder during SQL→Rel |
| `bloat=N` | Affects RelBuilder globally |
| `decorrelate=true` | Affects converter, not a rule |
| `expand=true` | Affects converter |
| `trim=true` | Affects converter |
| `relBuilderSimplify=false` | Affects RelBuilder thread hook |
| `simplifyValues=false` | Affects RelBuilder globally |
| `bottomUp=true` | HepPlanner match order, affects all rules |
| `subQueryRules` | Macro for three pre-rules in a separate planner pass |
| `lateDecorrelate=true` | Post-rule decorrelation step |
| `connectionConfig=true` | Planner context |
| `inSubQueryThreshold=N` | Converter config |
| `operatorTable=BIG_QUERY` | Factory config |

So the list of candidates for rule-parameterised syntax is quite small — really just:
- `functionsToReduce=X` and `withinDistinctOnly=true` (both configure `AggregateReduceFunctionsRule`)
- `throwIfNotUnique=false` (configures `AGGREGATE_EXPAND_WITHIN_DISTINCT`)

That's 12 `.iq` occurrences total — small enough that migration is safe.

---

### Idea 3: Minor structural cleanup of `execute()`

Separate from the two main ideas, `execute()` could be factored without changing the `.iq` syntax at all:

- Extract `parseArgs(String args) → Config` (a plain-data record)
- Extract `buildRelNode(Config, SqlCommand) → RelNode`
- Extract `applyRules(Config, RelNode) → RelNode`

This makes each part independently readable and testable, without any change to `.iq` files.

---

### Recommendation

All three ideas are complementary. Suggested order:

1. **Structural split** of `execute()` into helpers — zero visible impact, makes subsequent changes easier
2. **Rule-parameterised syntax** — improves readability of `.iq` files most visibly; small migration
3. **Top-level class** — purely mechanical, can be a separate commit

The rule-parameterised syntax is the highest-value change. The syntax `RULE_CLASS(param=val)` is self-documenting in a way the current global flags are not, and it parallels how the Java API is written (`Rule.Config.DEFAULT.withParam(val).toRule()`).
