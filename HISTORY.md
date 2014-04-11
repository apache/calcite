# Linq4j release history and change log

For a full list of releases, see <a href="https://github.com/julianhyde/linq4j/releases">github</a>.

## <a href="https://github.com/julianhyde/linq4j/releases/tag/linq4j-0.2">0.2</a> / 2014-04-11

* Fix <a href="https://github.com/julianhyde/linq4j/issues/15">#15</a>,
  "`Expressions.ifThenElse` does not work".
* Use `HashMap` for searching of declarations to reuse; consider both
  `optimizing` and `optimize` flags when reusing.
* Implement `equals` and `hashCode` for expressions. Hash codes for
  complex expressions are cached into a field of the expression.
* Add example, `com.example.Linq4jExample`.
* Fix optimizing away parameter declarations in assignment target.
* Support Windows path names in checkstyle-suppresions.
* Support `Statement.toString` via `ExpressionWriter`.
* Use `AtomicInteger` for naming of `ParameterExpression`s to avoid
  conflicts in multithreaded usage
* Cleanup: use `Functions.adapt` rather than `new AbstractList`
* Add `NOTICE` and `LICENSE` files in generated JAR file.
* Optimize `select()` if selector is identity.
* Enable checkstyle.

## <a href="https://github.com/julianhyde/linq4j/releases/tag/linq4j-0.1.13">0.1.13</a> / 2014-01-20

* Remove spurious "null" generated when converting expression to string.
* Allow a field declaration to not have an initializer.
* Add `Primitive.defaultValue`.
* Enable `oraclejdk8` in <a href="https://travis-ci.org/julianhyde/linq4j">Travis CI</a>.

## <a href="https://github.com/julianhyde/linq4j/releases/tag/linq4j-0.1.12">0.1.12</a> / 2013-12-07

* Add release notes.
* Fix implementation of `Enumerable.asEnumerable` in
  `DefaultQueryable` (inherited by most classes that implement
  `Queryable`).

## <a href="https://github.com/julianhyde/linq4j/releases/tag/linq4j-0.1.11">0.1.11</a> / 2013-11-06

* Lots of good stuff that this margin is too small to contain.
