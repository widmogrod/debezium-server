/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.cratedb.schema;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation of the schema diffing
 *
 * @author Gabriel Habryn
 */
public class Diff {
    public record ChangeSet(Schema.I value, Set<Change> changes) {
        public sealed interface Change permits Added, Nested, Positional, Removed, Unchanged { }
        public record Unchanged(Schema.I value) implements Change { }
        public record Added(Schema.I type) implements Change { }
        public record Removed(Schema.I type) implements Change { }
        public record Positional(Object keyOrIndex, Change type) implements Change { }
        public record Nested(ChangeSet changes) implements Change { }
    }

    public static ChangeSet compare(Schema.I a, Schema.I b) {
        Set<ChangeSet.Change> changes = new LinkedHashSet<>();

        if (a instanceof Schema.Array aArray && b instanceof Schema.Array bArray) {
            var nestedChangeSet = compare(aArray.innerType(), bArray.innerType());
            if (!nestedChangeSet.changes().isEmpty()) {
                changes.add(new ChangeSet.Nested(nestedChangeSet));
            }
            else {
                changes.add(new ChangeSet.Unchanged(a));
            }
        }
        else if (a instanceof Schema.Bit aBit && b instanceof Schema.Bit bBit) {
            if (!Objects.equals(aBit.size(), bBit.size())) {
                changes.add(new ChangeSet.Removed(a));
                changes.add(new ChangeSet.Added(b));
            }
            else {
                changes.add(new ChangeSet.Unchanged(a));
            }
        }
        else if (a instanceof Schema.Coli aColi && b instanceof Schema.Coli bColi) {
            Set<Schema.I> added = bColi.set().stream().filter(e -> !aColi.set().contains(e)).collect(Collectors.toSet());
            Set<Schema.I> removed = aColi.set().stream().filter(e -> !bColi.set().contains(e)).collect(Collectors.toSet());
            Set<Schema.I> unchanged = aColi.set().stream().filter(bColi.set()::contains).collect(Collectors.toSet());

            for (Schema.I add : added) {
                changes.add(new ChangeSet.Added(add));
            }
            for (Schema.I remove : removed) {
                changes.add(new ChangeSet.Removed(remove));
            }
            for (Schema.I unch : unchanged) {
                changes.add(new ChangeSet.Unchanged(unch));
            }
        }
        else if (a instanceof Schema.Dict aDict && b instanceof Schema.Dict bDict) {
            Set<Object> allKeys = new LinkedHashSet<>(aDict.fields().keySet());
            allKeys.addAll(bDict.fields().keySet());

            for (Object key : allKeys) {
                Schema.I aValue = aDict.fields().get(key);
                Schema.I bValue = bDict.fields().get(key);

                if (aValue == null) {
                    changes.add(new ChangeSet.Positional(key, new ChangeSet.Added(bValue)));
                }
                else if (bValue == null) {
                    changes.add(new ChangeSet.Positional(key, new ChangeSet.Removed(aValue)));
                }
                else {
                    var nestedChangeSet = compare(aValue, bValue);
                    if (!nestedChangeSet.changes().isEmpty()) {
                        changes.add(new ChangeSet.Positional(key, new ChangeSet.Nested(nestedChangeSet)));
                    }
                    else {
                        changes.add(new ChangeSet.Positional(key, new ChangeSet.Unchanged(aValue)));
                    }
                }
            }
        }
        else if (a.equals(b)) {
            changes.add(new ChangeSet.Unchanged(a));
        }
        else {
            changes.add(new ChangeSet.Removed(a));
            changes.add(new ChangeSet.Added(b));
        }

        return new ChangeSet(a, changes);
    }

    public static String prettyPrint(ChangeSet changeSet) {
        var result = new StringBuilder();

        result.append(Evolution.typeSuffix(changeSet.value())).append(" of \n");
        for (ChangeSet.Change change : changeSet.changes()) {
            result.append(prettyPrint(change)).append("\n");
        }

        return result.toString().trim();
    }

    private static String prettyPrint(ChangeSet.Change change) {
        var result = new StringBuilder();

        switch (change) {
            case ChangeSet.Added(Schema.I type) ->
                    result.append(ANSI_GREEN).append("  + ").append(type).append(ANSI_RESET);
            case ChangeSet.Removed(Schema.I type) ->
                    result.append(ANSI_RED).append("  - ").append(type).append(ANSI_RESET);
            case ChangeSet.Unchanged(Schema.I type) -> result.append("    ").append(type);
            case ChangeSet.Positional(Object keyOrIndex, ChangeSet.Change type) -> {
                var nested = prettyPrint(type);
                result.append("  > ").append(keyOrIndex).append(":\n").append(padLeft(1, nested));
            }
            case ChangeSet.Nested(ChangeSet changes) -> {
                var nestedResult = prettyPrint(changes);
                result.append(nestedResult);
            }
        }

        return result.toString();
    }

    private static String padLeft(Integer level, String text) {
        var indent = "    " .repeat(level);
        return text.lines()
                .map(line -> indent + line)
                .collect(Collectors.joining("\n"));
    }

    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_GREEN = "\u001B[32m";
}
