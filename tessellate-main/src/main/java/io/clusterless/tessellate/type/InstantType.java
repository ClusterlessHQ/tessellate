/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.type;

import cascading.CascadingException;
import cascading.operation.SerFunction;
import cascading.tuple.type.CoercibleType;
import cascading.util.Util;
import com.fasterxml.jackson.core.TreeNode;
import io.clusterless.tessellate.temporal.IntervalUnits;
import io.clusterless.tessellate.util.JSONUtil;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Date;
import java.util.function.Supplier;

/**
 * When transforming between primitive and Instant types, attempts to retain the unit precision
 */
public final class InstantType implements CoercibleType<Instant> {
    public static final Long ZERO = 0L;
    public static final InstantType ISO_MILLIS = new InstantType(ChronoUnit.MILLIS, DateTimeFormatter.ISO_INSTANT);
    public static final InstantType ISO_MICROS = new InstantType(ChronoUnit.MICROS, DateTimeFormatter.ISO_INSTANT);
    private final ChronoUnit unit;
    private final Supplier<DateTimeFormatter> dateTimeFormatter;

    // transforms
    private SerFunction<Long, Instant> longCanonical;
    private SerFunction<BigDecimal, Instant> bigCanonical;
    private SerFunction<Instant, Long> longCoerce;

    private InstantType() {
        this(ChronoUnit.MILLIS, DateTimeFormatter.ISO_INSTANT);
    }

    public InstantType(TemporalUnit unit) {
        this(!(unit instanceof ChronoUnit) ? ChronoUnit.MILLIS : unit, IntervalUnits.formatter(unit));
    }

    public InstantType(Supplier<DateTimeFormatter> dateTimeFormatter) {
        this(ChronoUnit.MILLIS, dateTimeFormatter);
    }

    public InstantType(TemporalUnit unit, DateTimeFormatter dateTimeFormatter) {
        this(unit, () -> dateTimeFormatter);
    }

    public InstantType(TemporalUnit unit, Supplier<DateTimeFormatter> dateTimeFormatter) {
        this.unit = !(unit instanceof ChronoUnit) ? ChronoUnit.MILLIS : (ChronoUnit) unit;
        this.dateTimeFormatter = dateTimeFormatter;

        initTransforms();
    }

    protected void initTransforms() {
        switch (unit) {
            case SECONDS:
                longCanonical = Instant::ofEpochSecond;
                bigCanonical = b -> Instant.ofEpochSecond(b.longValue(), b.remainder(BigDecimal.ONE).setScale(9, RoundingMode.HALF_UP).unscaledValue().longValue());
                longCoerce = Instant::getEpochSecond;
                return;
            case MILLIS:
                longCanonical = Instant::ofEpochMilli;
                bigCanonical = b -> {
                    BigDecimal value = b.movePointLeft(3);
                    return Instant.ofEpochSecond(value.longValue(), value.remainder(BigDecimal.ONE).setScale(9, RoundingMode.HALF_UP).unscaledValue().longValue());
                };
                longCoerce = Instant::toEpochMilli;
                return;
            case MICROS:
                longCanonical = l -> Instant.ofEpochSecond(Math.floorDiv(l, 1_000_000), Math.floorMod(l, 1_000_000) * 1_000L);
                bigCanonical = b -> {
                    BigDecimal value = b.movePointLeft(6);
                    return Instant.ofEpochSecond(value.longValue(), value.remainder(BigDecimal.ONE).setScale(9, RoundingMode.HALF_UP).unscaledValue().longValue());
                };
                longCoerce = i -> Math.addExact(Math.multiplyExact(i.getEpochSecond(), 1_000_000), i.getLong(ChronoField.MICRO_OF_SECOND));
                return;
            case NANOS:
                longCanonical = l -> Instant.ofEpochSecond(Math.floorDiv(l, 1_000_000_000), Math.floorMod(l, 1_000_000_000));
                bigCanonical = b -> Instant.ofEpochSecond(0, b.longValue());
                longCoerce = i -> Math.addExact(Math.multiplyExact(i.getEpochSecond(), 1_000_000_000), i.getLong(ChronoField.NANO_OF_SECOND));
                return;
            default:
                throw new IllegalArgumentException("unsupported chrono unit: " + unit);
        }
    }

    @Override
    public Class<Instant> getCanonicalType() {
        return Instant.class;
    }

    @Override
    public Instant canonical(Object value) {
        if (value == null) {
            return null;
        }

        Class from = value.getClass();

        if (from == Instant.class) {
            return (Instant) value;
        }

        if (from == String.class) {
            return getDateTimeFormatter().parse((String) value, Instant::from);
        }

        if (from == Date.class) {
            return Instant.ofEpochMilli(((Date) value).getTime()); // in UTC
        }

        // need to be forgiving up the upstream parser
        if (from == Integer.class) {
            return longCanonical.apply(((Integer) value).longValue());
        }

        if (from == Long.class) {
            return longCanonical.apply((Long) value);
        }

        if (from == Double.class) {
            // this is the safest but not most absurd way to make the conversion
            return bigCanonical.apply(BigDecimal.valueOf((Double) value));
        }

        if (from == BigDecimal.class) {
            return bigCanonical.apply((BigDecimal) value);
        }

        if (TreeNode.class.isAssignableFrom(from)) {
            // assumes json is a timestamp value
            return JSONUtil.dataToValueSafe((TreeNode) value, Instant.class);
        }

        throw new CascadingException("unknown type coercion requested from: " + Util.getTypeName(from));
    }

    @Override
    public <Coerce> Coerce coerce(Object value, Type to) {
        if (value == null) {
            if (to == long.class) {
                return (Coerce) ZERO;
            }
            return null;
        }

        Class from = value.getClass();

        if (from != Instant.class) {
            throw new IllegalStateException("was not normalized");
        }

        // no coercion, or already in canonical form
        if (to == Instant.class || to.getClass() == InstantType.class) {
            return (Coerce) value;
        }

        if (to == Long.class || to == long.class) {
            return (Coerce) longCoerce.apply((Instant) value);
        }

        if (to == String.class) {
            return (Coerce) getDateTimeFormatter().format((Instant) value);
        }

        throw new CascadingException("unknown type coercion requested, from: " + Util.getTypeName(from) + " to: " + Util.getTypeName(to));
    }

    public DateTimeFormatter getDateTimeFormatter() {
        return dateTimeFormatter.get();
    }

    public TemporalUnit getUnit() {
        return unit;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
