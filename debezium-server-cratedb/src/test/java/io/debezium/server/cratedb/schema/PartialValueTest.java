package io.debezium.server.cratedb.schema;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class PartialValueTest {
    @Test
    void testPartialValue() {
        var a = PartialValue.of(null, 1) ;
        var b = PartialValue.of(null, 1) ;
        var c = PartialValue.of(null, 2) ;
        var d = PartialValue.of(1, 2) ;
        var e = PartialValue.of(1, 2) ;
        var f = 1;
        assertThat(a).isEqualTo(b);
        assertThat(a).isNotEqualTo(c);
        assertThat(c).isNotEqualTo(d);
        assertThat(d).isEqualTo(e);
        assertThat(f).isNotEqualTo(e);
    }
}