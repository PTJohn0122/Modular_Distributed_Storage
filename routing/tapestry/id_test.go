package tapestry

import (
	"math/big"
	"testing"
)

func TestEnsureConsts(t *testing.T) {
	if BASE != int(16) {
		t.Errorf(
			"ID tests implemented for base 16 ids, current BASE is %v, re-run tests with correct BASE",
			BASE,
		)
	}
	if DIGITS != int(40) {
		t.Errorf(
			"ID tests implemented for 40-digit ids, current DIGITS is %v, re-run tests with correct DIGITS",
			DIGITS,
		)
	}
}

func TestHash(t *testing.T) {
	cases := []string{"hello", "world", "Jonathan Mace", "Jeff Rasley",
		"Rodrigo Fonseca", "Tom Doeppner", "Cody Mello", "foo", "bar",
		"Brown university", "Brown University"}
	for _, c := range cases {
		a := Hash(c)
		b := Hash(c)
		if a != b {
			t.Errorf("Did not get same ID for %v: %v and %v", c, a, b)
		}

		for _, d := range cases {
			b := Hash(d)
			if c != d && a == b {
				t.Errorf("Got same ID for different strings: %v, %v: %v", c, d, b)
			}
		}
	}
}

func TestDeterministic(t *testing.T) {
	cases := []string{"hello", "world", "Jonathan Mace", "Jeff Rasley",
		"Rodrigo Fonseca", "Tom Doeppner", "Cody Mello", "foo", "bar",
		"Brown university", "Brown University"}
	expected := []string{
		"AAF4C61DDCC5E8A2DABEDE0F3B482CD9AEA9434D",
		"7C211433F02071597741E6FF5A8EA34789ABBF43",
		"F909A9200B27BF625E8A0BA2E06758AEEC254CAD",
		"CF316664F024C6DC09025736055948AA73E54839",
		"8643ADD0432D58CA043FFF23C87FDF07F460B8FA",
		"19EA76F5E3B30953077D5747818D4692CF54847E",
		"519BFDD559CB03792ECD7565AEA263D08F3CF528",
		"0BEEC7B5EA3F0FDBC95D0DD47F3C5BC275DA8A33",
		"62CDB7020FF920E5AA642C3D4066950DD1F01F4D",
		"E4F3C8B0B3AE75E720AF1322F045FFD51031A6EE",
		"54D31843077B586455EAF042590739FBDAE8AA84",
	}
	for i, c := range cases {
		s := Hash(c).String()
		if expected[i] != s {
			t.Errorf("%v did not produce expected ID %v, instead %v", c, expected[i], s)
		}
	}
}

func TestParse(t *testing.T) {
	cases := []string{"hello", "world", "Jonathan Mace", "Jeff Rasley",
		"Rodrigo Fonseca", "Tom Doeppner", "Cody Mello", "foo", "bar",
		"Brown university", "Brown University"}
	expected := []string{
		"AAF4C61DDCC5E8A2DABEDE0F3B482CD9AEA9434D",
		"7C211433F02071597741E6FF5A8EA34789ABBF43",
		"F909A9200B27BF625E8A0BA2E06758AEEC254CAD",
		"CF316664F024C6DC09025736055948AA73E54839",
		"8643ADD0432D58CA043FFF23C87FDF07F460B8FA",
		"19EA76F5E3B30953077D5747818D4692CF54847E",
		"519BFDD559CB03792ECD7565AEA263D08F3CF528",
		"0BEEC7B5EA3F0FDBC95D0DD47F3C5BC275DA8A33",
		"62CDB7020FF920E5AA642C3D4066950DD1F01F4D",
		"E4F3C8B0B3AE75E720AF1322F045FFD51031A6EE",
		"54D31843077B586455EAF042590739FBDAE8AA84",
	}
	for i, c := range cases {
		id := Hash(c)
		parsed, err := ParseID(expected[i])
		if err != nil {
			t.Error(err)
		}
		if parsed != id {
			t.Errorf("ParseID(%v) != Hash(%v) (%v)", expected[i], c, id)
		}
		if parsed.String() != expected[i] {
			t.Errorf("%v.String() != %v", parsed, expected[i])
		}
	}
}

func TestBigID(t *testing.T) {
	cases := []struct {
		str    string
		expect int64
	}{{"0000000000000000000000000000000000000000", 0},
		{"0000000000000000000000000000000000000001", 1},
		{"0000000000000000000000000000000000000010", 16},
		{"0000000000000000000000000000000000011170", 70000},
	}

	for _, c := range cases {
		id, _ := ParseID(c.str)
		b := id.Big()
		expect := big.NewInt(c.expect)
		if b.Cmp(expect) != 0 {
			t.Errorf("'%v'.big() != %v (was %v)", id, expect, b)
		}
	}
}

func checkCloser(a, b, c string, closer bool, t *testing.T) {
	ida, _ := ParseID(a)
	idb, _ := ParseID(b)
	idc, _ := ParseID(c)
	if ida.Closer(idb, idc) != closer {
		t.Errorf("Expected '%v'.Closer('%v', '%v') == %v", a, b, c, closer)
	}
}

func TestCloser(t *testing.T) {
	a, _ := ParseID("0000000000000000000001000000000000000000")
	b, _ := ParseID("0000000000000000000000100000000000000000")
	c, _ := ParseID("0000000000000000000000010000000000000000")

	if !a.Closer(b, c) {
		t.Errorf("Expected '%v'.Closer('%v','%v)", a, b, c)
	}
	if a.Closer(c, b) {
		t.Errorf("Expected !'%v'.Closer('%v','%v)", a, c, b)
	}
	if !b.Closer(c, a) {
		t.Errorf("Expected '%v'.Closer('%v','%v)", b, c, a)
	}
	if b.Closer(a, c) {
		t.Errorf("Expected !'%v'.Closer('%v','%v)", b, a, c)
	}
	if !c.Closer(b, a) {
		t.Errorf("Expected '%v'.Closer('%v','%v)", c, b, a)
	}
	if c.Closer(a, b) {
		t.Errorf("Expected !'%v'.Closer('%v','%v)", c, a, b)
	}

	checkCloser("0000000000000000000001000000000000000000",
		"0000000000000000000001000000000000000000",
		"0000000000000000000001000000000000000000",
		false, t,
	)
	checkCloser("1000000000000000000000000000000000000000",
		"0FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
		"1000000000000000000000000000000000000002",
		true, t,
	)
}

func TestDigitDistance(t *testing.T) {
	cases := []struct {
		me, a, b string
		expected bool
	}{{"hello", "world", "Jonathan Mace", true},
		{"hello", "Jonathan Mace", "world", false},
		{"Brown University", "Brown university", "bar", false},
		{"Brown University", "bar", "Brown university", true},
		{"bar", "Brown University", "Brown university", true},
		{"bar", "Brown university", "Brown University", false},
		{"hello", "world", "world", false},
		{"hello", "Jonathan Mace", "Jonathan Mace", false},
	}

	for _, c := range cases {
		idme := Hash(c.me)
		ida := Hash(c.a)
		idb := Hash(c.b)
		closer := idme.Closer(ida, idb)
		expected := c.expected
		if closer != expected {
			t.Errorf(
				"Expected (\"%v\".Closer(\"%v\", \"%v\") == %v, but was %v.\nidme=%v (%v)\nida= %v (%v)\nidb= %v (%v)",
				c.me,
				c.a,
				c.b,
				expected,
				closer,
				idme,
				c.me,
				ida,
				c.a,
				idb,
				c.b,
			)
		}
	}
}

func TestDigitsSet(t *testing.T) {
	for i := 0; i < 16; i++ {
		if int(Digit(i)) != i {
			t.Errorf("Digit %v wasn't %v", Digit(i), i)
		}
	}
}

func TestIDSimple(t *testing.T) {
	var d [DIGITS]Digit
	for i := 0; i < DIGITS; i++ {
		d[i] = Digit(i)
	}
	for i := 0; i < DIGITS; i++ {
		if int(d[i]) != i {
			t.Errorf("%v != %v", d[i], i)
		}
	}
}

func TestSharedPrefixLength(t *testing.T) {
	a, _ := ParseID("1000000000000000000000000000000000000000")
	b, _ := ParseID("2000000000000000000000000000000000000000")
	prefixLen := SharedPrefixLength(a, b)
	if prefixLen != 0 {
		t.Error("Shared Predix Length isn't 6")
	}

	a, _ = ParseID("8643ADD0432D58CA043FFF23C87FDF07F460B8FA")
	b, _ = ParseID("8643ADD0432D58CA04AFFF23C87FDF07F460B8FA")
	prefixLen = SharedPrefixLength(a, b)
	if prefixLen != 18 {
		t.Error("Shared Predix Length isn't 6")
	}

	a, _ = ParseID("54D31843077B586455EAF042590739FBDAE8AA84")
	b, _ = ParseID("A4D31843077B586455EAF042590739FBDAE8AA84")
	prefixLen = SharedPrefixLength(a, b)
	if prefixLen != 0 {
		t.Error("Shared Predix Length isn't 6")
	}
}
