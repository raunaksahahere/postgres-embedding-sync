package keys

import "testing"

func TestCompare(t *testing.T) {
	if Int.Compare("9", "10") != -1 || Int.Compare("-5", "3") != -1 || Int.Compare("7", "7") != 0 {
		t.Fatal("int keys must compare numerically")
	}
	if Text.Compare("9", "10") != 1 || Text.Compare("B", "a") != -1 {
		t.Fatal("text keys must compare bytewise (COLLATE \"C\")")
	}
	if UUID.Compare("0b4e6a1c-0000-4000-8000-000000000000", "a0000000-0000-4000-8000-000000000000") != -1 {
		t.Fatal("uuid order")
	}
	for _, oid := range []uint32{20, 21, 23, 25, 1043, 2950} {
		if _, err := FromTypeOID(oid); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := FromTypeOID(1700); err == nil {
		t.Fatal("numeric keys are unsupported")
	}
}
