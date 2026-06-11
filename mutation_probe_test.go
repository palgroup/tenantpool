package tenantpool

import "testing"

// TestCIMutationProbe is a deliberate red test used once to prove the ci.yml
// unit gate can actually fail. It lives only on a throwaway PR branch and is
// never merged.
func TestCIMutationProbe(t *testing.T) {
	t.Fatal("deliberate failure: ci.yml unit gate mutation probe")
}
