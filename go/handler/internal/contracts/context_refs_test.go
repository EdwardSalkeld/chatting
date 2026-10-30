package contracts

import (
	"strings"
	"testing"
)

func TestValidateContextRefs(t *testing.T) {
	for _, ref := range []string{"repo:/workspace/memory", "repo:/", " repo : /folder with spaces/a:b "} {
		if err := ValidateContextRefs([]string{ref}); err != nil {
			t.Errorf("valid ref %q: %v", ref, err)
		}
	}
	if err := ValidateContextRefs(nil); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []string{"/workspace/memory", "repo:relative", "repo:", ":/workspace", "file:/workspace", "", " ", "repo:   "} {
		t.Run(ref, func(t *testing.T) {
			err := ValidateContextRefs([]string{"repo:/valid", ref})
			if err == nil || !strings.Contains(err.Error(), "context_refs[1]") || !strings.Contains(err.Error(), "repo:/absolute/path") {
				t.Fatalf("expected indexed corrective error, got %v", err)
			}
		})
	}
}
