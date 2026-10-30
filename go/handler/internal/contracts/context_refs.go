package contracts

import (
	"fmt"
	"strings"
)

// ValidateContextRefs checks the scheme and absolute path expected by the
// worker's parse_context_ref. Validate before persisting scheduled work so a
// bare path cannot be accepted now and fail only when the job becomes due.
func ValidateContextRefs(refs []string) error {
	for index, ref := range refs {
		scheme, path, ok := strings.Cut(ref, ":")
		if !ok || strings.TrimSpace(scheme) != "repo" || !strings.HasPrefix(strings.TrimSpace(path), "/") {
			return fmt.Errorf("context_refs[%d] must use repo:/absolute/path (for example repo:/workspace/memory); got %q", index, ref)
		}
	}
	return nil
}
