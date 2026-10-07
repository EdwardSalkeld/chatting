package routing

import "testing"

func TestNormalizePRRejectsEmbeddedGitHubURL(t *testing.T) {
	for _, value := range []string{
		"https://evil.example/https://github.com/owner/repo/pull/50",
		"https://github.com.evil.example/owner/repo/pull/50",
	} {
		if _, err := NormalizePR(value); err == nil {
			t.Errorf("NormalizePR(%q) accepted a non-GitHub host", value)
		}
	}
	key, err := NormalizePR("https://github.com/Owner/Repo/pull/50#discussion")
	if err != nil || key != "owner/repo#50" {
		t.Fatalf("NormalizePR(valid URL) = %q, %v", key, err)
	}
}
