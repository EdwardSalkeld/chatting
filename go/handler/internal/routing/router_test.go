package routing

import "testing"

func TestNormalizePRRejectsEmbeddedGitHubURL(t *testing.T) {
	for _, value := range []string{
		"https://evil.example/https://github.com/owner/repo/pull/50",
		"https://github.com.evil.example/owner/repo/pull/50",
		"https://github.com/owner/repo/pull/50.evil",
		"https://github.com/owner/repo/pull/50@evil.example",
		"https://github.com/owner/repo/pull/50/checks",
	} {
		if _, err := NormalizePR(value); err == nil {
			t.Errorf("NormalizePR(%q) accepted an invalid URL", value)
		}
	}
	key, err := NormalizePR("https://github.com/Owner/Repo/pull/50#discussion")
	if err != nil || key != "owner/repo#50" {
		t.Fatalf("NormalizePR(valid URL) = %q, %v", key, err)
	}
	key, err = prKey("https://github.com/Owner/Repo/pull/50/checks", true)
	if err != nil || key != "owner/repo#50" {
		t.Fatalf("prKey(valid notification URL) = %q, %v", key, err)
	}
	for _, value := range []string{
		"https://evil.example/https://github.com/owner/repo/pull/50/checks",
		"https://github.com.evil.example/owner/repo/pull/50/checks",
		"https://github.com/owner/repo/pull/50.evil/checks",
	} {
		if _, err := prKey(value, true); err == nil {
			t.Errorf("prKey(%q, true) accepted an invalid URL", value)
		}
	}
}
