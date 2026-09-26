package main

import (
	"strings"
	"testing"
)

// explorer.exe splits /select on commas; a torrent named "Movie,calc.exe"
// must reach it as one quoted path, and paths that cannot be quoted fall
// back to opening the parent folder.
func TestExplorerCmdLineQuotesTorrentControlledPath(t *testing.T) {
	const exe = `C:\Windows\explorer.exe`
	const parent = `C:\Users\me\Downloads`
	cases := []struct {
		path string
		want string
		ok   bool
	}{
		{parent + `\Movie,calc.exe`, `"` + exe + `" /select,"` + parent + `\Movie,calc.exe"`, true},
		{parent + `\Movie,/root,C:\x`, `"` + exe + `" /select,"` + parent + `\Movie,/root,C:\x"`, true},
		{parent + `\My Show`, `"` + exe + `" /select,"` + parent + `\My Show"`, true},
		{parent + `\bad"name`, `"` + exe + `" "` + parent + `"`, true},
		{parent + "\\ctl\nname", `"` + exe + `" "` + parent + `"`, true},
		{`C:\`, `"` + exe + `" /select,"C:\."`, true},
	}
	for _, tc := range cases {
		got, ok := explorerCmdLine(exe, tc.path, parent)
		if ok != tc.ok || got != tc.want {
			t.Errorf("explorerCmdLine(%q) = %q, %v; want %q, %v", tc.path, got, ok, tc.want, tc.ok)
		}
		if strings.Count(got, `"`)%2 != 0 {
			t.Errorf("unbalanced quotes in %q", got)
		}
	}
	if _, ok := explorerCmdLine(exe, `C:\a"b`, `C:\a"`); ok {
		t.Error("unquotable path and parent were accepted")
	}
}

func TestRevealInFileManagerRejectsRelativePath(t *testing.T) {
	// Check the reason: without the check the helper would be launched, and
	// a missing xdg-open (as on CI) would also produce an error.
	if err := revealInFileManager("-R"); err == nil || !strings.Contains(err.Error(), "not an absolute path") {
		t.Fatalf("relative path: err = %v; want refusal before launching anything", err)
	}
}
