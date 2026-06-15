package main

import (
	"os"
	"testing"

	"sainttorrent/pkg/downloader"
)

func TestNextThemeCycles(t *testing.T) {
	if got := nextTheme(draculaTheme); got != monoTheme {
		t.Errorf("nextTheme(dracula)=%s want mono", got.name)
	}
	if got := nextTheme(monoTheme); got != draculaTheme {
		t.Errorf("nextTheme(mono)=%s want dracula", got.name)
	}
}

func TestResolveInitialTheme(t *testing.T) {
	dir := t.TempDir()
	if err := saveUIPrefs(dir, "mono"); err != nil {
		t.Fatalf("saveUIPrefs: %v", err)
	}

	// Invalid flag falls back to default + warns, and does NOT fall through to
	// the persisted preference.
	var warns []string
	if got := resolveInitialTheme("bogus", true, dir, &warns); got != draculaTheme {
		t.Errorf("invalid flag -> %s want dracula", got.name)
	}
	if len(warns) == 0 {
		t.Error("invalid flag should warn")
	}

	// Valid flag wins.
	warns = nil
	if got := resolveInitialTheme("mono", true, dir, &warns); got != monoTheme {
		t.Errorf("flag mono -> %s", got.name)
	}

	// No flag -> valid persisted preference wins, silently.
	warns = nil
	if got := resolveInitialTheme("", true, dir, &warns); got != monoTheme {
		t.Errorf("persisted mono -> %s", got.name)
	}
	if len(warns) != 0 {
		t.Errorf("unexpected warns: %v", warns)
	}

	// Persistence disabled -> ignore persisted file, use default.
	warns = nil
	if got := resolveInitialTheme("", false, dir, &warns); got != draculaTheme {
		t.Errorf("no-persist -> %s want dracula", got.name)
	}

	// Missing prefs (first run) -> default, no warning.
	empty := t.TempDir()
	warns = nil
	if got := resolveInitialTheme("", true, empty, &warns); got != draculaTheme {
		t.Errorf("missing -> %s want dracula", got.name)
	}
	if len(warns) != 0 {
		t.Errorf("missing file must not warn: %v", warns)
	}

	// Malformed prefs -> default + warning.
	bad := t.TempDir()
	if err := os.WriteFile(uiPrefsPath(bad), []byte("{not json"), 0600); err != nil {
		t.Fatal(err)
	}
	warns = nil
	if got := resolveInitialTheme("", true, bad, &warns); got != draculaTheme {
		t.Errorf("malformed -> %s want dracula", got.name)
	}
	if len(warns) == 0 {
		t.Error("malformed prefs should warn")
	}
}

func TestLoadUIPrefsMissing(t *testing.T) {
	p, err := loadUIPrefs(t.TempDir())
	if err != nil {
		t.Fatalf("missing file should not error: %v", err)
	}
	if p.Theme != "" {
		t.Errorf("theme=%q want empty", p.Theme)
	}
}

func TestSaveLoadUIPrefsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if err := saveUIPrefs(dir, "mono"); err != nil {
		t.Fatalf("save: %v", err)
	}
	// 0600 perms on the written file.
	info, err := os.Stat(uiPrefsPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("perms=%v want 0600", info.Mode().Perm())
	}
	p, err := loadUIPrefs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if p.Theme != "mono" {
		t.Errorf("theme=%q want mono", p.Theme)
	}
}

func TestCycleThemePersists(t *testing.T) {
	dir := t.TempDir()
	mgr := downloader.NewTorrentManager()
	m := initialModel(mgr, ".", "", nil)
	m.configDir = dir
	m.persistEnabled = true

	m.cycleTheme() // dracula -> mono
	if m.theme != monoTheme {
		t.Fatalf("theme=%s want mono", m.theme.name)
	}
	p, err := loadUIPrefs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if p.Theme != "mono" {
		t.Errorf("persisted=%q want mono", p.Theme)
	}
}

func TestCycleThemeNoPersist(t *testing.T) {
	dir := t.TempDir()
	mgr := downloader.NewTorrentManager()
	m := initialModel(mgr, ".", "", nil)
	m.configDir = dir
	m.persistEnabled = false

	m.cycleTheme()
	if _, err := os.Stat(uiPrefsPath(dir)); !os.IsNotExist(err) {
		t.Errorf("ui.json must not be written with persistence off (err=%v)", err)
	}
}
