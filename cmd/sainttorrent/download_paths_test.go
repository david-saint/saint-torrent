package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sainttorrent/pkg/downloader"
)

type fallbackTestAdder struct {
	failDir  string
	calls    []string
	existing *downloader.Session
}

func (a *fallbackTestAdder) GetSession(string) *downloader.Session { return a.existing }

func (a *fallbackTestAdder) AddMagnet(_ string, dir string) (*downloader.Session, error) {
	a.calls = append(a.calls, dir)
	if dir == a.failDir {
		return nil, fmt.Errorf("disk became unavailable")
	}
	return &downloader.Session{}, nil
}

func (a *fallbackTestAdder) AddTorrentFile(_ string, dir string) (*downloader.Session, error) {
	return a.AddMagnet("", dir)
}

func TestDownloadPathCandidatesNormalizeAndDeduplicate(t *testing.T) {
	relative := filepath.Join("testdata", "downloads")
	absRelative, err := filepath.Abs(relative)
	if err != nil {
		t.Fatal(err)
	}

	paths := downloadPathOptions{
		primary:   relative,
		fallbacks: []string{relative, t.TempDir()},
	}.candidates()

	if len(paths) != 2 {
		t.Fatalf("candidates = %v, want two unique paths", paths)
	}
	if paths[0] != absRelative {
		t.Fatalf("primary = %q, want %q", paths[0], absRelative)
	}
}

func TestNormalizedPathsPreserveExplicitEmptyFallbackList(t *testing.T) {
	paths := (downloadPathOptions{primary: t.TempDir(), fallbacks: []string{}}).normalized()
	if paths.fallbacks == nil {
		t.Fatal("explicit empty fallbacks became nil")
	}
	payload, err := json.Marshal(socketMessage{
		DownloadDir:          paths.primary,
		FallbackDownloadDirs: paths.fallbacks,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), `"fallback_download_dirs":[]`) {
		t.Fatalf("socket payload does not preserve empty fallback list: %s", payload)
	}
}

func TestSelectDownloadPathUsesFirstWritableFallback(t *testing.T) {
	root := t.TempDir()
	blockingFile := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(blockingFile, []byte("blocked"), 0644); err != nil {
		t.Fatal(err)
	}
	fallback := filepath.Join(root, "fallback")

	selected, err := selectDownloadPath(downloadPathOptions{
		primary:   filepath.Join(blockingFile, "downloads"),
		fallbacks: []string{fallback},
	})
	if err != nil {
		t.Fatalf("selectDownloadPath: %v", err)
	}
	if selected != fallback {
		t.Fatalf("selected = %q, want fallback %q", selected, fallback)
	}

	entries, err := os.ReadDir(fallback)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("write probe was not cleaned up: %v", entries)
	}
}

func TestSelectDownloadPathReportsEveryFailure(t *testing.T) {
	root := t.TempDir()
	blockingFile := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(blockingFile, []byte("blocked"), 0644); err != nil {
		t.Fatal(err)
	}
	primary := filepath.Join(blockingFile, "primary")
	fallback := filepath.Join(blockingFile, "fallback")

	_, err := selectDownloadPath(downloadPathOptions{
		primary:   primary,
		fallbacks: []string{fallback},
	})
	if err == nil {
		t.Fatal("expected all candidates to fail")
	}
	for _, path := range []string{primary, fallback} {
		if !strings.Contains(err.Error(), path) {
			t.Errorf("error %q does not include failed path %q", err, path)
		}
	}
}

func TestAddTorrentRetriesFallbackAfterStorageInitializationFailure(t *testing.T) {
	primary := t.TempDir()
	fallback := t.TempDir()
	adder := &fallbackTestAdder{failDir: primary}

	sess, err := addTorrentWithDownloadPaths(
		adder,
		"magnet:?xt=urn:btih:542e85596f7a0dd05eefdb78b0ac1736496f8626&dn=RetryFallback",
		downloadPathOptions{primary: primary, fallbacks: []string{fallback}},
	)
	if err != nil {
		t.Fatalf("addTorrentWithDownloadPaths: %v", err)
	}
	if sess == nil {
		t.Fatal("expected fallback add to return a session")
	}
	if !slices.Equal(adder.calls, []string{primary, fallback}) {
		t.Fatalf("add calls = %v, want primary then fallback", adder.calls)
	}
}

func TestReaddingMagnetPreservesFallbacksWithoutReprobingPrimary(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	primary := filepath.Join(t.TempDir(), "downloads")
	fallback := t.TempDir()
	const magnet = "magnet:?xt=urn:btih:542e85596f7a0dd05eefdb78b0ac1736496f8626&dn=PreserveFallback"

	first, err := addTorrentWithDownloadPaths(mgr, magnet, downloadPathOptions{
		primary:   primary,
		fallbacks: []string{fallback},
	})
	if err != nil {
		t.Fatalf("first add: %v", err)
	}
	if got := first.FallbackDownloadDirs(); !slices.Equal(got, []string{fallback}) {
		t.Fatalf("initial fallbacks = %v, want %v", got, []string{fallback})
	}

	// Make the original primary unusable. A duplicate add must return the
	// existing session and retain its deferred fallback rather than clearing it.
	if err := os.Remove(primary); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(primary, []byte("blocked"), 0644); err != nil {
		t.Fatal(err)
	}
	second, err := addTorrentWithDownloadPaths(mgr, magnet, downloadPathOptions{
		primary:   primary,
		fallbacks: []string{fallback},
	})
	if err != nil {
		t.Fatalf("duplicate add: %v", err)
	}
	if second != first {
		t.Fatal("duplicate add returned a different session")
	}
	if got := second.FallbackDownloadDirs(); !slices.Equal(got, []string{fallback}) {
		t.Fatalf("fallbacks after duplicate add = %v, want %v", got, []string{fallback})
	}
}

func TestSocketDownloadPathsOverridesDefaults(t *testing.T) {
	defaults := downloadPathOptions{primary: "/default", fallbacks: []string{"/default-fallback"}}
	fromMessage := socketDownloadPaths(socketMessage{
		DownloadDir:          "/primary",
		FallbackDownloadDirs: []string{"/fallback-one", "/fallback-two"},
	}, defaults)

	if fromMessage.primary != "/primary" || !slices.Equal(fromMessage.fallbacks, []string{"/fallback-one", "/fallback-two"}) {
		t.Fatalf("unexpected socket paths: %+v", fromMessage)
	}
	if got := socketDownloadPaths(socketMessage{}, defaults); got.primary != defaults.primary || !slices.Equal(got.fallbacks, defaults.fallbacks) {
		t.Fatalf("empty socket paths did not retain defaults: %+v", got)
	}
	legacy := socketDownloadPaths(socketMessage{DownloadDir: "/legacy-primary"}, defaults)
	if legacy.primary != "/legacy-primary" || !slices.Equal(legacy.fallbacks, defaults.fallbacks) {
		t.Fatalf("legacy socket message did not retain server fallbacks: %+v", legacy)
	}
	explicitEmpty := socketDownloadPaths(socketMessage{DownloadDir: "/primary", FallbackDownloadDirs: []string{}}, defaults)
	if explicitEmpty.fallbacks == nil || len(explicitEmpty.fallbacks) != 0 {
		t.Fatalf("explicit empty fallback list was not preserved: %+v", explicitEmpty)
	}
}

func TestImmediateSocketAddUsesFallbackDirectory(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	root := t.TempDir()
	blockingFile := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(blockingFile, []byte("blocked"), 0644); err != nil {
		t.Fatal(err)
	}
	fallback := filepath.Join(root, "fallback")
	m := initialModel(mgr, t.TempDir(), "", nil)
	const infoHash = "542e85596f7a0dd05eefdb78b0ac1736496f8626"

	_, _ = m.Update(addTorrentMsg{msg: socketMessage{
		Items:                []string{"magnet:?xt=urn:btih:" + infoHash + "&dn=Fallback"},
		DownloadDir:          filepath.Join(blockingFile, "primary"),
		FallbackDownloadDirs: []string{fallback},
	}})

	sess := mgr.GetSession(infoHash)
	if sess == nil {
		t.Fatal("torrent was not added")
	}
	if got := sess.DownloadDir(); got != fallback {
		t.Fatalf("download directory = %q, want fallback %q", got, fallback)
	}
}

func TestHeadlessLegacySocketMessageInheritsServerFallback(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	root := t.TempDir()
	blockingFile := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(blockingFile, []byte("blocked"), 0644); err != nil {
		t.Fatal(err)
	}
	primary := filepath.Join(blockingFile, "primary")
	fallback := filepath.Join(root, "fallback")
	const infoHash = "642e85596f7a0dd05eefdb78b0ac1736496f8626"

	err := handleHeadlessSocketMessage(socketMessage{
		Items:       []string{"magnet:?xt=urn:btih:" + infoHash + "&dn=HeadlessFallback"},
		DownloadDir: primary,
	}, mgr, downloadPathOptions{primary: t.TempDir(), fallbacks: []string{fallback}})
	if err != nil {
		t.Fatalf("handleHeadlessSocketMessage: %v", err)
	}
	if got := mgr.GetSession(infoHash).DownloadDir(); got != fallback {
		t.Fatalf("download directory = %q, want inherited fallback %q", got, fallback)
	}
}
