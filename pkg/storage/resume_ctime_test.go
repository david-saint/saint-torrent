package storage

import (
	"go/build"
	"testing"
)

// TestStatChangeTimeDefinedOncePerTarget: resume_ctime_linux.go carried a
// "linux || openbsd" tag, but its _linux suffix still limited it to Linux, so
// OpenBSD had no statChangeTime and the package did not build there. Every
// target that compiles its caller must now select exactly one accessor file.
// The build constraints are evaluated here rather than cross-compiled, so the
// check runs on every host in milliseconds.
func TestStatChangeTimeDefinedOncePerTarget(t *testing.T) {
	accessors := []string{"resume_ctime_bsd.go", "resume_ctime_statim.go", "resume_ctime_other.go"}
	targets := [][2]string{
		{"aix", "ppc64"}, {"android", "arm64"}, {"darwin", "arm64"}, {"dragonfly", "amd64"},
		{"freebsd", "amd64"}, {"illumos", "amd64"}, {"ios", "arm64"}, {"linux", "386"},
		{"netbsd", "amd64"}, {"openbsd", "amd64"}, {"solaris", "amd64"}, {"windows", "amd64"},
	}
	for _, target := range targets {
		ctx := build.Default
		ctx.GOOS, ctx.GOARCH, ctx.CgoEnabled = target[0], target[1], false
		match := func(name string) bool {
			ok, err := ctx.MatchFile(".", name)
			if err != nil {
				t.Fatalf("%s/%s: MatchFile(%s): %v", target[0], target[1], name, err)
			}
			return ok
		}
		want := 0
		if match("resume_checkpoint_unix.go") {
			want = 1
		}
		var selected []string
		for _, name := range accessors {
			if match(name) {
				selected = append(selected, name)
			}
		}
		if len(selected) != want {
			t.Errorf("%s/%s selects %v, want %d statChangeTime accessor(s)", target[0], target[1], selected, want)
		}
	}
}
