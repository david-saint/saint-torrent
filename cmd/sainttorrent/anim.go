package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// animInterval is how often the UI re-renders to advance time-based animations
// (e.g. the download-speed pulse). It only rebuilds the view from the cached
// session snapshot — no session/network work happens here — so a smooth cadence
// is cheap and never competes with download throughput.
const animInterval = 100 * time.Millisecond

// animMsg ticks the animation clock. Distinct from tickMsg (the 500ms data
// refresh) so frame rate and data rate stay independent.
type animMsg time.Time

func animCmd() tea.Cmd {
	return tea.Tick(animInterval, func(t time.Time) tea.Msg { return animMsg(t) })
}

// pulseOpacity oscillates in [0.4, 1.0] on a 2-second cycle: it eases from 1.0
// down to 0.4 over the first second and back up over the next, so a pulsing
// element "fades out" once per second and "fades in" the next.
func pulseOpacity(now time.Time) float64 {
	const periodNs = float64(2 * time.Second)
	phase := math.Mod(float64(now.UnixNano()), periodNs) / periodNs // 0..1
	return 0.7 + 0.3*math.Cos(2*math.Pi*phase)
}

// dimHex scales an "#rrggbb" color toward black by f in [0,1] (1 = unchanged,
// 0 = black). On the dark terminal backgrounds both themes assume, this reads as
// an opacity fade of the foreground text.
func dimHex(hex string, f float64) string {
	f = math.Max(0, math.Min(1, f))
	h := strings.TrimPrefix(hex, "#")
	v, err := strconv.ParseUint(h, 16, 32)
	if err != nil || len(h) != 6 {
		return hex
	}
	r := float64((v>>16)&0xff) * f
	g := float64((v>>8)&0xff) * f
	b := float64(v&0xff) * f
	return fmt.Sprintf("#%02x%02x%02x", int(r), int(g), int(b))
}

// speedStyle returns the style for a row's download-speed cell: active downloads
// pulse in the theme accent (blinking between full and ~0.4 brightness once a
// second); everything else stays muted.
func speedStyle(st styles, downloading bool) lipgloss.Style {
	if !downloading || st.AccentHex == "" {
		return st.Muted
	}
	return lipgloss.NewStyle().Foreground(lipgloss.Color(dimHex(st.AccentHex, pulseOpacity(time.Now()))))
}
