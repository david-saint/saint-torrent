package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// styles is the semantic style set every theme fills from its palette. The
// shared view code references only these names, never concrete colors, so
// swapping the active theme restyles the whole UI. The list and details views
// are rendered per-theme (see theme.renderList / renderDetails) and may lean on
// the hairline/ramp "extras" at the bottom; secondary views use the chrome set.
type styles struct {
	Title, Header, Help, Subtle, Info, Warn, Error lipgloss.Style
	Card, PeersCard                                lipgloss.Style
	SelectedRow, NormalRow                         lipgloss.Style

	StatusDownloading, StatusSeeding, StatusPaused lipgloss.Style
	StatusMetadata, StatusError                    lipgloss.Style

	PriorityNormal, PriorityHigh, PrioritySkip lipgloss.Style

	// Hairline/mono extras: a brightness ramp + single accent, the thin meter
	// track, and the piece-map roles. Themes that don't use the airy layout
	// still fill these with sensible equivalents so the struct is always valid.
	Dim, Faint, Muted, Primary, Emphasis, Bold lipgloss.Style
	Accent, AccentBold, Track, Hairline        lipgloss.Style
	PieceHave, PieceDownloading, PiecePending  lipgloss.Style
}

// theme bundles a label, its filled style set, and the renderers for the two
// views whose whole layout is theme-specific (list + details). Secondary views
// are shared and consume only styles.
type theme struct {
	name, label   string
	styles        styles
	renderList    func(m *model) string
	renderDetails func(m *model) string
}

func draculaStyles() styles {
	c := func(hex string) lipgloss.Color { return lipgloss.Color(hex) }
	fg := func(hex string) lipgloss.Style { return lipgloss.NewStyle().Foreground(c(hex)) }
	badge := lipgloss.NewStyle().Bold(true).Padding(0, 1)
	return styles{
		Title:     lipgloss.NewStyle().Bold(true).Foreground(c("#f8f8f2")).Background(c("#bd93f9")).Padding(0, 1).MarginBottom(1),
		Header:    fg("#00f0ff").Bold(true),
		Help:      fg("#44475a").Italic(true),
		Subtle:    fg("#6272a4"),
		Info:      fg("#50fa7b"),
		Warn:      fg("#ffb86c"),
		Error:     fg("#ff5555"),
		Card:      lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(c("#6272a4")).Padding(1).MarginBottom(1),
		PeersCard: lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(c("#bd93f9")).Padding(1).MarginBottom(1),

		SelectedRow: lipgloss.NewStyle().Background(c("#44475a")).Foreground(c("#50fa7b")).Bold(true),
		NormalRow:   fg("#f8f8f2"),

		StatusDownloading: fg("#50fa7b").Bold(true),
		StatusSeeding:     fg("#bd93f9").Bold(true),
		StatusPaused:      fg("#ffb86c").Bold(true),
		StatusMetadata:    fg("#ff79c6").Bold(true),
		StatusError:       fg("#ff5555").Bold(true),

		PriorityNormal: badge.Background(c("#6272a4")).Foreground(c("#f8f8f2")),
		PriorityHigh:   badge.Background(c("#ff5555")).Foreground(c("#f8f8f2")),
		PrioritySkip:   badge.Background(c("#44475a")).Foreground(c("#6272a4")),

		// extras (used by the mono renderers; sensible Dracula equivalents):
		Dim:              fg("#6272a4"),
		Faint:            fg("#44475a"),
		Muted:            fg("#6272a4"),
		Primary:          fg("#f8f8f2"),
		Emphasis:         fg("#f8f8f2"),
		Bold:             fg("#f8f8f2").Bold(true),
		Accent:           fg("#50fa7b"),
		AccentBold:       fg("#50fa7b").Bold(true),
		Track:            fg("#44475a"),
		Hairline:         fg("#6272a4"),
		PieceHave:        fg("#50fa7b"),
		PieceDownloading: fg("#ffb86c"),
		PiecePending:     fg("#6272a4"),
	}
}

// monoStyles maps the spike's grayscale ramp + single teal accent
// (spikes/03a-mono-hairline) onto the semantic style set. The look is carried by
// brightness rather than hue, so errors/warnings use bright/bold rather than red.
func monoStyles() styles {
	c := func(hex string) lipgloss.Color { return lipgloss.Color(hex) }
	const (
		g2  = "#22222a" // hairline / track
		g3  = "#34343d" // faint / pending
		g4  = "#56565f" // dim
		g5  = "#85858f" // muted
		g6  = "#b4b4be" // primary
		g7  = "#e8e8ee" // emphasis
		acc = "#6fd8c8" // the one accent
	)
	fg := func(hex string) lipgloss.Style { return lipgloss.NewStyle().Foreground(c(hex)) }
	return styles{
		Title:     fg(g7).Bold(true),
		Header:    fg(g7),
		Help:      fg(g4),
		Subtle:    fg(g5),
		Info:      fg(acc),
		Warn:      fg(g6),
		Error:     fg(g7).Bold(true),
		Card:      lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(c(g3)).Padding(1).MarginBottom(1),
		PeersCard: lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(c(g3)).Padding(1).MarginBottom(1),

		SelectedRow: lipgloss.NewStyle().Background(c(g2)).Foreground(c(g7)),
		NormalRow:   fg(g6),

		StatusDownloading: fg(g7),
		StatusSeeding:     fg(g5),
		StatusPaused:      fg(g3),
		StatusMetadata:    fg(g4).Italic(true),
		StatusError:       fg(g7).Bold(true),

		PriorityNormal: fg(g5),
		PriorityHigh:   fg(g7).Bold(true),
		PrioritySkip:   fg(g3),

		Dim:              fg(g4),
		Faint:            fg(g3),
		Muted:            fg(g5),
		Primary:          fg(g6),
		Emphasis:         fg(g7),
		Bold:             fg(g7).Bold(true),
		Accent:           fg(acc),
		AccentBold:       fg(acc).Bold(true),
		Track:            fg(g2),
		Hairline:         fg(g2),
		PieceHave:        fg(g5),
		PieceDownloading: fg(acc),
		PiecePending:     fg(g3),
	}
}

const defaultThemeName = "dracula"

var (
	draculaTheme = &theme{
		name: "dracula", label: "Dracula",
		styles:     draculaStyles(),
		renderList: renderListDracula, renderDetails: renderDetailsDracula,
	}
	monoTheme = &theme{
		name: "mono", label: "Mono Hairline",
		styles:     monoStyles(),
		renderList: renderListMono, renderDetails: renderDetailsMono,
	}

	themes      = []*theme{draculaTheme, monoTheme}
	themeByName = map[string]*theme{
		draculaTheme.name: draculaTheme,
		monoTheme.name:    monoTheme,
	}
)

// nextTheme returns the theme after cur, wrapping around — used by the `t` key.
func nextTheme(cur *theme) *theme {
	for i, t := range themes {
		if t == cur {
			return themes[(i+1)%len(themes)]
		}
	}
	return themes[0]
}

// uiPrefs is the persisted appearance state (currently just the theme name).
type uiPrefs struct {
	Theme string `json:"theme"`
}

func uiPrefsPath(dir string) string { return filepath.Join(dir, "ui.json") }

// loadUIPrefs reads persisted UI preferences. A missing file is the normal
// first-run case and returns (zero, nil) — not an error. Other read failures or
// malformed JSON return a non-nil error so the caller can warn and fall back.
func loadUIPrefs(dir string) (uiPrefs, error) {
	var p uiPrefs
	if dir == "" {
		return p, nil
	}
	data, err := os.ReadFile(uiPrefsPath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return p, nil
		}
		return p, err
	}
	if err := json.Unmarshal(data, &p); err != nil {
		return p, err
	}
	return p, nil
}

// saveUIPrefs atomically writes the UI preferences with 0600 perms (temp file in
// the same directory, then rename).
func saveUIPrefs(dir, themeName string) error {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(uiPrefs{Theme: themeName}, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "ui-*.json.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, uiPrefsPath(dir))
}

// resolveInitialTheme picks the startup theme. Precedence:
//   - a non-empty --theme flag decides outright: a valid name wins; an invalid
//     name falls back to the default and records a warning (it does NOT fall
//     through to the persisted preference);
//   - with no flag, a valid persisted preference wins (only when persistence is
//     enabled); otherwise the default.
//
// Any warning is appended to warns.
func resolveInitialTheme(flag string, persist bool, dir string, warns *[]string) *theme {
	flag = strings.TrimSpace(strings.ToLower(flag))
	if flag != "" {
		if t, ok := themeByName[flag]; ok {
			return t
		}
		*warns = append(*warns, fmt.Sprintf("Unknown theme %q; using %s", flag, defaultThemeName))
		return themeByName[defaultThemeName]
	}
	if persist {
		prefs, err := loadUIPrefs(dir)
		if err != nil {
			*warns = append(*warns, fmt.Sprintf("Could not read UI preferences: %v", err))
		} else if prefs.Theme != "" {
			if t, ok := themeByName[strings.ToLower(prefs.Theme)]; ok {
				return t
			}
			*warns = append(*warns, fmt.Sprintf("Unknown saved theme %q; using %s", prefs.Theme, defaultThemeName))
		}
	}
	return themeByName[defaultThemeName]
}
