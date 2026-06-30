package main

import (
	"strings"
	"testing"
)

func TestListHelpFiveRowsPerColumn(t *testing.T) {
	dracItems := [][2]string{
		{"↑/↓", "Select"}, {"pgup/pgdn", "Page"},
		{"enter", "Details"}, {"space", "Pause/Resume"}, {"o", "Open Folder"},
		{"a", "Add"}, {"d", "Down Limit"}, {"u", "Up Limit"},
		{"x", "Delete Task"}, {"X", "Delete Task & Files"}, {"t", "Theme"}, {"q", "Quit"},
	}
	monoItems := [][2]string{
		{"↑/↓", "Select"}, {"pgup/pgdn", "Page"},
		{"enter", "Details"}, {"space", "Pause/Resume"}, {"o", "Open"}, {"a", "Add"},
		{"d", "Down"}, {"u", "Up"}, {"x", "Delete"}, {"X", "Delete+Files"}, {"t", "Theme"}, {"q", "Quit"},
	}
	for _, th := range themes {
		items := dracItems
		if th.name == "mono" {
			items = monoItems
		}
		// 12 items, 5 rows per column => 3 columns (5,5,2), 5 rows tall.
		help := renderHelpColumns(items, helpRowsPerColumn, th.styles, 115)
		rows := strings.Count(help, "\n") + 1
		if rows != 5 {
			t.Errorf("theme=%s w=115 help has %d rows, want 5:\n%s", th.name, rows, help)
		}
		// Every rendered line must fit the body width.
		bw := bodyWidth(115)
		for i, ln := range strings.Split(help, "\n") {
			if dw := dispWidth(ln); dw > bw {
				t.Errorf("theme=%s help line %d width %d > body %d: %q", th.name, i, dw, bw, ln)
			}
		}
		// Narrow terminal: width wins, footer may grow taller but never overruns.
		narrow := renderHelpColumns(items, helpRowsPerColumn, th.styles, 30)
		for i, ln := range strings.Split(narrow, "\n") {
			if dw := dispWidth(ln); dw > bodyWidth(30) {
				t.Errorf("theme=%s w=30 help line %d width %d > body %d: %q", th.name, i, dw, bodyWidth(30), ln)
			}
		}
	}
}
