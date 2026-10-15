package itbench

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

// Alerts is what a scenario's alert files give the window: the earliest time an
// alert became active, the time of the latest alerts_at file, and the time of the
// alerts_in_alerting_state file.
type Alerts struct {
	// FirstActive is the earliest activeAt over every alert of every file, or the
	// zero time if there is none.
	FirstActive time.Time
	// LastAt is the time in the name of the latest alerts_at file, or zero.
	LastAt time.Time
	// AlertingState is the time in the name of the latest alerts_in_alerting_state
	// file, or zero.
	AlertingState time.Time
	// Files counts the files read.
	Files int
}

var (
	alertsAtName    = regexp.MustCompile(`^alerts_at_(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}(?:\.\d+)?)\.json$`)
	alertingName    = regexp.MustCompile(`^alerts_in_alerting_state_(\d{4}-\d{2}-\d{2}T\d{6}(?:\.\d+)?)Z\.json$`)
	alertsAtLayout  = "2006-01-02T15-04-05.999999999"
	alertingLayout  = "2006-01-02T150405.999999999"
	errAlertsFormat = fmt.Errorf("itbench: an alerts file of a shape that is not read")
)

// ReadAlerts reads a scenario's alerts: the files in its alerts folder and the
// alerts_in_alerting_state file that may sit in the scenario folder itself. An
// alerts_at file is a JSON array of alerts; an alerts_in_alerting_state file is an
// object with the alerts under data. Only activeAt is read from an alert.
func ReadAlerts(scenario string) (Alerts, error) {
	var a Alerts
	for _, dir := range []string{filepath.Join(scenario, "alerts"), scenario} {
		ents, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return Alerts{}, err
		}
		for _, e := range ents {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			switch {
			case alertsAtName.MatchString(name):
				if dir == scenario {
					continue
				}
				t, err := time.ParseInLocation(alertsAtLayout, alertsAtName.FindStringSubmatch(name)[1], time.UTC)
				if err != nil {
					return Alerts{}, fmt.Errorf("itbench: the time of an alerts_at file: %w", err)
				}
				if t.After(a.LastAt) {
					a.LastAt = t
				}
				if err := a.read(filepath.Join(dir, name), false); err != nil {
					return Alerts{}, err
				}
			case alertingName.MatchString(name):
				t, err := time.ParseInLocation(alertingLayout, alertingName.FindStringSubmatch(name)[1], time.UTC)
				if err != nil {
					return Alerts{}, fmt.Errorf("itbench: the time of an alerts_in_alerting_state file: %w", err)
				}
				if t.After(a.AlertingState) {
					a.AlertingState = t
				}
				if err := a.read(filepath.Join(dir, name), true); err != nil {
					return Alerts{}, err
				}
			}
		}
	}
	return a, nil
}

type alert struct {
	ActiveAt string `json:"activeAt"`
}

func (a *Alerts) read(path string, wrapped bool) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var list []alert
	if wrapped {
		var w struct {
			Data struct {
				Alerts []alert `json:"alerts"`
			} `json:"data"`
		}
		if err := json.Unmarshal(b, &w); err != nil {
			return fmt.Errorf("%w: %s", errAlertsFormat, base(path))
		}
		list = w.Data.Alerts
	} else if err := json.Unmarshal(b, &list); err != nil {
		return fmt.Errorf("%w: %s", errAlertsFormat, base(path))
	}
	a.Files++
	for _, al := range list {
		if al.ActiveAt == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339, al.ActiveAt)
		if err != nil {
			return fmt.Errorf("itbench: an activeAt that is not RFC 3339 in %s", base(path))
		}
		t = t.UTC()
		if a.FirstActive.IsZero() || t.Before(a.FirstActive) {
			a.FirstActive = t
		}
	}
	return nil
}
