package coalesce_test

import (
	"errors"
	"maps"
	"math"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/coalesce"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config coalesce.Config
		valid  bool
	}{
		"the zero config":                 {coalesce.Config{}, true},
		"the default":                     {coalesce.DefaultConfig(), true},
		"a fraction":                      {coalesce.Config{ExtendTTLFraction: 0.5}, true},
		"the whole TTL":                   {coalesce.Config{ExtendTTLFraction: 1}, true},
		"a fixed interval":                {coalesce.Config{ExtendEvery: 90 * time.Second}, true},
		"a bound of a minute":             {coalesce.Config{RunMaxAge: time.Minute}, true},
		"an interval and a bound":         {coalesce.Config{ExtendEvery: time.Minute, RunMaxAge: time.Hour}, true},
		"a negative interval":             {coalesce.Config{ExtendEvery: -time.Second}, false},
		"a negative fraction":             {coalesce.Config{ExtendTTLFraction: -0.1}, false},
		"a fraction above one":            {coalesce.Config{ExtendTTLFraction: 1.01}, false},
		"a fraction that is not a number": {coalesce.Config{ExtendTTLFraction: math.NaN()}, false},
		"an infinite fraction":            {coalesce.Config{ExtendTTLFraction: math.Inf(1)}, false},
		"a fraction and an interval":      {coalesce.Config{ExtendTTLFraction: 0.5, ExtendEvery: time.Minute}, false},
		"a negative bound":                {coalesce.Config{RunMaxAge: -time.Hour}, false},
		"a bound under a minute":          {coalesce.Config{RunMaxAge: 59 * time.Second}, false},
		"a bound of a nanosecond":         {coalesce.Config{RunMaxAge: 1}, false},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.Validate()
			_, newErr := coalesce.New(tt.config)
			if tt.valid {
				if err != nil || newErr != nil {
					t.Fatalf("Validate = %v, New = %v; want a valid config", err, newErr)
				}
				return
			}
			if !errors.Is(err, coalesce.ErrConfig) || !errors.Is(newErr, coalesce.ErrConfig) {
				t.Fatalf("Validate = %v, New = %v; want both to wrap ErrConfig", err, newErr)
			}
		})
	}
}

func TestNewReturnsNothingForABadConfig(t *testing.T) {
	t.Parallel()

	c, err := coalesce.New(coalesce.Config{ExtendEvery: -1})
	if c != nil || err == nil {
		t.Fatalf("New = %v, %v; want no coalescer and an error", c, err)
	}
}

func TestExtensionInterval(t *testing.T) {
	t.Parallel()

	const ttl = 45 * time.Minute
	tests := map[string]struct {
		config coalesce.Config
		ttl    time.Duration
		want   time.Duration
	}{
		"neither form":                {coalesce.Config{}, ttl, 0},
		"neither form, with a bound":  {coalesce.Config{RunMaxAge: time.Hour}, ttl, 0},
		"half the TTL":                {coalesce.Config{ExtendTTLFraction: 0.5}, ttl, 22*time.Minute + 30*time.Second},
		"the whole TTL":               {coalesce.Config{ExtendTTLFraction: 1}, ttl, ttl},
		"a fraction of another TTL":   {coalesce.Config{ExtendTTLFraction: 0.5}, 4 * time.Minute, 2 * time.Minute},
		"a fixed interval":            {coalesce.Config{ExtendEvery: 90 * time.Second}, ttl, 90 * time.Second},
		"a fixed interval, other TTL": {coalesce.Config{ExtendEvery: 90 * time.Second}, time.Minute, 90 * time.Second},
		"the default":                 {coalesce.DefaultConfig(), 30 * time.Minute, 15 * time.Minute},
		"a fraction of no TTL":        {coalesce.Config{ExtendTTLFraction: 0.5}, 0, 0},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := tt.config.ExtensionInterval(tt.ttl); got != tt.want {
				t.Errorf("ExtensionInterval(%s) = %s, want %s", tt.ttl, got, tt.want)
			}
		})
	}
}

func TestDefaultConfigIsTheMeasuredSetting(t *testing.T) {
	t.Parallel()

	c := coalesce.DefaultConfig()
	if c.ExtendTTLFraction != 0.5 || c.ExtendEvery != 0 || c.RunMaxAge != 30*time.Minute {
		t.Errorf("DefaultConfig = %+v, want half a TTL and 30 minutes", c)
	}
}

func TestDescribe(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config coalesce.Config
		want   map[string]string
	}{
		"the zero config": {coalesce.Config{}, map[string]string{
			"extend_ttl_fraction": "0", "extend_every": "0s", "run_max_age": "0s",
		}},
		"the default": {coalesce.DefaultConfig(), map[string]string{
			"extend_ttl_fraction": "0.5", "extend_every": "0s", "run_max_age": "30m0s",
		}},
		"a fixed interval": {coalesce.Config{ExtendEvery: 90 * time.Second, RunMaxAge: 10 * time.Minute}, map[string]string{
			"extend_ttl_fraction": "0", "extend_every": "1m30s", "run_max_age": "10m0s",
		}},
		"an odd fraction": {coalesce.Config{ExtendTTLFraction: 0.125}, map[string]string{
			"extend_ttl_fraction": "0.125", "extend_every": "0s", "run_max_age": "0s",
		}},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := tt.config.Describe(); !maps.Equal(got, tt.want) {
				t.Errorf("Describe = %v, want %v", got, tt.want)
			}
		})
	}
}
