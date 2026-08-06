package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

const (
	acceptedStalenessLimit = 24 * time.Hour
	builtStalenessLimit    = 72 * time.Hour
)

var (
	tagDateRegex = regexp.MustCompile(`(\d{4})-(\d{2})-(\d{2})-(\d{2})(\d{2})(\d{2})$`)
	httpClient   = &http.Client{Timeout: 30 * time.Second}
)

// API response types

type ReleaseStreamTags struct {
	Name string `json:"name"`
	Tags []Tag  `json:"tags"`
}

type Tag struct {
	Name        string `json:"name"`
	Phase       string `json:"phase"`
	PullSpec    string `json:"pullSpec"`
	DownloadURL string `json:"downloadURL"`
}

type ReleaseDetail struct {
	Name    string         `json:"name"`
	Phase   string         `json:"phase"`
	Results ReleaseResults `json:"results"`
}

type ReleaseResults struct {
	BlockingJobs  map[string]JobResult `json:"blockingJobs"`
	InformingJobs map[string]JobResult `json:"informingJobs"`
	AsyncJobs     map[string]JobResult `json:"asyncJobs"`
}

type JobResult struct {
	State              string   `json:"state"`
	URL                string   `json:"url"`
	Retries            int      `json:"retries,omitempty"`
	PreviousAttemptURLs []string `json:"previousAttemptURLs,omitempty"`
	TransitionTime     string   `json:"transitionTime"`
}

// Report types

type Report struct {
	Streams   []StreamReport `json:"streams"`
	Lookback  time.Duration  `json:"lookback"`
	Generated time.Time      `json:"generated"`
}

type StreamReport struct {
	StreamName      string      `json:"stream_name"`
	StreamURL       string      `json:"stream_url"`
	TotalInWindow   int         `json:"total_in_window"`
	AcceptedCount   int         `json:"accepted_count"`
	RejectedCount   int         `json:"rejected_count"`
	FailedCount     int         `json:"failed_count"`
	AcceptedStale   bool        `json:"accepted_stale"`
	LastAcceptedAge string      `json:"last_accepted_age,omitempty"`
	LastAcceptedTag string      `json:"last_accepted_tag,omitempty"`
	BuildStale      bool        `json:"build_stale"`
	LastBuiltAge    string      `json:"last_built_age,omitempty"`
	LastBuiltTag    string      `json:"last_built_tag,omitempty"`
	FailedRejected  []TagReport `json:"failed_rejected,omitempty"`
}

type TagReport struct {
	Name            string       `json:"name"`
	Phase           string       `json:"phase"`
	BlockingFailed  []JobFailure `json:"blocking_failed,omitempty"`
	InformingFailed []JobFailure `json:"informing_failed,omitempty"`
	AnalysisHTMLURL string       `json:"analysis_html_url,omitempty"`
	RootCauses      []string     `json:"root_causes,omitempty"`
	RejectionStreak int          `json:"rejection_streak,omitempty"`
}

type JobFailure struct {
	Name    string `json:"name"`
	URL     string `json:"url"`
	Retries int    `json:"retries,omitempty"`
}

func generateReport(o *options) (*Report, error) {
	report := &Report{
		Lookback:  o.lookback,
		Generated: time.Now().UTC(),
	}

	cutoff := time.Now().UTC().Add(-o.lookback)

	// Build the GCS agent index concurrently
	var agentIndex *AgentBuildIndex
	var agentErr error
	var agentWg sync.WaitGroup
	agentWg.Add(1)
	go func() {
		defer agentWg.Done()
		agentIndex, agentErr = buildAgentIndex(100)
		if agentErr != nil {
			klog.Errorf("Failed to build agent index: %v", agentErr)
		}
	}()

	// Fetch tags for all streams in parallel
	type streamResult struct {
		idx    int
		report StreamReport
		err    error
	}
	results := make(chan streamResult, len(o.streams))

	for i, stream := range o.streams {
		go func(idx int, stream string) {
			sr, err := processStream(stream, cutoff, o.includeHealthy)
			results <- streamResult{idx: idx, report: sr, err: err}
		}(i, stream)
	}

	streamReports := make([]StreamReport, len(o.streams))
	for range o.streams {
		r := <-results
		if r.err != nil {
			klog.Errorf("Error processing stream %s: %v", o.streams[r.idx], r.err)
			streamReports[r.idx] = StreamReport{
				StreamName: o.streams[r.idx],
				StreamURL:  fmt.Sprintf("%s/#%s", releaseControllerURL, o.streams[r.idx]),
			}
			continue
		}
		streamReports[r.idx] = r.report
	}

	// Wait for agent index before enriching with analysis
	agentWg.Wait()

	if agentIndex != nil && agentErr == nil {
		enrichWithAnalysis(streamReports, agentIndex)
	}

	report.Streams = streamReports
	return report, nil
}

func processStream(stream string, cutoff time.Time, includeHealthy bool) (StreamReport, error) {
	sr := StreamReport{
		StreamName: stream,
		StreamURL:  fmt.Sprintf("%s/#%s", releaseControllerURL, stream),
	}

	tags, err := fetchStreamTags(stream)
	if err != nil {
		return sr, fmt.Errorf("fetching tags: %w", err)
	}

	// Filter tags by lookback window, only include terminal phases
	var recentTags []Tag
	for _, tag := range tags.Tags {
		if tag.Phase != "Accepted" && tag.Phase != "Rejected" && tag.Phase != "Failed" {
			continue
		}
		ts, ok := parseTagTimestamp(tag.Name)
		if !ok {
			// For tags without embedded timestamps (stable streams), include them
			recentTags = append(recentTags, tag)
			continue
		}
		if ts.After(cutoff) {
			recentTags = append(recentTags, tag)
		}
	}

	sr.TotalInWindow = len(recentTags)

	// Count and collect failed/rejected
	for _, tag := range recentTags {
		switch tag.Phase {
		case "Accepted":
			sr.AcceptedCount++
			if !includeHealthy {
				continue
			}
		case "Rejected":
			sr.RejectedCount++
		case "Failed":
			sr.FailedCount++
		}

		if tag.Phase == "Failed" || tag.Phase == "Rejected" || includeHealthy {
			tr := processTag(stream, tag)
			sr.FailedRejected = append(sr.FailedRejected, tr)
		}
	}

	// Calculate rejection streak (consecutive non-accepted from newest)
	if len(sr.FailedRejected) > 0 {
		streak := 0
		for _, tag := range recentTags {
			if tag.Phase == "Accepted" {
				break
			}
			if tag.Phase == "Rejected" || tag.Phase == "Failed" {
				streak++
			}
		}
		for i := range sr.FailedRejected {
			sr.FailedRejected[i].RejectionStreak = streak
		}
	}

	// Compute staleness across ALL tags (not just the lookback window)
	now := time.Now().UTC()
	computeStaleness(&sr, tags.Tags, now)

	return sr, nil
}

func computeStaleness(sr *StreamReport, allTags []Tag, now time.Time) {
	var foundAccepted, foundBuilt bool

	for _, tag := range allTags {
		isTerminal := tag.Phase == "Accepted" || tag.Phase == "Rejected" || tag.Phase == "Failed"
		if !isTerminal {
			continue
		}

		ts, ok := parseTagTimestamp(tag.Name)
		if !ok {
			continue
		}

		age := now.Sub(ts)

		if !foundBuilt {
			foundBuilt = true
			sr.LastBuiltTag = tag.Name
			sr.LastBuiltAge = formatAge(age)
			if age > builtStalenessLimit {
				sr.BuildStale = true
			}
		}

		if !foundAccepted && tag.Phase == "Accepted" {
			foundAccepted = true
			sr.LastAcceptedTag = tag.Name
			sr.LastAcceptedAge = formatAge(age)
			if age > acceptedStalenessLimit {
				sr.AcceptedStale = true
			}
		}

		if foundAccepted && foundBuilt {
			break
		}
	}

	if !foundAccepted {
		sr.AcceptedStale = true
		sr.LastAcceptedAge = "never"
	}
	if !foundBuilt {
		sr.BuildStale = true
		sr.LastBuiltAge = "never"
	}
}

func formatAge(d time.Duration) string {
	hours := d.Hours()
	if hours < 1 {
		return fmt.Sprintf("%.0f minutes", d.Minutes())
	}
	if hours < 48 {
		return fmt.Sprintf("%.1f hours", hours)
	}
	return fmt.Sprintf("%.1f days", hours/24)
}

func processTag(stream string, tag Tag) TagReport {
	tr := TagReport{
		Name:  tag.Name,
		Phase: tag.Phase,
	}

	if tag.Phase == "Failed" {
		// Failed builds have no job results (payload couldn't be assembled)
		return tr
	}

	detail, err := fetchReleaseDetail(stream, tag.Name)
	if err != nil {
		klog.Errorf("Error fetching detail for %s: %v", tag.Name, err)
		return tr
	}

	for name, job := range detail.Results.BlockingJobs {
		if job.State == "Failed" {
			tr.BlockingFailed = append(tr.BlockingFailed, JobFailure{
				Name:    name,
				URL:     job.URL,
				Retries: job.Retries,
			})
		}
	}
	sort.Slice(tr.BlockingFailed, func(i, j int) bool {
		return tr.BlockingFailed[i].Name < tr.BlockingFailed[j].Name
	})

	for name, job := range detail.Results.InformingJobs {
		if job.State == "Failed" {
			tr.InformingFailed = append(tr.InformingFailed, JobFailure{
				Name:    name,
				URL:     job.URL,
				Retries: job.Retries,
			})
		}
	}
	sort.Slice(tr.InformingFailed, func(i, j int) bool {
		return tr.InformingFailed[i].Name < tr.InformingFailed[j].Name
	})

	return tr
}

func enrichWithAnalysis(streams []StreamReport, index *AgentBuildIndex) {
	var wg sync.WaitGroup
	for i := range streams {
		for j := range streams[i].FailedRejected {
			if streams[i].FailedRejected[j].Phase == "Accepted" {
				continue
			}
			wg.Add(1)
			go func(sr *StreamReport, tr *TagReport) {
				defer wg.Done()
				rows, htmlURL, err := index.GetAnalysis(tr.Name)
				if err != nil {
					klog.V(2).Infof("No analysis found for %s: %v", tr.Name, err)
					return
				}
				tr.AnalysisHTMLURL = htmlURL
				if rows != nil {
					for _, row := range *rows {
						if row.RootCauseSummary != "" {
							tr.RootCauses = append(tr.RootCauses, fmt.Sprintf("[%s] %s", row.JobName, row.RootCauseSummary))
						}
					}
				}
			}(&streams[i], &streams[i].FailedRejected[j])
		}
	}
	wg.Wait()
}

func fetchStreamTags(stream string) (*ReleaseStreamTags, error) {
	url := fmt.Sprintf("%s/api/v1/releasestream/%s/tags", releaseControllerURL, stream)
	klog.V(2).Infof("Fetching tags from %s", url)

	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GET %s returned %d: %s", url, resp.StatusCode, string(body))
	}

	var tags ReleaseStreamTags
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
		return nil, fmt.Errorf("decoding response from %s: %w", url, err)
	}

	return &tags, nil
}

func fetchReleaseDetail(stream, tagName string) (*ReleaseDetail, error) {
	url := fmt.Sprintf("%s/api/v1/releasestream/%s/release/%s", releaseControllerURL, stream, tagName)
	klog.V(2).Infof("Fetching release detail from %s", url)

	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GET %s returned %d: %s", url, resp.StatusCode, string(body))
	}

	var detail ReleaseDetail
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		return nil, fmt.Errorf("decoding response from %s: %w", url, err)
	}

	return &detail, nil
}

func parseTagTimestamp(tagName string) (time.Time, bool) {
	matches := tagDateRegex.FindStringSubmatch(tagName)
	if matches == nil {
		return time.Time{}, false
	}
	dateStr := fmt.Sprintf("%s-%s-%s-%s%s%s", matches[1], matches[2], matches[3], matches[4], matches[5], matches[6])
	t, err := time.Parse("2006-01-02-150405", dateStr)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

// String formats the report for human-readable CLI output
func (r *Report) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "=== OKD Release Stream Report ===\n")
	fmt.Fprintf(&b, "Lookback: %s | Generated: %s\n\n", r.Lookback, r.Generated.Format(time.RFC3339))

	totalFailures := 0
	streamsWithFailures := 0
	staleStreams := 0

	for _, sr := range r.Streams {
		fmt.Fprintf(&b, "--- %s ---\n", sr.StreamName)
		fmt.Fprintf(&b, "%s\n", sr.StreamURL)

		if sr.TotalInWindow == 0 {
			fmt.Fprintf(&b, "  No builds in window\n")
		} else {
			fmt.Fprintf(&b, "  %d builds in window", sr.TotalInWindow)
			parts := []string{}
			if sr.AcceptedCount > 0 {
				parts = append(parts, fmt.Sprintf("%d Accepted", sr.AcceptedCount))
			}
			if sr.RejectedCount > 0 {
				parts = append(parts, fmt.Sprintf("%d Rejected", sr.RejectedCount))
			}
			if sr.FailedCount > 0 {
				parts = append(parts, fmt.Sprintf("%d Failed", sr.FailedCount))
			}
			if len(parts) > 0 {
				fmt.Fprintf(&b, " | %s", strings.Join(parts, " | "))
			}
			fmt.Fprintf(&b, "\n")
		}

		if sr.AcceptedStale {
			if sr.LastAcceptedTag != "" {
				fmt.Fprintf(&b, "  WARNING: No accepted payload in %s (last: %s)\n", sr.LastAcceptedAge, sr.LastAcceptedTag)
			} else {
				fmt.Fprintf(&b, "  WARNING: No accepted payload found\n")
			}
		}
		if sr.BuildStale {
			if sr.LastBuiltTag != "" {
				fmt.Fprintf(&b, "  WARNING: No payload built in %s (last: %s)\n", sr.LastBuiltAge, sr.LastBuiltTag)
			} else {
				fmt.Fprintf(&b, "  WARNING: No payload built\n")
			}
		}

		if sr.AcceptedStale || sr.BuildStale {
			staleStreams++
		}

		failCount := sr.RejectedCount + sr.FailedCount
		if failCount > 0 {
			streamsWithFailures++
			totalFailures += failCount
		}

		if len(sr.FailedRejected) == 0 && !sr.AcceptedStale && !sr.BuildStale {
			fmt.Fprintf(&b, "  No issues detected\n")
		}

		for _, tr := range sr.FailedRejected {
			fmt.Fprintf(&b, "\n  %s: %s\n", strings.ToUpper(tr.Phase), tr.Name)

			if len(tr.BlockingFailed) > 0 {
				names := make([]string, len(tr.BlockingFailed))
				for i, jf := range tr.BlockingFailed {
					names[i] = jf.Name
				}
				fmt.Fprintf(&b, "    Blocking:  %s\n", strings.Join(names, ", "))
			}

			if len(tr.InformingFailed) > 0 {
				names := make([]string, len(tr.InformingFailed))
				for i, jf := range tr.InformingFailed {
					names[i] = jf.Name
				}
				fmt.Fprintf(&b, "    Informing: %s\n", strings.Join(names, ", "))
			}

			if tr.Phase == "Failed" && len(tr.BlockingFailed) == 0 {
				fmt.Fprintf(&b, "    Payload could not be assembled (no jobs ran)\n")
			}

			if tr.AnalysisHTMLURL != "" {
				fmt.Fprintf(&b, "    Analysis:  %s\n", tr.AnalysisHTMLURL)
			}

			if len(tr.RootCauses) > 0 {
				for _, rc := range tr.RootCauses {
					fmt.Fprintf(&b, "    Root cause: %s\n", rc)
				}
			}

			if tr.RejectionStreak > 1 {
				fmt.Fprintf(&b, "    Streak:    %d consecutive rejections\n", tr.RejectionStreak)
			}
		}
		fmt.Fprintf(&b, "\n")
	}

	fmt.Fprintf(&b, "--- Summary ---\n")
	fmt.Fprintf(&b, "Streams: %d | With failures: %d | Stale: %d | Failed/Rejected builds: %d\n",
		len(r.Streams), streamsWithFailures, staleStreams, totalFailures)

	return b.String()
}

// JSON formats the report as JSON
func (r *Report) JSON() string {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		klog.Errorf("Error marshaling report to JSON: %v", err)
		return "{}"
	}
	return string(data)
}
