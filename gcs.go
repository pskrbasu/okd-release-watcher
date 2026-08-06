package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"

	"k8s.io/klog/v2"
)

const (
	gcsBucket       = "test-platform-results"
	gcsAPIBase      = "https://storage.googleapis.com/storage/v1/b/" + gcsBucket + "/o"
	gcsWebBase      = "https://gcsweb-ci.apps.ci.l2s4.p1.openshiftapps.com/gcs/" + gcsBucket + "/"
	agentJobName    = "periodic-ci-openshift-release-main-claude-payload-agent-okd-scos-no-slack"
	agentJobPrefix  = "logs/" + agentJobName + "/"
	artifactSubpath = "artifacts/claude-payload-agent/openshift-claude-payload-agent/artifacts/"
)

// GCS API response types

type GCSListResponse struct {
	Prefixes      []string  `json:"prefixes,omitempty"`
	Items         []GCSItem `json:"items,omitempty"`
	NextPageToken string    `json:"nextPageToken,omitempty"`
}

type GCSItem struct {
	Name        string `json:"name"`
	TimeCreated string `json:"timeCreated"`
	Size        string `json:"size"`
}

// Agent build index maps payload tags to their analysis artifacts

type AgentBuildIndex struct {
	mu      sync.RWMutex
	entries map[string]AgentBuildEntry
}

type AgentBuildEntry struct {
	BuildID    string
	AutodlPath string
	HTMLPath   string
}

// autodl.json types

type AutodlJSON struct {
	TableName string                       `json:"table_name"`
	Schema    map[string]string            `json:"schema"`
	Rows      []map[string]json.RawMessage `json:"rows"`
}

type PayloadTriageRow struct {
	PayloadTag             string `json:"payload_tag"`
	Stream                 string `json:"stream"`
	Phase                  string `json:"phase"`
	RejectionStreak        string `json:"rejection_streak"`
	TotalBlockingJobs      string `json:"total_blocking_jobs"`
	FailedBlockingJobs     string `json:"failed_blocking_jobs"`
	JobName                string `json:"job_name"`
	ProwURL                string `json:"prow_url"`
	FailureType            string `json:"failure_type"`
	RootCauseSummary       string `json:"root_cause_summary"`
	StreakLength            string `json:"streak_length"`
	IsNewFailure           string `json:"is_new_failure"`
	CandidatePRURL         string `json:"candidate_pr_url"`
	CandidateTitle         string `json:"candidate_title"`
	CandidateConfidence    string `json:"candidate_confidence_score"`
	ForceAcceptRecommended string `json:"force_accept_recommended"`
}

func buildAgentIndex(maxBuilds int) (*AgentBuildIndex, error) {
	index := &AgentBuildIndex{
		entries: make(map[string]AgentBuildEntry),
	}

	buildIDs, err := listAgentBuilds(maxBuilds)
	if err != nil {
		return nil, fmt.Errorf("listing agent builds: %w", err)
	}

	klog.V(2).Infof("Found %d agent builds, scanning for artifacts", len(buildIDs))

	// Scan builds in parallel (bounded concurrency)
	sem := make(chan struct{}, 10)
	var wg sync.WaitGroup

	for _, buildID := range buildIDs {
		wg.Add(1)
		go func(bid string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			entries, err := scanBuildArtifacts(bid)
			if err != nil {
				klog.V(3).Infof("Error scanning build %s: %v", bid, err)
				return
			}

			index.mu.Lock()
			for tag, entry := range entries {
				if _, exists := index.entries[tag]; !exists {
					index.entries[tag] = entry
				}
			}
			index.mu.Unlock()
		}(buildID)
	}

	wg.Wait()
	klog.V(2).Infof("Agent index built with %d entries", len(index.entries))
	return index, nil
}

func listAgentBuilds(maxBuilds int) ([]string, error) {
	var allPrefixes []string
	pageToken := ""

	for {
		u := fmt.Sprintf("%s?prefix=%s&delimiter=/", gcsAPIBase, url.QueryEscape(agentJobPrefix))
		if pageToken != "" {
			u += "&pageToken=" + url.QueryEscape(pageToken)
		}

		resp, err := httpClient.Get(u)
		if err != nil {
			return nil, fmt.Errorf("GET %s: %w", u, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("GET %s returned %d: %s", u, resp.StatusCode, string(body))
		}

		var result GCSListResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, fmt.Errorf("decoding GCS response: %w", err)
		}

		for _, prefix := range result.Prefixes {
			// Extract build ID from prefix like "logs/jobname/1234567/"
			parts := strings.Split(strings.TrimSuffix(prefix, "/"), "/")
			if len(parts) > 0 {
				allPrefixes = append(allPrefixes, parts[len(parts)-1])
			}
		}

		if result.NextPageToken == "" {
			break
		}
		pageToken = result.NextPageToken
	}

	// Sort descending (newest first) — build IDs are numeric and monotonically increasing
	sort.Sort(sort.Reverse(sort.StringSlice(allPrefixes)))

	if len(allPrefixes) > maxBuilds {
		allPrefixes = allPrefixes[:maxBuilds]
	}

	return allPrefixes, nil
}

func scanBuildArtifacts(buildID string) (map[string]AgentBuildEntry, error) {
	prefix := agentJobPrefix + buildID + "/" + artifactSubpath
	u := fmt.Sprintf("%s?prefix=%s", gcsAPIBase, url.QueryEscape(prefix))

	resp, err := httpClient.Get(u)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", u, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET returned %d", resp.StatusCode)
	}

	var result GCSListResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	entries := make(map[string]AgentBuildEntry)

	for _, item := range result.Items {
		filename := item.Name[strings.LastIndex(item.Name, "/")+1:]

		// Match autodl.json files: payload-analysis-{TAG}-autodl.json
		if strings.HasPrefix(filename, "payload-analysis-") && strings.HasSuffix(filename, "-autodl.json") {
			tag := strings.TrimPrefix(filename, "payload-analysis-")
			tag = strings.TrimSuffix(tag, "-autodl.json")

			htmlFilename := fmt.Sprintf("payload-analysis-%s-summary.html", tag)
			htmlPath := prefix + htmlFilename

			entries[tag] = AgentBuildEntry{
				BuildID:    buildID,
				AutodlPath: item.Name,
				HTMLPath:   htmlPath,
			}
		}
	}

	return entries, nil
}

// GetAnalysis fetches the analysis for a given payload tag
func (idx *AgentBuildIndex) GetAnalysis(tag string) (*[]PayloadTriageRow, string, error) {
	idx.mu.RLock()
	entry, ok := idx.entries[tag]
	idx.mu.RUnlock()

	if !ok {
		return nil, "", fmt.Errorf("no analysis found for tag %s", tag)
	}

	htmlURL := gcsWebBase + entry.HTMLPath

	rows, err := fetchAutodlJSON(entry.AutodlPath)
	if err != nil {
		klog.V(2).Infof("Failed to fetch autodl.json for %s: %v", tag, err)
		return nil, htmlURL, nil
	}

	return &rows, htmlURL, nil
}

func fetchAutodlJSON(objectPath string) ([]PayloadTriageRow, error) {
	u := fmt.Sprintf("%s/%s?alt=media", gcsAPIBase, url.PathEscape(objectPath))

	resp, err := httpClient.Get(u)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", u, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GET returned %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading body: %w", err)
	}

	var autodl AutodlJSON
	if err := json.Unmarshal(body, &autodl); err != nil {
		return nil, fmt.Errorf("decoding autodl.json: %w", err)
	}

	var rows []PayloadTriageRow
	for _, rawRow := range autodl.Rows {
		rowBytes, err := json.Marshal(rawRow)
		if err != nil {
			continue
		}
		var row PayloadTriageRow
		if err := json.Unmarshal(rowBytes, &row); err != nil {
			klog.V(3).Infof("Error parsing row: %v", err)
			continue
		}
		rows = append(rows, row)
	}

	return rows, nil
}
