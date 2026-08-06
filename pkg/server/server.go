package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pskrbasu/okd-release-watcher/pkg/report"
	"k8s.io/klog/v2"
)

var httpClient = &http.Client{Timeout: 30 * time.Second}

func Serve(o *report.Options) error {
	token := os.Getenv("TOKEN")
	if token == "" {
		return fmt.Errorf("TOKEN environment variable is required")
	}

	var msgCache sync.Map

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			klog.Errorf("Error reading request body: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		var event map[string]interface{}
		if err := json.Unmarshal(body, &event); err != nil {
			klog.Errorf("Error parsing event: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		if event["type"] == "url_verification" {
			w.Header().Set("Content-Type", "application/json")
			challenge := map[string]string{"challenge": event["challenge"].(string)}
			json.NewEncoder(w).Encode(challenge)
			return
		}

		if event["type"] != "event_callback" {
			w.WriteHeader(http.StatusOK)
			return
		}

		eventData, ok := event["event"].(map[string]interface{})
		if !ok {
			w.WriteHeader(http.StatusOK)
			return
		}

		if eventData["type"] != "message" || eventData["subtype"] != nil {
			w.WriteHeader(http.StatusOK)
			return
		}

		ts, _ := eventData["ts"].(string)
		channel, _ := eventData["channel"].(string)
		text, _ := eventData["text"].(string)

		if _, loaded := msgCache.LoadOrStore(ts, true); loaded {
			w.WriteHeader(http.StatusOK)
			return
		}

		w.WriteHeader(http.StatusOK)

		go func() {
			handleMessage(o, token, channel, ts, text)
		}()
	})

	klog.Infof("Starting bot server on :8080")
	return http.ListenAndServe(":8080", nil)
}

func handleMessage(o *report.Options, token, channel, ts, text string) {
	text = strings.ToLower(strings.TrimSpace(text))

	if idx := strings.Index(text, ">"); idx >= 0 && strings.HasPrefix(text, "<@") {
		text = strings.TrimSpace(text[idx+1:])
	}

	switch {
	case strings.HasPrefix(text, "help"):
		sendHelp(o, token, channel, ts)
	case strings.HasPrefix(text, "report"):
		handleReport(o, token, channel, ts, text)
	}
}

func sendHelp(o *report.Options, token, channel, ts string) {
	help := `*OKD Payload Reporter*
Available commands:
• *report* — Generate a release stream health report
  Arguments:
  - lookback=<duration> (default: 24h)
  - streams=<stream1>,<stream2> (default: ` + strings.Join(report.DefaultStreams, ", ") + `)
  - healthy — Include accepted builds
• *help* — Show this message

Example: report lookback=48h healthy`

	sendSlackMessage(token, channel, ts, help)
}

func handleReport(o *report.Options, token, channel, ts, text string) {
	reportOpts := &report.Options{
		Streams:        o.Streams,
		Lookback:       o.Lookback,
		IncludeHealthy: o.IncludeHealthy,
		SlackAlias:     o.SlackAlias,
	}

	parts := strings.Fields(text)
	for _, part := range parts[1:] {
		switch {
		case strings.HasPrefix(part, "lookback="):
			if d, err := time.ParseDuration(strings.TrimPrefix(part, "lookback=")); err == nil {
				reportOpts.Lookback = d
			}
		case strings.HasPrefix(part, "streams="):
			reportOpts.Streams = strings.Split(strings.TrimPrefix(part, "streams="), ",")
		case part == "healthy":
			reportOpts.IncludeHealthy = true
		}
	}

	r, err := report.GenerateReport(reportOpts)
	if err != nil {
		sendSlackMessage(token, channel, ts, fmt.Sprintf("Error generating report: %v", err))
		return
	}

	summary := formatSlackSummary(r, o.SlackAlias)
	detail := formatSlackDetail(r)

	sendSlackMessage(token, channel, ts, summary)
	if detail != "" {
		sendSlackMessage(token, channel, ts, detail)
	}
}

func formatSlackSummary(r *report.Report, slackAlias string) string {
	var b strings.Builder

	if slackAlias != "" {
		fmt.Fprintf(&b, "<!subteam^%s> ", slackAlias)
	}

	totalFailures := 0
	staleCount := 0
	for _, sr := range r.Streams {
		totalFailures += sr.RejectedCount + sr.FailedCount
		if sr.AcceptedStale || sr.BuildStale {
			staleCount++
		}
	}

	if totalFailures == 0 && staleCount == 0 {
		fmt.Fprintf(&b, "All monitored OKD release streams are healthy (lookback: %s)", r.Lookback)
	} else {
		parts := []string{}
		if totalFailures > 0 {
			parts = append(parts, fmt.Sprintf("%d failed/rejected builds", totalFailures))
		}
		if staleCount > 0 {
			parts = append(parts, fmt.Sprintf("%d stale streams", staleCount))
		}
		fmt.Fprintf(&b, "OKD Release Report — %s (lookback: %s)",
			strings.Join(parts, ", "), r.Lookback)
	}

	return b.String()
}

func formatSlackDetail(r *report.Report) string {
	var b strings.Builder

	for _, sr := range r.Streams {
		hasIssues := sr.RejectedCount+sr.FailedCount > 0 || sr.AcceptedStale || sr.BuildStale
		if !hasIssues {
			continue
		}

		fmt.Fprintf(&b, "<%s|%s>\n", sr.StreamURL, sr.StreamName)
		fmt.Fprintf(&b, "  %d builds | %d Accepted | %d Rejected | %d Failed\n",
			sr.TotalInWindow, sr.AcceptedCount, sr.RejectedCount, sr.FailedCount)

		if sr.LatestTag != "" {
			fmt.Fprintf(&b, "  Latest: %s (*%s*)\n", sr.LatestTag, sr.LatestPhase)
		}

		if sr.AcceptedStale {
			if sr.LastAcceptedTag != "" {
				fmt.Fprintf(&b, "  *WARNING:* No accepted payload in %s (last: %s)\n", sr.LastAcceptedAge, sr.LastAcceptedTag)
			} else {
				fmt.Fprintf(&b, "  *WARNING:* No accepted payload found\n")
			}
		}
		if sr.BuildStale {
			if sr.LastBuiltTag != "" {
				fmt.Fprintf(&b, "  *WARNING:* No payload built in %s (last: %s)\n", sr.LastBuiltAge, sr.LastBuiltTag)
			} else {
				fmt.Fprintf(&b, "  *WARNING:* No payload built\n")
			}
		}

		for _, tr := range sr.FailedRejected {
			if tr.Phase == "Accepted" {
				continue
			}
			fmt.Fprintf(&b, "\n  *%s:* %s\n", strings.ToUpper(tr.Phase), tr.Name)

			for _, jf := range tr.BlockingFailed {
				retryStr := ""
				if jf.Retries > 0 {
					retryStr = fmt.Sprintf(", %d retries", jf.Retries)
				}
				fmt.Fprintf(&b, "    • [blocking] %s (Failed%s)\n", jf.Name, retryStr)
			}
			for _, jf := range tr.InformingFailed {
				retryStr := ""
				if jf.Retries > 0 {
					retryStr = fmt.Sprintf(", %d retries", jf.Retries)
				}
				fmt.Fprintf(&b, "    • [informing] %s (Failed%s)\n", jf.Name, retryStr)
			}

			if tr.AnalysisHTMLURL != "" {
				fmt.Fprintf(&b, "    <%s|Claude Analysis>\n", tr.AnalysisHTMLURL)
			}

			for _, rc := range tr.RootCauses {
				fmt.Fprintf(&b, "    _Root cause:_ %s\n", rc)
			}
		}
		fmt.Fprintf(&b, "\n")
	}

	return b.String()
}

func sendSlackMessage(token, channel, threadTS, text string) {
	payload := map[string]string{
		"channel":   channel,
		"text":      text,
		"thread_ts": threadTS,
	}

	body, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST", "https://slack.com/api/chat.postMessage", bytes.NewReader(body))
	if err != nil {
		klog.Errorf("Error creating Slack request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := httpClient.Do(req)
	if err != nil {
		klog.Errorf("Error sending Slack message: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		klog.Errorf("Slack API returned %d: %s", resp.StatusCode, string(respBody))
	}
}
