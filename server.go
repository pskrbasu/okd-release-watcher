package main

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

	"k8s.io/klog/v2"
)

func (o *options) serve() error {
	token := os.Getenv("TOKEN")
	if token == "" {
		return fmt.Errorf("TOKEN environment variable is required")
	}

	// Dedup cache to avoid processing the same Slack event twice
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

		// Handle Slack URL verification challenge
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

		// Only process messages (not subtypes like bot messages, edits, etc.)
		if eventData["type"] != "message" || eventData["subtype"] != nil {
			w.WriteHeader(http.StatusOK)
			return
		}

		ts, _ := eventData["ts"].(string)
		channel, _ := eventData["channel"].(string)
		text, _ := eventData["text"].(string)

		// Deduplicate
		if _, loaded := msgCache.LoadOrStore(ts, true); loaded {
			w.WriteHeader(http.StatusOK)
			return
		}

		w.WriteHeader(http.StatusOK)

		go func() {
			o.handleMessage(token, channel, ts, text)
		}()
	})

	klog.Infof("Starting bot server on :8080")
	return http.ListenAndServe(":8080", nil)
}

func (o *options) handleMessage(token, channel, ts, text string) {
	text = strings.ToLower(strings.TrimSpace(text))

	// Strip any bot mention from the text
	if idx := strings.Index(text, ">"); idx >= 0 && strings.HasPrefix(text, "<@") {
		text = strings.TrimSpace(text[idx+1:])
	}

	switch {
	case strings.HasPrefix(text, "help"):
		o.sendHelp(token, channel, ts)
	case strings.HasPrefix(text, "report"):
		o.handleReport(token, channel, ts, text)
	}
}

func (o *options) sendHelp(token, channel, ts string) {
	help := `*OKD Payload Reporter*
Available commands:
• *report* — Generate a release stream health report
  Arguments:
  - lookback=<duration> (default: 24h)
  - streams=<stream1>,<stream2> (default: ` + strings.Join(defaultStreams, ", ") + `)
  - healthy — Include accepted builds
• *help* — Show this message

Example: report lookback=48h healthy`

	sendSlackMessage(token, channel, ts, help)
}

func (o *options) handleReport(token, channel, ts, text string) {
	// Parse inline arguments from the message
	reportOpts := &options{
		streams:        o.streams,
		lookback:       o.lookback,
		includeHealthy: o.includeHealthy,
		slackAlias:     o.slackAlias,
	}

	parts := strings.Fields(text)
	for _, part := range parts[1:] { // skip "report" itself
		switch {
		case strings.HasPrefix(part, "lookback="):
			if d, err := time.ParseDuration(strings.TrimPrefix(part, "lookback=")); err == nil {
				reportOpts.lookback = d
			}
		case strings.HasPrefix(part, "streams="):
			reportOpts.streams = strings.Split(strings.TrimPrefix(part, "streams="), ",")
		case part == "healthy":
			reportOpts.includeHealthy = true
		case part == "tag":
			if o.slackAlias != "" {
				// Will be handled in the message prefix
			}
		}
	}

	report, err := generateReport(reportOpts)
	if err != nil {
		sendSlackMessage(token, channel, ts, fmt.Sprintf("Error generating report: %v", err))
		return
	}

	// Build summary for the top-level message
	summary := formatSlackSummary(report, o.slackAlias)
	detail := formatSlackDetail(report)

	// Post summary as reply, detail as thread
	sendSlackMessage(token, channel, ts, summary)
	if detail != "" {
		sendSlackMessage(token, channel, ts, detail)
	}
}

func formatSlackSummary(r *Report, slackAlias string) string {
	var b strings.Builder

	if slackAlias != "" {
		fmt.Fprintf(&b, "<!subteam^%s> ", slackAlias)
	}

	totalFailures := 0
	for _, sr := range r.Streams {
		totalFailures += sr.RejectedCount + sr.FailedCount
	}

	if totalFailures == 0 {
		fmt.Fprintf(&b, "All monitored OKD release streams are healthy (lookback: %s)", r.Lookback)
	} else {
		fmt.Fprintf(&b, "OKD Release Report — %d failed/rejected builds across %d streams (lookback: %s)",
			totalFailures, len(r.Streams), r.Lookback)
	}

	return b.String()
}

func formatSlackDetail(r *Report) string {
	var b strings.Builder

	for _, sr := range r.Streams {
		if sr.RejectedCount+sr.FailedCount == 0 && len(sr.FailedRejected) == 0 {
			continue
		}

		fmt.Fprintf(&b, "<%s|%s>\n", sr.StreamURL, sr.StreamName)
		fmt.Fprintf(&b, "  %d builds | %d Accepted | %d Rejected | %d Failed\n",
			sr.TotalInWindow, sr.AcceptedCount, sr.RejectedCount, sr.FailedCount)

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
