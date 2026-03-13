// ssh_cpu_check is a Go binary that replaces the Python ThreadPoolExecutor SSH
// loop in check_cpu_usage.py. It reads a JSON list of servers from stdin,
// connects to each one concurrently via SSH, samples /proc/stat twice with a
// 1-second gap to measure per-CPU idle/busy status, and writes JSON results to
// stdout. Progress and errors are written to stderr.
//
// Build:
//   go mod tidy
//   go build -o ssh_cpu_check ./ssh_cpu_check.go
//
// Input JSON (stdin):
//   {"user": "archy", "servers": [{"idx": 1, "server": "TA-...", "team": "TAO"}, ...]}
//
// Output JSON (stdout):
//   {"results": [{"idx": 1, "server": "TA-...", "team": "TAO", "sockets": 2,
//     "iso_cpus": "2-5", "pct_busy_socket0": "50.00", ...}, ...]}
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// ---------- Input / Output types ----------

type ServerInput struct {
	Idx    int    `json:"idx"`
	Server string `json:"server"`
	Team   string `json:"team"`
}

type Input struct {
	User    string        `json:"user"`
	Servers []ServerInput `json:"servers"`
}

type ServerResult struct {
	Idx            int    `json:"idx"`
	Server         string `json:"server"`
	Team           string `json:"team"`
	Sockets        int    `json:"sockets"`
	IsoCPUs        string `json:"iso_cpus"`
	PctBusySocket0 string `json:"pct_busy_socket0"`
	PctBusySocket1 string `json:"pct_busy_socket1"`
	PctFreeSocket0 string `json:"pct_free_socket0"`
	PctFreeSocket1 string `json:"pct_free_socket1"`
	BusySocket0    string `json:"busy_socket0"`
	BusySocket1    string `json:"busy_socket1"`
	IdleSocket0    string `json:"idle_socket0"`
	IdleSocket1    string `json:"idle_socket1"`
	Error          string `json:"error,omitempty"`
}

type Output struct {
	Results []ServerResult `json:"results"`
}

// ---------- CPU list parsing ----------

// parseCPUList parses a CPU list like "2,3,5-7" into [2,3,5,6,7].
// Mirrors Python's parse_cpu_list.
func parseCPUList(s string) []int {
	s = strings.TrimSpace(s)
	if s == "" || strings.ToLower(s) == "none" {
		return nil
	}
	seen := map[int]bool{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if strings.Contains(part, "-") {
			ends := strings.SplitN(part, "-", 2)
			start, err1 := strconv.Atoi(strings.TrimSpace(ends[0]))
			end, err2 := strconv.Atoi(strings.TrimSpace(ends[1]))
			if err1 == nil && err2 == nil {
				for i := start; i <= end; i++ {
					seen[i] = true
				}
			}
		} else if part != "" {
			if n, err := strconv.Atoi(part); err == nil {
				seen[n] = true
			}
		}
	}
	result := make([]int, 0, len(seen))
	for k := range seen {
		result = append(result, k)
	}
	sort.Ints(result)
	return result
}

// ---------- lscpu output parsing ----------

// parseLscpuOutput parses the combined stdout of:
//
//	"cat /sys/devices/system/cpu/isolated 2>/dev/null || grep -o 'isolcpus=[^ ]*' /proc/cmdline | cut -d= -f2"
//	" && lscpu | grep -E 'Socket|NUMA'"
//
// Returns (sockets, socketCPUSets, isoCPUs).
// socketCPUSets maps socket index → CPU list; nil when no isolated CPUs found.
// Mirrors Python's parse_lscpu_output.
func parseLscpuOutput(raw string) (int, map[int][]int, string) {
	lines := strings.Split(raw, "\n")
	isoCPUs := ""
	lscpuStart := 0

	// First line: isolated CPUs if it contains only digits, commas, hyphens, spaces.
	if len(lines) > 0 {
		first := strings.TrimSpace(lines[0])
		onlyDigitCommaHyphen := first != "" && func() bool {
			for _, c := range first {
				if !strings.ContainsRune("0123456789,- ", c) {
					return false
				}
			}
			return true
		}()
		if onlyDigitCommaHyphen {
			isoCPUs = strings.ReplaceAll(first, " ", "")
			lscpuStart = 1
		}
	}

	// Find socket count by scanning for "Socket(s):" line.
	sockets := 0
	for _, line := range lines[lscpuStart:] {
		if strings.HasPrefix(strings.TrimSpace(line), "Socket(s):") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				if n, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
					sockets = n
				}
			}
			break
		}
	}

	// If no isolated CPUs, socket CPU sets are unavailable (mirrors Python returning None).
	if isoCPUs == "" {
		return sockets, nil, isoCPUs
	}

	// Parse per-socket CPU ranges.
	// Python hardcodes index x+3 (iso line at 0, Socket(s) at 1, NUMA node(s) at 2,
	// then NUMA node0 CPU(s) at 3, node1 at 4, …).
	socketCPUSets := map[int][]int{}
	for x := 0; x < sockets; x++ {
		idx := x + 3
		if idx >= len(lines) {
			break
		}
		line := lines[idx]
		colonIdx := strings.Index(line, ":")
		if colonIdx < 0 {
			continue
		}
		rangeStr := strings.Join(strings.Fields(line[colonIdx+1:]), "")
		socketCPUSets[x] = parseCPUList(rangeStr)
	}
	return sockets, socketCPUSets, isoCPUs
}

// ---------- SSH helpers ----------

func runCommand(client *ssh.Client, cmd string) (string, error) {
	sess, err := client.NewSession()
	if err != nil {
		return "", err
	}
	defer sess.Close()
	out, err := sess.Output(cmd)
	return string(out), err
}

func parseProcStat(out string) map[int][]int {
	m := map[int][]int{}
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "cpu") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		cpuStr := parts[0][3:] // strip "cpu"
		cpu, err := strconv.Atoi(cpuStr)
		if err != nil {
			continue // skip the aggregate "cpu" line
		}
		times := make([]int, 0, len(parts)-1)
		for _, t := range parts[1:] {
			if n, err := strconv.Atoi(t); err == nil {
				times = append(times, n)
			}
		}
		m[cpu] = times
	}
	return m
}

type cpuStatus struct {
	cpu    int
	status string // "Busy", "Idle", or "Unknown"
}

// getCPUIdleStatus mirrors Python's get_cpu_idle_status: reads /proc/stat
// twice with a 1-second gap and computes busy/idle per CPU.
func getCPUIdleStatus(client *ssh.Client, isoCPUs string) ([]cpuStatus, error) {
	isoCPUList := parseCPUList(isoCPUs)
	isoCPUSet := make(map[int]bool, len(isoCPUList))
	for _, c := range isoCPUList {
		isoCPUSet[c] = true
	}

	var cpuIndices []int
	if len(isoCPUList) == 0 {
		out, err := runCommand(client, "nproc")
		if err != nil {
			return nil, fmt.Errorf("nproc: %w", err)
		}
		n, err := strconv.Atoi(strings.TrimSpace(out))
		if err != nil {
			return nil, fmt.Errorf("parsing nproc: %w", err)
		}
		for i := 0; i < n; i++ {
			cpuIndices = append(cpuIndices, i)
		}
	} else {
		cpuIndices = isoCPUList
	}

	stat1, err := runCommand(client, "cat /proc/stat | grep '^cpu[0-9]'")
	if err != nil {
		return nil, fmt.Errorf("first /proc/stat read: %w", err)
	}
	time.Sleep(1 * time.Second)
	stat2, err := runCommand(client, "cat /proc/stat | grep '^cpu[0-9]'")
	if err != nil {
		return nil, fmt.Errorf("second /proc/stat read: %w", err)
	}

	times1 := parseProcStat(stat1)
	times2 := parseProcStat(stat2)

	results := make([]cpuStatus, 0, len(cpuIndices))
	for _, cpu := range cpuIndices {
		t1, t2 := times1[cpu], times2[cpu]
		status := "Unknown"
		if len(t1) >= 4 && len(t2) >= 4 {
			idle1 := t1[3]
			if len(t1) > 4 {
				idle1 += t1[4]
			}
			idle2 := t2[3]
			if len(t2) > 4 {
				idle2 += t2[4]
			}
			var total1, total2 int
			for _, v := range t1 {
				total1 += v
			}
			for _, v := range t2 {
				total2 += v
			}
			totalDelta := total2 - total1
			var usage float64
			if totalDelta > 0 {
				usage = 100.0 * (1.0 - float64(idle2-idle1)/float64(totalDelta))
			}
			if usage > 1.0 {
				status = "Busy"
			} else {
				status = "Idle"
			}
		}
		results = append(results, cpuStatus{cpu: cpu, status: status})
	}
	return results, nil
}

// ---------- SSH auth ----------

// loadPrivateKey parses a private key file. Returns an error (with a clear
// message) if the key is passphrase-protected.
func loadPrivateKey(path string) (ssh.Signer, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	signer, err := ssh.ParsePrivateKey(b)
	if err != nil {
		var passErr *ssh.PassphraseMissingError
		if errors.As(err, &passErr) {
			return nil, fmt.Errorf("passphrase-protected (add it to ssh-agent with ssh-add)")
		}
		return nil, err
	}
	return signer, nil
}

// buildAuthMethods builds SSH auth methods by trying, in order:
//  1. SSH agent via SSH_AUTH_SOCK
//  2. Key file from SSH_IDENTITY_FILE env var (explicit override)
//  3. All ~/.ssh/id_* files that are not .pub files
//
// Returns auth methods and the agent connection (caller should defer Close).
func buildAuthMethods() ([]ssh.AuthMethod, net.Conn) {
	var methods []ssh.AuthMethod
	var agentConn net.Conn

	// 1. SSH agent
	if sock := os.Getenv("SSH_AUTH_SOCK"); sock != "" {
		conn, err := net.Dial("unix", sock)
		if err != nil {
			log.Printf("SSH_AUTH_SOCK set but dial failed: %v", err)
		} else {
			agentConn = conn
			agentClient := agent.NewClient(conn)
			signers, err := agentClient.Signers()
			if err != nil {
				log.Printf("SSH agent: Signers() failed: %v", err)
			} else if len(signers) == 0 {
				log.Printf("SSH agent: connected but has 0 keys (run: ssh-add ~/.ssh/your_key)")
			} else {
				log.Printf("SSH agent: %d key(s) available", len(signers))
				methods = append(methods, ssh.PublicKeys(signers...))
			}
		}
	} else {
		log.Printf("SSH_AUTH_SOCK not set; skipping agent")
	}

	// 2. Explicit key override
	if keyPath := os.Getenv("SSH_IDENTITY_FILE"); keyPath != "" {
		signer, err := loadPrivateKey(keyPath)
		if err != nil {
			log.Printf("SSH_IDENTITY_FILE=%s: %v", keyPath, err)
		} else {
			log.Printf("Loaded key from SSH_IDENTITY_FILE: %s", keyPath)
			methods = append(methods, ssh.PublicKeys(signer))
		}
	}

	// 3. All unencrypted private key files in ~/.ssh/ — both id_* and *.pem
	home, _ := os.UserHomeDir()
	seen := map[string]bool{}
	for _, pattern := range []string{
		filepath.Join(home, ".ssh", "id_*"),
		filepath.Join(home, ".ssh", "*.pem"),
	} {
		matches, _ := filepath.Glob(pattern)
		for _, keyPath := range matches {
			if strings.HasSuffix(keyPath, ".pub") || seen[keyPath] {
				continue
			}
			seen[keyPath] = true
			signer, err := loadPrivateKey(keyPath)
			if err != nil {
				log.Printf("Key %s: %v", filepath.Base(keyPath), err)
				continue
			}
			log.Printf("Loaded key: %s", filepath.Base(keyPath))
			methods = append(methods, ssh.PublicKeys(signer))
		}
	}

	return methods, agentConn
}

// ---------- Per-server processing ----------

func fmtPct(v float64) string {
	return strconv.FormatFloat(v, 'f', 2, 64)
}

func processServer(s ServerInput, sshConfig *ssh.ClientConfig) ServerResult {
	res := ServerResult{Idx: s.Idx, Server: s.Server, Team: s.Team}

	upper := strings.ToUpper(s.Server)
	if !strings.HasPrefix(upper, "TA-") && !strings.HasPrefix(upper, "AC-") {
		res.Error = "SKIP: prefix not TA- or AC-"
		return res
	}

	isoCmd := "cat /sys/devices/system/cpu/isolated 2>/dev/null || grep -o 'isolcpus=[^ ]*' /proc/cmdline | cut -d= -f2"
	lscpuCmd := "lscpu | grep -E 'Socket|NUMA'"
	combinedCmd := isoCmd + " && " + lscpuCmd

	client, err := ssh.Dial("tcp", s.Server+":22", sshConfig)
	if err != nil {
		res.Error = fmt.Sprintf("SSH connect ERROR: %v", err)
		log.Printf("[%d] %s: %s", s.Idx, s.Server, res.Error)
		return res
	}
	defer client.Close()

	cpuSocketOut, err := runCommand(client, combinedCmd)
	if err != nil {
		res.Error = fmt.Sprintf("SSH command ERROR: %v", err)
		log.Printf("[%d] %s: %s", s.Idx, s.Server, res.Error)
		return res
	}

	sockets, socketCPUSets, isoCPUs := parseLscpuOutput(strings.TrimSpace(cpuSocketOut))
	res.Sockets = sockets
	res.IsoCPUs = isoCPUs

	if socketCPUSets == nil {
		res.Error = fmt.Sprintf("ERROR Parsed lscpu output: no isolated CPUs, %d sockets", sockets)
		log.Printf("[%d] %s: %s", s.Idx, s.Server, res.Error)
		return res
	}

	cpuResults, err := getCPUIdleStatus(client, isoCPUs)
	if err != nil {
		res.Error = fmt.Sprintf("CPU check ERROR: %v", err)
		log.Printf("[%d] %s: %s", s.Idx, s.Server, res.Error)
		return res
	}

	socket0Set := make(map[int]bool)
	socket1Set := make(map[int]bool)
	for _, cpu := range socketCPUSets[0] {
		socket0Set[cpu] = true
	}
	for _, cpu := range socketCPUSets[1] {
		socket1Set[cpu] = true
	}

	var busy0, busy1, idle0, idle1 []string
	for _, r := range cpuResults {
		cs := strconv.Itoa(r.cpu)
		switch {
		case socket0Set[r.cpu]:
			if r.status == "Busy" {
				busy0 = append(busy0, cs)
			} else if r.status == "Idle" {
				idle0 = append(idle0, cs)
			}
		case socket1Set[r.cpu]:
			if r.status == "Busy" {
				busy1 = append(busy1, cs)
			} else if r.status == "Idle" {
				idle1 = append(idle1, cs)
			}
		}
	}

	total0 := len(busy0) + len(idle0)
	total1 := len(busy1) + len(idle1)

	switch {
	case sockets == 0:
		res.PctBusySocket0, res.PctFreeSocket0 = "n/a", "n/a"
		res.PctBusySocket1, res.PctFreeSocket1 = "n/a", "n/a"
		res.BusySocket0, res.IdleSocket0 = "n/a", "n/a"
		res.BusySocket1, res.IdleSocket1 = "n/a", "n/a"
	case sockets == 1:
		if total0 > 0 {
			b := float64(len(busy0)) / float64(total0) * 100
			res.PctBusySocket0, res.PctFreeSocket0 = fmtPct(b), fmtPct(100-b)
		} else {
			res.PctBusySocket0, res.PctFreeSocket0 = "0.00", "0.00"
		}
		res.PctBusySocket1, res.PctFreeSocket1 = "n/a", "n/a"
		res.BusySocket0 = strings.Join(busy0, ",")
		res.IdleSocket0 = strings.Join(idle0, ",")
		res.BusySocket1, res.IdleSocket1 = "n/a", "n/a"
	default:
		if total0 > 0 {
			b := float64(len(busy0)) / float64(total0) * 100
			res.PctBusySocket0, res.PctFreeSocket0 = fmtPct(b), fmtPct(100-b)
		} else {
			res.PctBusySocket0, res.PctFreeSocket0 = "0.00", "0.00"
		}
		if total1 > 0 {
			b := float64(len(busy1)) / float64(total1) * 100
			res.PctBusySocket1, res.PctFreeSocket1 = fmtPct(b), fmtPct(100-b)
		} else {
			res.PctBusySocket1, res.PctFreeSocket1 = "0.00", "0.00"
		}
		res.BusySocket0 = strings.Join(busy0, ",")
		res.IdleSocket0 = strings.Join(idle0, ",")
		res.BusySocket1 = strings.Join(busy1, ",")
		res.IdleSocket1 = strings.Join(idle1, ",")
	}

	log.Printf("[%d] %s (%s): sockets=%d iso=%s busy0=%s busy1=%s",
		s.Idx, s.Server, s.Team, sockets, isoCPUs,
		res.PctBusySocket0, res.PctBusySocket1)
	return res
}

// ---------- Main ----------

func main() {
	// log writes to stderr — does not pollute the JSON stdout output.
	log.SetFlags(log.Ltime)

	inputBytes, err := io.ReadAll(os.Stdin)
	if err != nil {
		log.Fatalf("reading stdin: %v", err)
	}

	var input Input
	if err := json.Unmarshal(inputBytes, &input); err != nil {
		log.Fatalf("parsing input JSON: %v", err)
	}

	authMethods, agentConn := buildAuthMethods()
	if agentConn != nil {
		defer agentConn.Close()
	}
	if len(authMethods) == 0 {
		log.Printf("WARNING: no SSH auth methods found — all connections will fail.")
		log.Printf("  Fix options:")
		log.Printf("  1. Set SSH_AUTH_SOCK (run: eval $(ssh-agent) && ssh-add)")
		log.Printf("  2. Set SSH_IDENTITY_FILE=/path/to/your/private/key")
		log.Printf("  3. Place an unencrypted key at ~/.ssh/id_rsa or ~/.ssh/id_ed25519")
	}

	sshConfig := &ssh.ClientConfig{
		User:            input.User,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // mirrors Paramiko AutoAddPolicy
		Timeout:         5 * time.Second,
	}

	// Limit concurrent SSH connections to avoid exhausting local file descriptors.
	const maxConcurrent = 50
	sem := make(chan struct{}, maxConcurrent)

	var mu sync.Mutex
	var wg sync.WaitGroup
	results := make([]ServerResult, 0, len(input.Servers))

	log.Printf("Starting SSH checks for %d servers (max %d concurrent)", len(input.Servers), maxConcurrent)

	for _, srv := range input.Servers {
		wg.Add(1)
		sem <- struct{}{}
		go func(s ServerInput) {
			defer wg.Done()
			defer func() { <-sem }()
			r := processServer(s, sshConfig)
			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		}(srv)
	}

	wg.Wait()
	log.Printf("All %d servers done", len(input.Servers))

	if err := json.NewEncoder(os.Stdout).Encode(Output{Results: results}); err != nil {
		log.Fatalf("encoding output JSON: %v", err)
	}
}
