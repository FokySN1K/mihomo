package statistic

import (
	"os"
	"sort"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/xsync"
	"github.com/metacubex/mihomo/component/memory"
)

var DefaultManager *Manager

func init() {
	DefaultManager = &Manager{
		uploadTemp:    atomic.NewInt64(0),
		downloadTemp:  atomic.NewInt64(0),
		uploadBlip:    atomic.NewInt64(0),
		downloadBlip:  atomic.NewInt64(0),
		uploadTotal:   atomic.NewInt64(0),
		downloadTotal: atomic.NewInt64(0),
		nodes:         make(map[string]*nodeCounter),
		nodeProviders: make(map[string]string),
		pid:           int32(os.Getpid()),
	}

	go DefaultManager.handle()
}

type Manager struct {
	connections        xsync.Map[string, Tracker]
	uploadTemp         atomic.Int64
	downloadTemp       atomic.Int64
	uploadBlip         atomic.Int64
	downloadBlip       atomic.Int64
	uploadTotal        atomic.Int64
	downloadTotal      atomic.Int64
	proxyUploadTemp    atomic.Int64
	proxyDownloadTemp  atomic.Int64
	proxyUploadBlip    atomic.Int64
	proxyDownloadBlip  atomic.Int64
	proxyUploadTotal   atomic.Int64
	proxyDownloadTotal atomic.Int64
	pid                int32
	memory             uint64

	// nodeMu guards the per-node counters and the provider names. The
	// counters themselves are atomic: a busy dial phase closes many
	// connections at once and every close adds its bytes.
	nodeMu        sync.Mutex
	nodes         map[string]*nodeCounter
	nodeProviders map[string]string
}

func (m *Manager) Join(c Tracker) {
	if DefaultRequestNotify != nil {
		DefaultRequestNotify(c)
	}
	m.connections.Store(c.ID(), c)
}

func (m *Manager) Leave(c Tracker) {
	m.connections.Delete(c.ID())
	m.attributeNode(c.Info())
}

// NodeUsage is the cumulative traffic one outbound node carried since the
// process started, together with the provider the node came from. The SNKP
// telemetry channel asks for this snapshot and turns the totals into deltas,
// so the counters never reset on their own.
type NodeUsage struct {
	Name     string `json:"name"`
	Provider string `json:"provider"`
	Up       int64  `json:"up"`
	Down     int64  `json:"down"`
}

type nodeCounter struct {
	up   atomic.Int64
	down atomic.Int64
}

// attributeNode adds a finished connection's bytes to the outbound node that
// carried it. The chain holds the outbound first (Chain.Last()), and the
// provider chain, when the config uses proxy providers, names the airport.
// Direct traffic and nameless outbounds are not attributed: the share quota
// already counts them through the plain traffic totals.
func (m *Manager) attributeNode(info *TrackerInfo) {
	if info == nil {
		return
	}
	name := info.Chain.Last()
	if name == "" || IsDirect(name) {
		return
	}
	up, down := info.UploadTotal.Load(), info.DownloadTotal.Load()
	if up == 0 && down == 0 {
		return
	}

	m.nodeMu.Lock()
	if m.nodes == nil {
		m.nodes = make(map[string]*nodeCounter)
		m.nodeProviders = make(map[string]string)
	}
	counter := m.nodes[name]
	if counter == nil {
		counter = &nodeCounter{}
		m.nodes[name] = counter
	}
	if provider := info.ProviderChain.Last(); provider != "" {
		m.nodeProviders[name] = provider
	}
	m.nodeMu.Unlock()

	counter.up.Add(up)
	counter.down.Add(down)
}

// NodeSnapshot returns the per-node traffic accumulated so far, the busiest
// node first.
func (m *Manager) NodeSnapshot() []NodeUsage {
	m.nodeMu.Lock()
	usages := make([]NodeUsage, 0, len(m.nodes))
	for name, counter := range m.nodes {
		usages = append(usages, NodeUsage{
			Name:     name,
			Provider: m.nodeProviders[name],
			Up:       counter.up.Load(),
			Down:     counter.down.Load(),
		})
	}
	m.nodeMu.Unlock()

	sort.Slice(usages, func(i, j int) bool {
		left, right := usages[i].Up+usages[i].Down, usages[j].Up+usages[j].Down
		if left != right {
			return left > right
		}
		return usages[i].Name < usages[j].Name
	})
	return usages
}

func (m *Manager) Get(id string) (c Tracker) {
	if value, ok := m.connections.Load(id); ok {
		c = value
	}
	return
}

func (m *Manager) Range(f func(c Tracker) bool) {
	m.connections.Range(func(key string, value Tracker) bool {
		return f(value)
	})
}

func (m *Manager) PushUploaded(direct bool, size int64) {
	if !direct {
		m.proxyUploadTemp.Add(size)
		m.proxyUploadTotal.Add(size)
	}
	m.uploadTemp.Add(size)
	m.uploadTotal.Add(size)
}

func (m *Manager) PushDownloaded(direct bool, size int64) {
	if !direct {
		m.proxyDownloadTemp.Add(size)
		m.proxyDownloadTotal.Add(size)
	}
	m.downloadTemp.Add(size)
	m.downloadTotal.Add(size)
}

func (m *Manager) Now() (up int64, down int64) {
	return m.uploadBlip.Load(), m.downloadBlip.Load()
}

func (m *Manager) Total() (up, down int64) {
	return m.uploadTotal.Load(), m.downloadTotal.Load()
}

func (m *Manager) Memory() uint64 {
	m.updateMemory()
	return m.memory
}

func (m *Manager) Snapshot() *Snapshot {
	var connections []*TrackerInfo
	m.Range(func(c Tracker) bool {
		connections = append(connections, c.Info())
		return true
	})
	return &Snapshot{
		UploadTotal:   m.uploadTotal.Load(),
		DownloadTotal: m.downloadTotal.Load(),
		Connections:   connections,
		Memory:        m.memory,
	}
}

func (m *Manager) updateMemory() {
	stat, err := memory.GetMemoryInfo(m.pid)
	if err != nil {
		return
	}
	m.memory = stat.RSS
}

func (m *Manager) ResetStatistic() {
	m.uploadTemp.Store(0)
	m.uploadBlip.Store(0)
	m.uploadTotal.Store(0)
	m.downloadTemp.Store(0)
	m.downloadBlip.Store(0)
	m.downloadTotal.Store(0)

	m.proxyUploadTemp.Store(0)
	m.proxyUploadBlip.Store(0)
	m.proxyUploadTotal.Store(0)
	m.proxyDownloadTemp.Store(0)
	m.proxyDownloadBlip.Store(0)
	m.proxyDownloadTotal.Store(0)

}

func (m *Manager) handle() {
	ticker := time.NewTicker(time.Second)

	for range ticker.C {
		m.uploadBlip.Store(m.uploadTemp.Swap(0))
		m.downloadBlip.Store(m.downloadTemp.Swap(0))
		m.proxyUploadBlip.Store(m.proxyUploadTemp.Swap(0))
		m.proxyDownloadBlip.Store(m.proxyDownloadTemp.Swap(0))
	}
}

type Snapshot struct {
	DownloadTotal int64          `json:"downloadTotal"`
	UploadTotal   int64          `json:"uploadTotal"`
	Connections   []*TrackerInfo `json:"connections"`
	Memory        uint64         `json:"memory"`
}
