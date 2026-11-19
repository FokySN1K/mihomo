package statistic

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

func newTestManager() *Manager {
	return &Manager{
		nodes:         make(map[string]*nodeCounter),
		nodeProviders: make(map[string]string),
	}
}

func TestNodeSnapshotAttributesClosedConnections(t *testing.T) {
	manager := newTestManager()

	first := &TrackerInfo{
		Chain:         C.Chain{"node-b", "GROUP"},
		ProviderChain: C.Chain{"airport-1", ""},
	}
	first.UploadTotal.Store(100)
	first.DownloadTotal.Store(900)
	manager.attributeNode(first)

	second := &TrackerInfo{Chain: C.Chain{"node-a", "GROUP"}}
	second.UploadTotal.Store(10)
	second.DownloadTotal.Store(20)
	manager.attributeNode(second)

	usages := manager.NodeSnapshot()
	if len(usages) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(usages))
	}
	if usages[0].Name != "node-b" || usages[0].Provider != "airport-1" {
		t.Fatalf("expected the busiest node first, got %+v", usages[0])
	}
	if usages[0].Up != 100 || usages[0].Down != 900 {
		t.Fatalf("unexpected traffic on the first node: %+v", usages[0])
	}
	if usages[1].Name != "node-a" {
		t.Fatalf("unexpected second node: %+v", usages[1])
	}
}

func TestNodeSnapshotSkipsDirectAndEmpty(t *testing.T) {
	manager := newTestManager()

	direct := &TrackerInfo{Chain: C.Chain{"DIRECT"}}
	direct.UploadTotal.Store(1)
	manager.attributeNode(direct)

	empty := &TrackerInfo{Chain: C.Chain{}}
	empty.DownloadTotal.Store(1)
	manager.attributeNode(empty)

	manager.attributeNode(&TrackerInfo{Chain: C.Chain{"node-idle", "GROUP"}})

	if usages := manager.NodeSnapshot(); len(usages) != 0 {
		t.Fatalf("direct, nameless and idle connections must not reach the snapshot, got %+v", usages)
	}
}
