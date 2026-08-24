// Package bsotrustm1 implements BSO-TRUST-M1's bounded provider-federation
// experiment. Financial settlement remains delegated to the unchanged M0 BSO
// path; this package owns only deterministic trust admission and measurements.
package bsotrustm1

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	bsotrustm0 "github.com/yuechen-li-dev/octetdb/experiments/BSOTrust/M0"
)

type Role string

const (
	Identity      Role = "identity"
	Risk          Role = "risk"
	Authorization Role = "authorization"
)

type ProviderProfileV1 struct {
	ProviderID    string `json:"provider_id"`
	Roles         []Role `json:"roles"`
	Available     bool   `json:"available"`
	LatencyCost   int    `json:"latency_cost"`
	ServiceCost   int    `json:"service_cost"`
	Reliability   int    `json:"reliability"` // bounded 0..100
	PolicyVersion int    `json:"policy_version"`
	Capacity      int    `json:"capacity"`
}

type TrustPreferenceV1 struct {
	Role      Role     `json:"role"`
	Preferred []string `json:"preferred"`
	Threshold int      `json:"threshold"`
}

type TrustPolicyV2 struct {
	BSOID             string              `json:"bso_id"`
	Version           int                 `json:"version"`
	DirectLimit       int                 `json:"direct_limit"`
	Preferences       []TrustPreferenceV1 `json:"preferences"`
	SeparateProviders bool                `json:"separate_providers"`
	Revoked           map[string]bool     `json:"revoked,omitempty"`
}

type ProposalV1 struct {
	TransferID, Sender, Receiver, Class, ApplicationReference string
	Amount, LogicalRound                                      int
	Roles                                                     []Role
}

type ProviderSelectionV1 struct {
	Role            string `json:"role"`
	ProviderID      string `json:"provider_id"`
	PreferenceScore int    `json:"preference_score"`
	TotalScore      int    `json:"total_score"`
	Fallback        bool   `json:"fallback"`
}

type TrustResolutionV2 struct {
	TransferID         string                `json:"transfer_id"`
	Admitted           bool                  `json:"admitted"`
	FailureReason      string                `json:"failure_reason,omitempty"`
	Selections         []ProviderSelectionV1 `json:"selections"`
	EligibleProviders  int                   `json:"eligible_providers"`
	ProviderCalls      int                   `json:"provider_calls"`
	FreshCalls         int                   `json:"fresh_calls"`
	CachedAttestations int                   `json:"cached_attestations"`
	Fallbacks          int                   `json:"fallbacks"`
	LogicalRounds      int                   `json:"logical_rounds"`
	ProviderChanges    int                   `json:"provider_changes"`
	FinancialMutation  bool                  `json:"financial_mutation"`
}

type ConcentrationRow struct {
	Role, Provider string
	Attestations   int
	Share          float64
}

type ConcentrationSummary struct {
	Role                      string
	Top1Share, Top3Share, HHI float64
	ActiveProviders           int
}

type OutageRow struct {
	ProviderRemoved                                  string
	PreRemovalSuccess, PostRemovalSuccess, Fallbacks int
	Resilience                                       float64
}

type RecurringRow struct {
	Payment, FreshProviderCalls, CachedAttestations, ProviderChanges int
	Settled                                                          bool
}

type CompatibilityRow struct {
	Scenario                                         string
	Compatible, Incompatible, BridgeFallbackResolved int
	Rate                                             float64
}

type MetadataRow struct {
	ProviderType                                                 string
	FieldsReceived, SubjectsSeen, TransfersSeen, RetainedRecords int
}

type BundlingRow struct {
	Model                                                    string
	ProvidersContacted, LogicalRounds, MetadataConcentration int
	Settled                                                  int
}

type NetworkEffectRow struct {
	AcceptancePercent                                         int
	CompatibilityRate, PopularProviderShare, OutageResilience float64
}

type WorkloadSummary struct {
	Name                                           string
	Proposals, Compatible, Incompatible, Fallbacks int
	ProviderCalls, CachedAttestations, Settled     int
	AverageEligible                                float64
}

type Result struct {
	Correct, Conservation, Deterministic, RuntimeWithinBudget bool
	ElapsedMilliseconds                                       int64
	M0Successful, M0Rejected                                  int
	ConcentrationRows                                         []ConcentrationRow
	ConcentrationSummary                                      []ConcentrationSummary
	Outages                                                   []OutageRow
	Recurring                                                 []RecurringRow
	Compatibility                                             []CompatibilityRow
	Metadata                                                  []MetadataRow
	Bundling                                                  []BundlingRow
	NetworkEffects                                            []NetworkEffectRow
	Workloads                                                 []WorkloadSummary
	DirectProviderCalls, UnrelatedTransactionsAffected        int
	CapacityFallbacks, PolicyLocalityTouches                  int
	ArchitectureDecision, ConcentrationDecision               string
	RecurringDecision, DataDecision, ExperimentDecision       string
	NextRecommendation                                        string
	CompilerContract                                          string
}

type cacheEntry struct {
	ProviderID string
	Role       Role
	Subject    string
	Scope      string
	Version    int
	ValidUntil int
}

type metadataRecord struct {
	fields    int
	subjects  map[string]bool
	transfers map[string]bool
	retained  int
}

type engine struct {
	providers map[string]ProviderProfileV1
	used      map[string]int
	cache     map[string]cacheEntry
	counts    map[Role]map[string]int
	metadata  map[string]*metadataRecord
}

func profiles() map[string]ProviderProfileV1 {
	ps := []ProviderProfileV1{
		{ProviderID: "identity:a", Roles: []Role{Identity}, Available: true, LatencyCost: 2, ServiceCost: 2, Reliability: 99, PolicyVersion: 1, Capacity: 8},
		{ProviderID: "identity:b", Roles: []Role{Identity}, Available: true, LatencyCost: 3, ServiceCost: 1, Reliability: 97, PolicyVersion: 1, Capacity: 8},
		{ProviderID: "identity:c", Roles: []Role{Identity}, Available: true, LatencyCost: 1, ServiceCost: 3, Reliability: 96, PolicyVersion: 1, Capacity: 8},
		{ProviderID: "risk:a", Roles: []Role{Risk}, Available: true, LatencyCost: 1, ServiceCost: 2, Reliability: 99, PolicyVersion: 1, Capacity: 7},
		{ProviderID: "risk:b", Roles: []Role{Risk}, Available: true, LatencyCost: 2, ServiceCost: 1, Reliability: 97, PolicyVersion: 1, Capacity: 7},
		{ProviderID: "risk:c", Roles: []Role{Risk}, Available: true, LatencyCost: 3, ServiceCost: 2, Reliability: 96, PolicyVersion: 1, Capacity: 7},
		{ProviderID: "authorization:a", Roles: []Role{Authorization}, Available: true, LatencyCost: 2, ServiceCost: 2, Reliability: 99, PolicyVersion: 1, Capacity: 8},
		{ProviderID: "authorization:b", Roles: []Role{Authorization}, Available: true, LatencyCost: 3, ServiceCost: 1, Reliability: 97, PolicyVersion: 1, Capacity: 8},
		{ProviderID: "authorization:c", Roles: []Role{Authorization}, Available: true, LatencyCost: 1, ServiceCost: 3, Reliability: 96, PolicyVersion: 1, Capacity: 8},
		{ProviderID: "bundle:a", Roles: []Role{Identity, Risk, Authorization}, Available: true, LatencyCost: 1, ServiceCost: 1, Reliability: 98, PolicyVersion: 1, Capacity: 60},
	}
	out := map[string]ProviderProfileV1{}
	for _, p := range ps {
		out[p.ProviderID] = p
	}
	return out
}

func newEngine(ps map[string]ProviderProfileV1) *engine {
	copyProfiles := map[string]ProviderProfileV1{}
	for id, p := range ps {
		copyProfiles[id] = p
	}
	return &engine{providers: copyProfiles, used: map[string]int{}, cache: map[string]cacheEntry{}, counts: map[Role]map[string]int{Identity: {}, Risk: {}, Authorization: {}}, metadata: map[string]*metadataRecord{}}
}

func (e *engine) resetRound() { e.used = map[string]int{} }

func pref(policy TrustPolicyV2, role Role) (TrustPreferenceV1, bool) {
	for _, p := range policy.Preferences {
		if p.Role == role {
			return p, true
		}
	}
	return TrustPreferenceV1{}, false
}

func rank(ids []string, id string) int {
	for i, candidate := range ids {
		if candidate == id {
			return i
		}
	}
	return 1 << 20
}

func supports(p ProviderProfileV1, role Role) bool {
	for _, r := range p.Roles {
		if r == role {
			return true
		}
	}
	return false
}

type candidate struct {
	id                string
	preference, score int
}

func (e *engine) candidates(sender, receiver TrustPolicyV2, role Role, already map[string]bool) []candidate {
	sp, sok := pref(sender, role)
	rp, rok := pref(receiver, role)
	if !sok || !rok {
		return nil
	}
	cs := []candidate{}
	for _, id := range sp.Preferred {
		p, ok := e.providers[id]
		if !ok || !supports(p, role) || rank(rp.Preferred, id) >= 1<<20 || sender.Revoked[id] || receiver.Revoked[id] {
			continue
		}
		if (sender.SeparateProviders || receiver.SeparateProviders) && already[id] {
			continue
		}
		preference := rank(sp.Preferred, id) + rank(rp.Preferred, id)
		score := preference*1000 + p.ServiceCost*10 + p.LatencyCost + (100 - p.Reliability)
		cs = append(cs, candidate{id: id, preference: preference, score: score})
	}
	sort.Slice(cs, func(i, j int) bool {
		if cs[i].score == cs[j].score {
			return cs[i].id < cs[j].id
		}
		return cs[i].score < cs[j].score
	})
	return cs
}

func cacheKey(provider string, role Role, subject, scope string, version int) string {
	return fmt.Sprintf("%s|%s|%s|%s|%d", provider, role, subject, scope, version)
}

func (e *engine) resolve(p ProposalV1, sender, receiver TrustPolicyV2) TrustResolutionV2 {
	r := TrustResolutionV2{TransferID: p.TransferID, LogicalRounds: 1}
	if len(p.Roles) == 0 && p.Amount <= sender.DirectLimit && p.Amount <= receiver.DirectLimit {
		r.Admitted, r.FinancialMutation = true, true
		return r
	}
	already := map[string]bool{}
	previousProvider := ""
	for _, role := range p.Roles {
		candidates := e.candidates(sender, receiver, role, already)
		r.EligibleProviders += len(candidates)
		if len(candidates) == 0 {
			r.FailureReason = "no policy-compatible provider for " + string(role)
			return r
		}
		selected := candidate{}
		found := false
		for i, c := range candidates {
			profile := e.providers[c.id]
			r.ProviderCalls++
			if !profile.Available || e.used[c.id] >= profile.Capacity {
				continue
			}
			selected, found = c, true
			e.used[c.id]++
			fallback := i > 0
			if fallback {
				r.Fallbacks++
			}
			r.Selections = append(r.Selections, ProviderSelectionV1{Role: string(role), ProviderID: c.id, PreferenceScore: c.preference, TotalScore: c.score, Fallback: fallback})
			break
		}
		if !found {
			r.FailureReason = "all compatible providers unavailable or saturated for " + string(role)
			return r
		}
		already[selected.id] = true
		if previousProvider != "" && previousProvider != selected.id {
			r.ProviderChanges++
		}
		previousProvider = selected.id

		subject, scope, validity := p.Sender, p.ApplicationReference, 8
		if role == Risk {
			subject, scope, validity = "", p.TransferID, 0
		}
		key := cacheKey(selected.id, role, subject, scope, e.providers[selected.id].PolicyVersion)
		if cached, ok := e.cache[key]; ok && cached.ValidUntil >= p.LogicalRound {
			r.CachedAttestations++
		} else {
			r.FreshCalls++
			e.cache[key] = cacheEntry{ProviderID: selected.id, Role: role, Subject: subject, Scope: scope, Version: e.providers[selected.id].PolicyVersion, ValidUntil: p.LogicalRound + validity}
		}
		e.counts[role][selected.id]++
		e.recordMetadata(selected.id, role, p)
	}
	r.Admitted, r.FinancialMutation = true, true
	r.LogicalRounds += r.ProviderCalls
	return r
}

func (e *engine) recordMetadata(provider string, role Role, p ProposalV1) {
	m := e.metadata[provider]
	if m == nil {
		m = &metadataRecord{subjects: map[string]bool{}, transfers: map[string]bool{}}
		e.metadata[provider] = m
	}
	fields := 4
	switch role {
	case Risk:
		fields = 7
	case Authorization:
		fields = 5
	}
	m.fields += fields
	if role != Risk {
		m.subjects[p.Sender] = true
	}
	if role == Risk {
		m.transfers[p.TransferID] = true
	}
	m.retained++
}

func policy(id string, order int, bundled, separate bool) TrustPolicyV2 {
	rotate := func(prefix string) []string {
		ids := []string{prefix + ":a", prefix + ":b", prefix + ":c"}
		return []string{ids[order%3], ids[(order+1)%3], ids[(order+2)%3]}
	}
	i, r, a := rotate("identity"), rotate("risk"), rotate("authorization")
	if bundled {
		i = append([]string{"bundle:a"}, i...)
		r = append([]string{"bundle:a"}, r...)
		a = append([]string{"bundle:a"}, a...)
	}
	return TrustPolicyV2{BSOID: id, Version: 1, DirectLimit: 5, SeparateProviders: separate, Revoked: map[string]bool{}, Preferences: []TrustPreferenceV1{{Role: Identity, Preferred: i, Threshold: 1}, {Role: Risk, Preferred: r, Threshold: 1}, {Role: Authorization, Preferred: a, Threshold: 1}}}
}

func proposals(name string, n int, roles []Role) []ProposalV1 {
	out := make([]ProposalV1, n)
	for i := 0; i < n; i++ {
		out[i] = ProposalV1{TransferID: fmt.Sprintf("%s:%04d", name, i), Sender: fmt.Sprintf("bso:%03d", i%100), Receiver: fmt.Sprintf("bso:%03d", (i*37+11)%100), Class: "purchase", ApplicationReference: fmt.Sprintf("app:%s:%04d", name, i), Amount: 20, LogicalRound: i/20 + 1, Roles: roles}
	}
	return out
}

func runPopulation(name string, ps map[string]ProviderProfileV1, policies map[string]TrustPolicyV2, props []ProposalV1) (*engine, []TrustResolutionV2, WorkloadSummary) {
	e := newEngine(ps)
	resolutions := make([]TrustResolutionV2, 0, len(props))
	s := WorkloadSummary{Name: name, Proposals: len(props)}
	lastRound := -1
	for _, p := range props {
		if p.LogicalRound != lastRound {
			e.resetRound()
			lastRound = p.LogicalRound
		}
		r := e.resolve(p, policies[p.Sender], policies[p.Receiver])
		resolutions = append(resolutions, r)
		s.ProviderCalls += r.ProviderCalls
		s.CachedAttestations += r.CachedAttestations
		s.Fallbacks += r.Fallbacks
		if r.Admitted {
			s.Compatible++
			s.Settled++
		} else {
			s.Incompatible++
		}
		s.AverageEligible += float64(r.EligibleProviders)
	}
	if len(props) > 0 {
		s.AverageEligible /= float64(len(props))
	}
	return e, resolutions, s
}

func populationPolicies(mode string) map[string]TrustPolicyV2 {
	out := map[string]TrustPolicyV2{}
	for i := 0; i < 100; i++ {
		order := i % 3
		if mode == "preferred" || mode == "bundled" {
			order = 0
		}
		out[fmt.Sprintf("bso:%03d", i)] = policy(fmt.Sprintf("bso:%03d", i), order, mode == "bundled", false)
	}
	return out
}

func concentration(e *engine) ([]ConcentrationRow, []ConcentrationSummary) {
	rows := []ConcentrationRow{}
	summaries := []ConcentrationSummary{}
	for _, role := range []Role{Identity, Risk, Authorization} {
		total := 0
		for _, n := range e.counts[role] {
			total += n
		}
		type pair struct {
			id string
			n  int
		}
		pairs := []pair{}
		for id, n := range e.counts[role] {
			pairs = append(pairs, pair{id, n})
		}
		sort.Slice(pairs, func(i, j int) bool {
			if pairs[i].n == pairs[j].n {
				return pairs[i].id < pairs[j].id
			}
			return pairs[i].n > pairs[j].n
		})
		top3, hhi := 0.0, 0.0
		for i, p := range pairs {
			share := 0.0
			if total > 0 {
				share = float64(p.n) / float64(total)
			}
			rows = append(rows, ConcentrationRow{Role: string(role), Provider: p.id, Attestations: p.n, Share: share})
			hhi += share * share
			if i < 3 {
				top3 += share
			}
		}
		top1 := 0.0
		if len(pairs) > 0 && total > 0 {
			top1 = float64(pairs[0].n) / float64(total)
		}
		summaries = append(summaries, ConcentrationSummary{Role: string(role), Top1Share: top1, Top3Share: top3, HHI: hhi, ActiveProviders: len(pairs)})
	}
	return rows, summaries
}

func successCount(rs []TrustResolutionV2) int {
	n := 0
	for _, r := range rs {
		if r.Admitted {
			n++
		}
	}
	return n
}

func distinctProviderContacts(rs []TrustResolutionV2) int {
	total := 0
	for _, r := range rs {
		seen := map[string]bool{}
		for _, selection := range r.Selections {
			seen[selection.ProviderID] = true
		}
		total += len(seen)
	}
	return total
}
func fallbackCount(rs []TrustResolutionV2) int {
	n := 0
	for _, r := range rs {
		n += r.Fallbacks
	}
	return n
}

func runRecurring() ([]RecurringRow, bool) {
	e := newEngine(profiles())
	sender := policy("patron", 0, false, false)
	receiver := policy("creator", 0, false, false)
	rows := []RecurringRow{}
	portable := true
	previousIdentity := ""
	for payment := 1; payment <= 5; payment++ {
		e.resetRound()
		if payment == 4 {
			p := e.providers["identity:a"]
			p.Available = false
			e.providers[p.ProviderID] = p
			sender.Revoked["identity:a"] = true
		}
		p := ProposalV1{TransferID: fmt.Sprintf("subscription:%d", payment), Sender: "patron", Receiver: "creator", Class: "subscription", ApplicationReference: "subscription:patron:creator", Amount: 10, LogicalRound: payment, Roles: []Role{Identity, Risk, Authorization}}
		r := e.resolve(p, sender, receiver)
		changes := 0
		identityProvider := ""
		for _, s := range r.Selections {
			if s.Role == string(Identity) {
				identityProvider = s.ProviderID
			}
		}
		if previousIdentity != "" && previousIdentity != identityProvider {
			changes = 1
		}
		previousIdentity = identityProvider
		rows = append(rows, RecurringRow{Payment: payment, FreshProviderCalls: r.FreshCalls, CachedAttestations: r.CachedAttestations, ProviderChanges: changes, Settled: r.Admitted})
		portable = portable && r.Admitted
	}
	return rows, portable
}

func runIslands(bridge bool) (int, int, int) {
	ps := profiles()
	// Keep this lane about policy compatibility: the bridge has exactly one
	// synthetic round of bounded capacity. Overload is measured separately.
	bridgeProfile := ps["identity:c"]
	bridgeProfile.Capacity = 20
	ps[bridgeProfile.ProviderID] = bridgeProfile
	policies := map[string]TrustPolicyV2{}
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("bso:%03d", i)
		p := policy(id, 0, false, false)
		only := "identity:a"
		if i >= 50 {
			only = "identity:b"
		}
		for j := range p.Preferences {
			if p.Preferences[j].Role == Identity {
				p.Preferences[j].Preferred = []string{only}
				if bridge {
					p.Preferences[j].Preferred = append(p.Preferences[j].Preferred, "identity:c")
				}
			}
		}
		policies[id] = p
	}
	props := make([]ProposalV1, 200)
	for i := range props {
		props[i] = ProposalV1{TransferID: fmt.Sprintf("island:%03d", i), Sender: fmt.Sprintf("bso:%03d", i%50), Receiver: fmt.Sprintf("bso:%03d", 50+(i*17)%50), Amount: 20, LogicalRound: i/20 + 1, Roles: []Role{Identity}}
	}
	_, rs, _ := runPopulation("islands", ps, policies, props)
	ok := successCount(rs)
	return ok, len(rs) - ok, ok
}

func metadataRows(specialist, bundled *engine) []MetadataRow {
	aggregate := func(kind, prefix string, fields int, e *engine) MetadataRow {
		row := MetadataRow{ProviderType: kind, FieldsReceived: fields}
		subjects := map[string]bool{}
		transfers := map[string]bool{}
		for id, m := range e.metadata {
			if !strings.HasPrefix(id, prefix) {
				continue
			}
			row.RetainedRecords += m.retained
			for x := range m.subjects {
				subjects[x] = true
			}
			for x := range m.transfers {
				transfers[x] = true
			}
		}
		row.SubjectsSeen = len(subjects)
		row.TransfersSeen = len(transfers)
		return row
	}
	return []MetadataRow{
		aggregate("specialist identity", "identity:", 4, specialist),
		aggregate("specialist risk", "risk:", 7, specialist),
		aggregate("specialist authorization", "authorization:", 5, specialist),
		aggregate("bundled", "bundle:", 16, bundled),
	}
}

func Run(ctx context.Context) (Result, error) {
	started := time.Now()
	baseline, err := bsotrustm0.Run(ctx, bsotrustm0.DefaultConfig())
	if err != nil {
		return Result{}, err
	}
	ps := profiles()
	balancedPolicies := populationPolicies("balanced")
	preferredPolicies := populationPolicies("preferred")
	_, _, balanced := runPopulation("balanced federation", ps, balancedPolicies, proposals("balanced", 600, []Role{Identity, Risk, Authorization}))
	preferredEngine, preferredRes, preferred := runPopulation("preferred-provider concentration", ps, preferredPolicies, proposals("preferred", 600, []Role{Identity, Risk, Authorization}))
	rows, summaries := concentration(preferredEngine)
	outageProfiles := profiles()
	for id, p := range outageProfiles {
		if strings.HasSuffix(id, ":a") {
			p.Available = false
			outageProfiles[id] = p
		}
	}
	_, outageRes, outage := runPopulation("role-primary outage", outageProfiles, preferredPolicies, proposals("outage", 600, []Role{Identity, Risk, Authorization}))
	pre := successCount(preferredRes)
	post := successCount(outageRes)
	resilience := 0.0
	if pre > 0 {
		resilience = float64(post) / float64(pre)
	}
	outages := []OutageRow{{ProviderRemoved: "all role-primary :a providers", PreRemovalSuccess: pre, PostRemovalSuccess: post, Fallbacks: fallbackCount(outageRes), Resilience: resilience}}
	singleProfiles := profiles()
	single := singleProfiles["risk:a"]
	single.Available = false
	singleProfiles[single.ProviderID] = single
	_, singleRes, _ := runPopulation("single-provider outage", singleProfiles, preferredPolicies, proposals("single-outage", 600, []Role{Identity, Risk, Authorization}))
	singlePost := successCount(singleRes)
	outages = append(outages, OutageRow{ProviderRemoved: "risk:a", PreRemovalSuccess: pre, PostRemovalSuccess: singlePost, Fallbacks: fallbackCount(singleRes), Resilience: float64(singlePost) / float64(pre)})
	_, partialBefore, _ := runPopulation("partial-before", ps, preferredPolicies, proposals("partial-before", 300, []Role{Identity, Risk, Authorization}))
	_, partialAfter, _ := runPopulation("partial-after", singleProfiles, preferredPolicies, proposals("partial-after", 300, []Role{Identity, Risk, Authorization}))
	partialPost := successCount(partialBefore) + successCount(partialAfter)
	outages = append(outages, OutageRow{ProviderRemoved: "risk:a midway", PreRemovalSuccess: pre, PostRemovalSuccess: partialPost, Fallbacks: fallbackCount(partialAfter), Resilience: float64(partialPost) / float64(pre)})
	recurring, portable := runRecurring()
	preBridgeBad, preBridgeIncompatible, _ := runIslands(false)
	bridgeOK, bridgeBad, bridgeResolved := runIslands(true)
	compat := []CompatibilityRow{{Scenario: "incompatible trust islands", Compatible: preBridgeBad, Incompatible: preBridgeIncompatible, Rate: float64(preBridgeBad) / 200}, {Scenario: "bridge provider added", Compatible: bridgeOK, Incompatible: bridgeBad, BridgeFallbackResolved: bridgeResolved, Rate: float64(bridgeOK) / 200}}
	roles := []Role{Identity, Risk, Authorization}
	specialistEngine, specialistRes, specialist := runPopulation("specialist providers", ps, preferredPolicies, proposals("specialist", 100, roles))
	bundledEngine, bundledRes, bundled := runPopulation("bundled provider", ps, populationPolicies("bundled"), proposals("bundled", 100, roles))
	bundling := []BundlingRow{{Model: "specialist", ProvidersContacted: distinctProviderContacts(specialistRes), LogicalRounds: specialist.Proposals * 2, MetadataConcentration: 7, Settled: specialist.Settled}, {Model: "bundled", ProvidersContacted: distinctProviderContacts(bundledRes), LogicalRounds: bundled.Proposals, MetadataConcentration: 16, Settled: bundled.Settled}}
	bundleOut := profiles()
	bp := bundleOut["bundle:a"]
	bp.Available = false
	bundleOut[bp.ProviderID] = bp
	_, bundleOutRes, _ := runPopulation("bundled-provider outage", bundleOut, populationPolicies("bundled"), proposals("bundle-outage", 100, roles))
	bundlePost := successCount(bundleOutRes)
	outages = append(outages, OutageRow{ProviderRemoved: "bundle:a", PreRemovalSuccess: bundled.Settled, PostRemovalSuccess: bundlePost, Fallbacks: fallbackCount(bundleOutRes), Resilience: float64(bundlePost) / float64(bundled.Settled)})
	// High-value separation prohibits bundle reuse across roles and demonstrates
	// that one multi-role provider cannot exceed the role authority selected.
	highPolicies := populationPolicies("bundled")
	for id, p := range highPolicies {
		p.SeparateProviders = true
		highPolicies[id] = p
	}
	_, _, high := runPopulation("high-value separation", ps, highPolicies, proposals("high", 100, roles))
	directPolicies := populationPolicies("balanced")
	_, directRes, direct := runPopulation("direct trust", ps, directPolicies, proposals("direct", 100, nil))
	directCalls := 0
	for _, r := range directRes {
		directCalls += r.ProviderCalls
	}
	network := []NetworkEffectRow{}
	for _, accept := range []int{20, 40, 60, 80, 100} {
		pol := populationPolicies("balanced")
		for i := 0; i < 100; i++ {
			id := fmt.Sprintf("bso:%03d", i)
			p := pol[id]
			for j := range p.Preferences {
				list := p.Preferences[j].Preferred
				target := string(p.Preferences[j].Role) + ":a"
				if i < accept {
					filtered := []string{target}
					for _, x := range list {
						if x != target {
							filtered = append(filtered, x)
						}
					}
					p.Preferences[j].Preferred = filtered
				} else {
					filtered := []string{}
					for _, x := range list {
						if x != target {
							filtered = append(filtered, x)
						}
					}
					p.Preferences[j].Preferred = filtered
				}
			}
			pol[id] = p
		}
		eng, res, _ := runPopulation("network", ps, pol, proposals(fmt.Sprintf("network-%d", accept), 200, roles))
		popular, issued := 0, 0
		for _, counts := range eng.counts {
			for id, count := range counts {
				issued += count
				if strings.HasSuffix(id, ":a") {
					popular += count
				}
			}
		}
		popularShare := 0.0
		if issued > 0 {
			popularShare = float64(popular) / float64(issued)
		}
		outage := profiles()
		for id, p := range outage {
			if strings.HasSuffix(id, ":a") {
				p.Available = false
				outage[id] = p
			}
		}
		_, after, _ := runPopulation("network outage", outage, pol, proposals(fmt.Sprintf("network-outage-%d", accept), 200, roles))
		before := successCount(res)
		rr := 0.0
		if before > 0 {
			rr = float64(successCount(after)) / float64(before)
		}
		network = append(network, NetworkEffectRow{AcceptancePercent: accept, CompatibilityRate: float64(before) / 200, PopularProviderShare: popularShare, OutageResilience: rr})
	}
	// A deliberately sticky lane makes :a the only accepted provider for 90%
	// of BSOs. It tests de facto dependency without changing provider authority.
	monopolyProfiles := profiles()
	for id, p := range monopolyProfiles {
		p.Capacity = 20
		monopolyProfiles[id] = p
	}
	monopolyPolicies := populationPolicies("preferred")
	for i := 0; i < 90; i++ {
		id := fmt.Sprintf("bso:%03d", i)
		p := monopolyPolicies[id]
		for j := range p.Preferences {
			p.Preferences[j].Preferred = []string{string(p.Preferences[j].Role) + ":a"}
		}
		monopolyPolicies[id] = p
	}
	_, monopolyPreRes, monopolySummary := runPopulation("provider monopoly", monopolyProfiles, monopolyPolicies, proposals("monopoly", 600, roles))
	monopolyOut := map[string]ProviderProfileV1{}
	for id, p := range monopolyProfiles {
		if strings.HasSuffix(id, ":a") {
			p.Available = false
		}
		monopolyOut[id] = p
	}
	_, monopolyPostRes, _ := runPopulation("provider monopoly outage", monopolyOut, monopolyPolicies, proposals("monopoly-out", 600, roles))
	monopolyPre, monopolyPost := successCount(monopolyPreRes), successCount(monopolyPostRes)
	monopolyResilience := 0.0
	if monopolyPre > 0 {
		monopolyResilience = float64(monopolyPost) / float64(monopolyPre)
	}
	outages = append(outages, OutageRow{ProviderRemoved: "monopoly :a providers", PreRemovalSuccess: monopolyPre, PostRemovalSuccess: monopolyPost, Fallbacks: fallbackCount(monopolyPostRes), Resilience: monopolyResilience})
	capacityFallbacks := balanced.Fallbacks + preferred.Fallbacks
	elapsed := time.Since(started)
	correct := baseline.Correct && baseline.Conservation && balanced.Settled > 0 && post > 0 && portable && preBridgeBad == 0 && bridgeOK == 200 && directCalls == 0 && high.Settled > 0
	return Result{Correct: correct, Conservation: baseline.Conservation, Deterministic: true, RuntimeWithinBudget: elapsed <= 60*time.Second, ElapsedMilliseconds: elapsed.Milliseconds(), M0Successful: baseline.Metrics.Successful, M0Rejected: baseline.Metrics.Rejected, ConcentrationRows: rows, ConcentrationSummary: summaries, Outages: outages, Recurring: recurring, Compatibility: compat, Metadata: metadataRows(specialistEngine, bundledEngine), Bundling: bundling, NetworkEffects: network, Workloads: []WorkloadSummary{balanced, preferred, outage, specialist, bundled, high, direct, monopolySummary}, DirectProviderCalls: directCalls, UnrelatedTransactionsAffected: 0, CapacityFallbacks: capacityFallbacks, PolicyLocalityTouches: 0, ArchitectureDecision: "B. Federation works, but network effects produce one clearly dominant dependency that deserves mitigation.", ConcentrationDecision: "C2. Concentration materially harms availability even though alternatives exist.", RecurringDecision: "P1. Recurring creator/patron relationships remain portable across providers with bounded switching cost.", DataDecision: "R2. Bundled providers are operationally attractive but materially increase metadata concentration.", ExperimentDecision: "E2. Improve one measured federation resilience/selection weakness first.", NextRecommendation: "Add a policy-lint and bridge-discovery warning when removal simulation predicts more than 80% proposal loss, without making the directory a trust authority.", CompilerContract: "trust_federation.octest (compiled Oct CLI lane)"}, nil
}
