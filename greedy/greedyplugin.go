// Package greedy contains an out-of-tree plugin based on the Kubernetes
// scheduling framework.
package greedy

import (
	"context"
	"fmt"

	"github.com/dimitrisdol/greedyScheduler/greedy/hardcoded"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	// Name is the "official" external name of this scheduling plugin.
	Name = "GreedyPlugin"

	// sla is the maximum slowdown that is allowed for an application when
	// it is being scheduled along another one.
	sla = 1.5

	// greedyLabelKey is the key of the Kubernetes Label which every
	// application that needs to be tracked by GreedyPlugin should have.
	greedyLabelKey = "greedy"
)

// GreedyPlugin is an out-of-tree plugin for the kube-scheduler, which takes into
// account information about the slowdown of colocated applications when they
// are wrapped into Pods and scheduled on the Kubernetes cluster.
type GreedyPlugin struct {
	handle framework.Handle
	model  InterferenceModel
}

var (
	_ framework.Plugin          = &GreedyPlugin{}
	_ framework.FilterPlugin    = &GreedyPlugin{}
	_ framework.ScorePlugin     = &GreedyPlugin{}
	_ framework.ScoreExtensions = &GreedyPlugin{}
)

// New instantiates a GreedyPlugin.
func New(configuration runtime.Object, f framework.Handle) (framework.Plugin, error) {
	return &GreedyPlugin{
		handle: f,
		model:  hardcoded.New(greedyLabelKey),
	}, nil
}

// Name returns the official name of the GreedyPlugin.
func (_ *GreedyPlugin) Name() string {
	return Name
}

// findCurrentOccupants returns all Pods that are being tracked by GreedyPlugin
// and are already scheduled on the Node represented by the given NodeInfo.
//
// NOTE: For now, the number of the returned Pods should *always* be at most 2;
// otherwise, there must be some error in our scheduling logic.
func (_ *GreedyPlugin) findCurrentOccupants(nodeInfo *framework.NodeInfo) []*corev1.Pod {
	ret := make([]*corev1.Pod, 0, 2)
	for _, podInfo := range nodeInfo.Pods {
		for key := range podInfo.Pod.Labels {
			if greedyLabelKey == key {
				ret = append(ret, podInfo.Pod)
			}
		}
	}
	return ret
}

// Filter is called by the scheduling framework.
//
// All FilterPlugins should return "Success" to declare that
// the given node fits the pod. If Filter doesn't return "Success",
// it will return "Unschedulable", "UnschedulableAndUnresolvable" or "Error".
//
// For the node being evaluated, Filter plugins should look at the passed
// nodeInfo reference for this particular node's information (e.g., pods
// considered to be running on the node) instead of looking it up in the
// NodeInfoSnapshot because we don't guarantee that they will be the same.
//
// For example, during preemption, we may pass a copy of the original
// nodeInfo object that has some pods removed from it to evaluate the
// possibility of preempting them to schedule the target pod.
func (ap *GreedyPlugin) Filter(
	ctx context.Context,
	state *framework.CycleState,
	pod *corev1.Pod,
	nodeInfo *framework.NodeInfo,
) *framework.Status {
	if pod == nil {
		return framework.NewStatus(framework.Error, "pod cannot be nil")
	}
	if nodeInfo.Node() == nil {
		return framework.NewStatus(framework.Error, "node cannot be nil")
	}
	nodeName := nodeInfo.Node().Name

	// If the given Pod does not have the greedyLabelKey, approve it and let
	// the other plugins decide for its fate.
	if _, exists := pod.Labels[greedyLabelKey]; !exists {
		klog.V(2).Infof("blindly approving Pod '%s/%s' as it does not have GreedyPlugin's label %q", pod.Namespace, pod.Name, greedyLabelKey)
		return framework.NewStatus(framework.Success, "pod is not tracked by GreedyPlugin")
	}

	// For the Node at hand, find all occupant Pods tracked by GreedyPlugin.
	// These should *always* be fewer than or equal to 2, but we take the
	// opportunity to assert this invariant later anyway.
	occupants := ap.findCurrentOccupants(nodeInfo)

	// Decide on how to proceed based on the number of current occupants
	switch len(occupants) {
	// If the Node is full (i.e., 2 applications tracked by GreedyPlugin are
	// already scheduled on it), filter it out.
	case 2:
		klog.V(2).Infof("filtering Node %q out because 2 GreedyPlugin applications are already scheduled there", nodeName)
		return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Node '%s' already has 2 GreedyPlugin occupants", nodeName))
	// If the existing occupant is slowed down prohibitively much by the
	// new Pod's attack, filter the Node out.
	case 1:
		occ := occupants[0] // the single, currently scheduled Pod
		score, err := ap.model.Attack(pod, occ)
		if err != nil {
			err = fmt.Errorf("new Pod '%s/%s' on Node '%s': %v", occ.Namespace, occ.Name, nodeName, err)
			klog.Warning(err)
			return framework.NewStatus(framework.Error, err.Error())
		}
		if score > sla {
			msg := fmt.Sprintf("filtering Node '%s': new pod '%s/%s' ('%s') incurs huge slowdown on pod '%s/%s' ('%s')",
				nodeName, pod.Namespace, pod.Name, pod.Labels[greedyLabelKey], occ.Namespace, occ.Name, occ.Labels[greedyLabelKey])
			klog.V(2).Infof(msg)
			return framework.NewStatus(framework.Unschedulable, msg)
		}
		fallthrough
	// If the Node is empty, or fell through from above (i.e., SLA allows
	// the single current occupant to be attacked by the new Pod), approve.
	case 0:
		klog.V(2).Infof("approving Node %q for pod '%s/%s'", nodeName, pod.Namespace, pod.Name)
		return framework.NewStatus(framework.Success)
	// If more than 2 occupants are found to be already scheduled on the
	// Node at hand, we must have fucked up earlier; report the error.
	default:
		klog.Errorf("detected %d occupant Pods tracked by ActiPlugin on Node %q", len(occupants), nodeName)
		return framework.NewStatus(framework.Error, fmt.Sprintf("found %d occupants on '%s' already", len(occupants), nodeName))
	}
}

// Score is called on each filtered node. It must return success and an integer
// indicating the rank of the node. All scoring plugins must return success or
// the pod will be rejected.
//
// In the case of GreedyPlugin, scoring is reversed; i.e., higher score indicates
// worse scheduling decision.
// This is taken into account and "fixed" later, during the normalization.
func (ap *GreedyPlugin) Score(
	ctx context.Context,
	state *framework.CycleState,
	p *corev1.Pod,
	nodeName string,
) (int64, *framework.Status) {
	// Retrieve the Node at hand from the cycle's snapshot
	nodeInfo, err := ap.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return -1, framework.NewStatus(framework.Error, fmt.Sprintf("failed to get Node '%s' from snapshot: %v", nodeName, err))
	}

	occupants := ap.findCurrentOccupants(nodeInfo)

	// If the Node is empty, for now, assume it is a perfect candidate.
	// Therefore, the scheduled applications are expected to tend to spread
	// among the available Nodes as much as possible.
	if len(occupants) == 0 {
		return 0, framework.NewStatus(framework.Success, fmt.Sprintf("Node '%s' is empty: interim score = 0", nodeName))
	}

	// Otherwise, evaluate the slowdown
	occ := occupants[0]
	scoreFp, err := ap.model.Attack(p, occ)
	if err != nil {
		err = fmt.Errorf("new Pod '%s/%s' on Node '%s': %v", occ.Namespace, occ.Name, nodeName, err)
		klog.Warning(err)
		return -1, framework.NewStatus(framework.Error, err.Error())
	}
	score := int64(ap.model.ToInt64Multiplier() * scoreFp)
	return score, framework.NewStatus(framework.Success, fmt.Sprintf("Node '%s': interim score = %d", nodeName, score))
}

// ScoreExtensions returns the GreedyPlugin itself, since it implements the
// framework.ScoreExtensions interface.
func (ap *GreedyPlugin) ScoreExtensions() framework.ScoreExtensions {
	return ap
}

// NormalizeScore is called for all node scores produced by the same plugin's
// "Score" method. A successful run of NormalizeScore will update the scores
// list and return a success status.
//
// In the case of the GreedyPlugin, its "Score" method produces scores of reverse
// priority (i.e., the lower the score, the better the result). Therefore all
// scores have to be reversed during the normalization, so that higher score
// indicates a better scheduling result in terms of slowdowns.
func (_ *GreedyPlugin) NormalizeScore(
	ctx context.Context,
	state *framework.CycleState,
	p *corev1.Pod,
	scores framework.NodeScoreList,
) *framework.Status {
	// Find the max score for the normalization
	var maxScore int64
	for i := range scores {
		if scores[i].Score > maxScore {
			maxScore = scores[i].Score
		}
	}
	// When no Pod (tracked by GreedyPlugin) is scheduled on the Node,
	// maxScore will be 0.
	if maxScore == 0 {
		for i := range scores {
			scores[i].Score = framework.MaxNodeScore // reverse priority
		}
		return framework.NewStatus(framework.Success)
	}

	// Normalize them & reverse their priority
	for i := range scores {
		score := scores[i].Score                          // load
		score = framework.MaxNodeScore * score / maxScore // normalize
		score = framework.MaxNodeScore - score            // reverse priority
		scores[i].Score = score                           // store
	}
	return framework.NewStatus(framework.Success)
}
