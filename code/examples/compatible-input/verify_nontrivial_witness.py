"""Check an abstract nontrivial Q witness; never execute a protocol.

The generated hashes model content-addressed parent references, not signatures.
Default operation is read-only. --write-new exclusively creates the JSON artifact.
"""
import argparse
import hashlib
import json
from pathlib import Path

CREATORS = ("U", "V", "W", "Z")
CORRECT = ("U", "V", "W")
A, B = "V3", "Z2"
W, Q, THETA, F = 4, 3, 2, 1


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def vertex_round(vertex):
    return int(vertex[1:])


def key(vertex):
    return vertex_round(vertex), CREATORS.index(vertex[0])


def make_parents(variant):
    p = {c + "0": [] for c in CREATORS}
    for c in CREATORS:
        p[c + "1"] = ["U0", "V0", "W0"]
        p[c + "2"] = ["U1", "V1", "W1"]
    for c in CORRECT:
        p[c + "3"] = ["U2", "V2", "W2"]
        p[c + "4"] = ["U3", "V3", "W3"]
        p[c + "5"] = ["V4", "W4", "Z4"]
        p[c + "6"] = ["U5", "V5", "W5"]
        p[c + "7"] = ["U6", "V6", "W6"]
        p[c + "8"] = ["V7", "W7", "Z7"]
        p[c + "9"] = ["U8", "V8", "W8"]
        p[c + "10"] = ["U9", "V9", "W9"]
    p["Z3"] = ["Z2", "U2", "W2"]
    p["Z4"] = (["Z3", "U3", "W3"] if variant == "minus"
               else ["Z3", "V3", "U3"])
    p["Z5"] = ["U4", "V4", "Z4"]
    p["Z6"] = ["U5", "V5", "Z5"]
    p["Z7"] = ["U6", "V6", "Z6"]
    return p


def build(variant):
    p = make_parents(variant)
    assert len({(x[0], vertex_round(x)) for x in p}) == len(p)
    ancestry, digests, headers = {}, {}, {}
    for x in sorted(p, key=key):
        parents = p[x]
        r = vertex_round(x)
        assert all(y in p and vertex_round(y) == r - 1 for y in parents)
        if r:
            assert len({y[0] for y in parents}) >= Q
        else:
            assert not parents
        ancestry[x] = {x}
        for y in parents:
            ancestry[x].update(ancestry[y])
        header = {"creator": x[0], "round": r,
                  "payload": "target-A" if x == A else "target-B" if x == B else "neutral",
                  "parent_digests": sorted(digests[y] for y in parents)}
        digest = hashlib.sha256(canonical(header).encode()).hexdigest()
        assert digest not in digests.values()
        digests[x], headers[x] = digest, header
    for x in p:
        assert headers[x]["parent_digests"] == sorted(digests[y] for y in p[x])
        assert hashlib.sha256(canonical(headers[x]).encode()).hexdigest() == digests[x]

    genesis = {c + "0" for c in CREATORS}
    prefix, events = set(genesis), []
    for index, leader in enumerate(("W2", "W6", "U8", "W10"), 1):
        old = set(prefix)
        prefix.update(ancestry[leader])
        assert old <= prefix
        assert all(set(p[x]) <= prefix for x in prefix)
        delivered = prefix - old
        assert delivered
        events.append({"index": index, "leader": leader,
                       "rho": max(map(vertex_round, prefix)),
                       "prefix": sorted(prefix, key=key),
                       "slice": sorted(delivered, key=key)})
    execution = set().union(*(set(e["slice"]) for e in events))
    assert sum(len(e["slice"]) for e in events) == len(execution)
    delivered = set()
    for e in events:
        delivered.update(e["slice"])
        assert all((ancestry[x] & execution) <= delivered for x in delivered)

    target_slice = events[1]["slice"]
    assert A in target_slice and B in target_slice
    assert A not in ancestry[B] and B not in ancestry[A]
    horizon = max(map(vertex_round, target_slice)) + W
    selected_event = next(e for e in events if e["index"] >= 2 and e["rho"] >= horizon)
    assert horizon == 10 and selected_event["index"] == 4
    selected = set(selected_event["prefix"])
    assert "U4" not in events[1]["prefix"] and "U4" in selected

    def observation(x, r, creators=CREATORS):
        return sum(c + str(r) in selected and x in ancestry[c + str(r)] for c in creators)

    def eligibility(x, creators=CREATORS):
        return [r for r in range(vertex_round(x), vertex_round(x) + W + 1)
                if observation(x, r, creators) >= Q]

    table = []
    for r in range(2, 8):
        bits = {}
        for c in CREATORS:
            x = c + str(r)
            present = x in selected
            bits[c] = {"present": present,
                       "A": bool(present and A in ancestry[x]),
                       "B": bool(present and B in ancestry[x])}
        ca, cb = observation(A, r, CORRECT), observation(B, r, CORRECT)
        aa, ab = observation(A, r), observation(B, r)
        table.append({"round": r, "bits": bits, "C_A_cor": ca, "C_B_cor": cb,
                      "D_cor": ca - cb, "C_A": aa, "C_B": ab, "Delta": aa - ab})
    assert eligibility(A, CORRECT) and eligibility(B, CORRECT)
    pair_window = list(range(max(vertex_round(A), vertex_round(B)) + 1,
                             max(vertex_round(A), vertex_round(B)) + W + 1))
    assert any(row["round"] in pair_window and row["D_cor"] >= THETA + F for row in table)

    edges, visibility = set(), []
    for x in target_slice:
        for y in target_slice:
            if x == y:
                continue
            if x in ancestry[y]:
                edges.add((x, y))
                continue
            if y in ancestry[x] or not eligibility(x) or not eligibility(y):
                continue
            window = range(max(vertex_round(x), vertex_round(y)) + 1,
                           max(vertex_round(x), vertex_round(y)) + W + 1)
            margins = [observation(x, r) - observation(y, r) for r in window]
            if max(margins) >= THETA and min(margins) > -THETA:
                edges.add((x, y))
                visibility.append({"from": x, "to": y, "margins": margins})
    assert (A, B) in edges

    def topological_order(constraints):
        remaining, ordered = set(target_slice), []
        while remaining:
            ready = sorted((x for x in remaining
                            if not any(y == x and z in remaining for z, y in constraints)), key=key)
            assert ready, "cycle detected"
            chosen = ready[0]
            ordered.append(chosen)
            remaining.remove(chosen)
        assert all(ordered.index(x) < ordered.index(y) for x, y in constraints)
        return ordered

    order = topological_order(edges)
    causal_edges = {(x, y) for x in target_slice for y in target_slice
                    if x != y and x in ancestry[y]}
    causal_order = topological_order(causal_edges)
    assert order.index(A) < order.index(B)
    assert causal_order.index(B) < causal_order.index(A)
    try:
        topological_order(edges | {(B, A)})
    except AssertionError as failure:
        assert str(failure) == "cycle detected"
    else:
        raise AssertionError("B-first must be incompatible with the MRV graph")
    selected_digest = hashlib.sha256(canonical(sorted(digests[x] for x in selected)).encode()).hexdigest()
    return {"variant": variant, "parents": p, "abstract_header_digests": digests,
            "events": events, "target_slice_index": 2, "horizon": horizon,
            "selected_event": selected_event["index"], "selected_prefix_digest": selected_digest,
            "target_windows": {"I_A": list(range(3, 8)), "I_B": list(range(2, 7)), "J": pair_window},
            "observation_table": table,
            "correct_only_eligibility_witnesses": {"A": eligibility(A, CORRECT), "B": eligibility(B, CORRECT)},
            "all_target_slice_vertices_eligible": all(eligibility(x) for x in target_slice),
            "graph_edges": sorted(map(list, edges), key=lambda e: (key(e[0]), key(e[1]))),
            "visibility_edges": sorted(visibility, key=lambda e: (key(e["from"]), key(e["to"]))),
            "graph_edge_count": len(edges), "causal_edge_count": len(causal_edges),
            "order_with_key_round_creator": order,
            "causal_only_order_with_same_key": causal_order}


def artifact():
    minus, plus = build("minus"), build("plus")
    for x in (A, B):
        assert minus["abstract_header_digests"][x] == plus["abstract_header_digests"][x]
    assert minus["target_windows"] == plus["target_windows"]
    assert minus["events"] == plus["events"]
    for old, new in zip(minus["observation_table"], plus["observation_table"]):
        assert old["round"] == new["round"]
        for c in CORRECT:
            assert old["bits"][c] == new["bits"][c]
    before, after = minus["observation_table"][2], plus["observation_table"][2]
    zold, znew = before["bits"]["Z"], after["bits"]["Z"]
    assert int(zold["A"]) - int(zold["B"]) == -1
    assert int(znew["A"]) - int(znew["B"]) == 0
    assert minus["selected_prefix_digest"] != plus["selected_prefix_digest"]
    changed_correct = [x for x in minus["events"][-1]["prefix"] if x[0] in CORRECT
                       and minus["abstract_header_digests"][x] != plus["abstract_header_digests"][x]]
    assert changed_correct
    return {"scope": "abstract DAG/exporter-contract witness, not a protocol execution",
            "parameters": {"n": 4, "f": F, "q": Q, "q_vis": Q, "theta": THETA, "W": W},
            "targets": {"A": A, "B": B}, "fixed_faulty_creator": "Z",
            "checks": {"parent_quorum_and_previous_round": True, "creator_round_uniqueness": True,
                       "content_addressed_parent_consistency": True, "nested_ancestor_closed_prefixes": True,
                       "exact_once_causal_slice_delivery": True, "selected_snapshot_first_horizon": True,
                       "correct_presence_and_target_bits_fixed": True,
                       "fixed_complete_target_headers": True, "faulty_contribution_nontrivially_changes": True,
                       "correct_only_eligibility_and_direction_Q": True,
                       "full_target_slice_graph_acyclic": True, "MRV_A_first_causal_key_B_first": True},
            "changed_correct_abstract_header_digests": changed_correct,
            "inputs": [minus, plus]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-new", action="store_true")
    args = parser.parse_args()
    result = artifact()
    output = Path(__file__).with_name("nontrivial_witness.json")
    if args.write_new:
        with output.open("x", encoding="utf-8") as handle:
            json.dump(result, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
    else:
        with output.open(encoding="utf-8") as handle:
            assert json.load(handle) == result, "saved witness differs from recomputation"
    print(json.dumps({"checks": result["checks"],
                      "margins": {s["variant"]: [r["Delta"] for r in s["observation_table"] if r["round"] in s["target_windows"]["J"]] for s in result["inputs"]},
                      "changed_correct_header_count": len(result["changed_correct_abstract_header_digests"]),
                      "orders": {s["variant"]: s["order_with_key_round_creator"] for s in result["inputs"]}}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
