# Copyright(C) Facebook, Inc. and its affiliates.
from csv import DictWriter, reader
from datetime import datetime
from glob import glob
from math import isfinite
from multiprocessing import Pool
from os.path import basename, exists, getsize, join
from re import findall, search
from statistics import mean

from benchmark.config import DEFAULT_MRV_WINDOW
from benchmark.utils import Print


MRV_SLICE_MARKER = 'MRV_SliceStats'
MRV_REGISTERED_MARKER = 'MRV_SliceRegistered'
MRV_RELEASE_MARKER = 'MRV_SliceRelease'
MRV_EXACT_ONCE_MARKER = 'MRV_ExactOnceViolation'
MRV_LIFECYCLE_MARKER = 'MRV_MemberLifecycleViolation'

MRV_INTEGER_FIELDS = (
    'slice_id',
    'mrv_window',
    'slice_size',
    'slice_max_round',
    'seal_horizon',
    'seal_wait_rounds',
    'seal_delay_ms',
    'snapshot_frontier',
    'trigger_slice_id',
    'eligible_vertex_count',
    'ineligible_vertex_count',
    'all_pair_count',
    'causal_pair_count',
    'incomparable_pair_count',
    'eligible_pair_count',
    'ineligible_pair_count',
    'edge_count',
    'conflict_count',
    'no_signal_count',
    'intra_scc_ordering_edge_count',
    'inter_scc_ordering_edge_count',
    'constrained_pair_count',
    'scc_count',
    'nontrivial_scc_count',
    'vertices_in_nontrivial_sccs',
    'max_scc_size',
    'incomparable_pair_inversion_count',
    'constrained_inversion_count',
    'unconstrained_inversion_count',
    'moved_vertex_count',
)
MRV_FLOAT_FIELDS = (
    'position_displacement_median',
    'position_displacement_p95',
)
MRV_OPTIONAL_INTEGER_FIELDS = ('release_delay_ms',)
MRV_BOOLEAN_FIELDS = ('unchanged_slice',)
MRV_STATS_DIGEST_FIELDS = (
    'trigger_export_prefix_digest',
    'execution_order_digest',
)
MRV_REGISTRATION_FIELDS = (
    'leader_round',
    'leader_digest',
    'member_set_digest',
    'member_order_digest',
    'cumulative_member_count',
    'export_prefix_digest',
)
MRV_RAW_FIELDS = (
    MRV_INTEGER_FIELDS
    + MRV_OPTIONAL_INTEGER_FIELDS
    + MRV_BOOLEAN_FIELDS
    + MRV_FLOAT_FIELDS
    + MRV_STATS_DIGEST_FIELDS
    + MRV_REGISTRATION_FIELDS
)

MRV_RATE_SPECS = (
    ('eligibility_rate', 'eligible_vertex_count', 'slice_size'),
    ('causal_pair_rate', 'causal_pair_count', 'all_pair_count'),
    ('incomparable_pair_rate', 'incomparable_pair_count', 'all_pair_count'),
    ('eligible_pair_rate', 'eligible_pair_count', 'incomparable_pair_count'),
    ('ineligible_pair_rate', 'ineligible_pair_count', 'incomparable_pair_count'),
    ('conflict_pair_rate', 'conflict_count', 'incomparable_pair_count'),
    ('no_signal_pair_rate', 'no_signal_count', 'incomparable_pair_count'),
    ('edge_yield', 'edge_count', 'eligible_pair_count'),
    ('conflict_rate', 'conflict_count', 'eligible_pair_count'),
    ('no_signal_rate', 'no_signal_count', 'eligible_pair_count'),
    ('cross_scc_edge_rate', 'inter_scc_ordering_edge_count', 'edge_count'),
    (
        'nontrivial_scc_vertex_rate',
        'vertices_in_nontrivial_sccs',
        'slice_size',
    ),
    ('direct_edge_coverage', 'edge_count', 'incomparable_pair_count'),
    (
        'strict_direct_edge_coverage',
        'inter_scc_ordering_edge_count',
        'incomparable_pair_count',
    ),
    (
        'constrained_pair_coverage',
        'constrained_pair_count',
        'incomparable_pair_count',
    ),
    (
        'incomparable_pair_inversion_rate',
        'incomparable_pair_inversion_count',
        'incomparable_pair_count',
    ),
    (
        'constrained_inversion_rate',
        'constrained_inversion_count',
        'constrained_pair_count',
    ),
    (
        'unconstrained_inversion_rate',
        'unconstrained_inversion_count',
        'unconstrained_pair_count',
    ),
    ('moved_vertex_rate', 'moved_vertex_count', 'slice_size'),
    ('average_scc_size', 'slice_size', 'scc_count'),
)

MRV_CONTEXT_FIELDS = (
    'experiment_id',
    'deployment',
    'node_count',
    'input_rate',
    'client_target_rate',
    'faults',
    'run_index',
    'replica_index',
    'source_log',
)
MRV_STATUS_FIELDS = (
    'data_status',
    'derived_status',
    'release_delay_status',
    'cross_replica_status',
    'mismatch_class',
    'lifecycle_reason',
    'member_digest',
    'first_slice_id',
    'duplicate_slice_id',
    'member_round',
    'member_creator',
    'observed_replica_count',
    'expected_replica_count',
    'snapshot_frontier_mismatch',
    'error',
    'raw_record',
)
MRV_RATE_FIELDS = tuple(
    field for name, _, _ in MRV_RATE_SPECS for field in (name, f'{name}_status')
)
MRV_CSV_FIELDS = (
    MRV_CONTEXT_FIELDS
    + MRV_RAW_FIELDS
    + ('unconstrained_pair_count', 'unchanged_slice_rate')
    + MRV_RATE_FIELDS + MRV_STATUS_FIELDS
)


def _parse_mrv_slice_line(line):
    """Parse one benchmark-only aggregate record without guessing fields."""
    _, marker, body = line.partition(MRV_SLICE_MARKER)
    if not marker:
        raise ValueError(f'missing {MRV_SLICE_MARKER} marker')

    values = {}
    for token in body.lstrip(': ').split():
        if '=' not in token:
            raise ValueError(f'malformed token {token!r}')
        key, value = token.split('=', 1)
        if key in values:
            raise ValueError(f'duplicate field {key}')
        values[key] = value

    required = set(
        MRV_INTEGER_FIELDS
        + MRV_BOOLEAN_FIELDS
        + MRV_FLOAT_FIELDS
        + MRV_STATS_DIGEST_FIELDS
    )
    missing = sorted(required - set(values))
    if missing:
        raise ValueError(f'missing field(s): {", ".join(missing)}')

    record = {}
    try:
        for key in MRV_INTEGER_FIELDS:
            record[key] = int(values[key])
        for key in MRV_FLOAT_FIELDS:
            record[key] = float(values[key])
    except ValueError as e:
        raise ValueError(f'invalid numeric field: {e}') from e

    for key in MRV_BOOLEAN_FIELDS:
        value = values[key].lower()
        if value not in ('true', 'false'):
            raise ValueError(f'invalid boolean field {key}={values[key]!r}')
        record[key] = value == 'true'

    for key in MRV_STATS_DIGEST_FIELDS:
        record[key] = values[key]

    if any(not isfinite(record[key]) for key in MRV_FLOAT_FIELDS):
        raise ValueError('non-finite displacement value')

    for key in MRV_OPTIONAL_INTEGER_FIELDS:
        value = values.get(key, 'unavailable').lower()
        if value == 'unavailable':
            record[key] = None
        else:
            try:
                record[key] = int(value)
            except ValueError as e:
                raise ValueError(f'invalid numeric field {key}={value!r}') from e

    non_negative_fields = MRV_INTEGER_FIELDS + MRV_OPTIONAL_INTEGER_FIELDS
    if any(record[key] is not None and record[key] < 0 for key in non_negative_fields):
        raise ValueError('negative count, round, window, or delay')
    if record['mrv_window'] < 1:
        raise ValueError('mrv_window must be positive')

    expected_pairs = record['slice_size'] * (record['slice_size'] - 1) // 2
    if record['all_pair_count'] != expected_pairs:
        raise ValueError('all_pair_count does not match slice_size')
    if (
        record['eligible_vertex_count'] + record['ineligible_vertex_count']
        != record['slice_size']
    ):
        raise ValueError('vertex eligibility identity failed')

    if record['all_pair_count'] != (
        record['causal_pair_count'] + record['incomparable_pair_count']
    ):
        raise ValueError('all_pair_count identity failed')
    if record['incomparable_pair_count'] != (
        record['ineligible_pair_count']
        + record['edge_count']
        + record['conflict_count']
        + record['no_signal_count']
    ):
        raise ValueError('incomparable_pair_count identity failed')
    if record['eligible_pair_count'] != (
        record['edge_count']
        + record['conflict_count']
        + record['no_signal_count']
    ):
        raise ValueError('eligible_pair_count identity failed')
    if (
        record['eligible_pair_count'] + record['ineligible_pair_count']
        != record['incomparable_pair_count']
    ):
        raise ValueError('pair eligibility identity failed')
    if (
        record['intra_scc_ordering_edge_count']
        + record['inter_scc_ordering_edge_count']
        != record['edge_count']
    ):
        raise ValueError('ordering edge SCC identity failed')
    if record['constrained_pair_count'] > record['incomparable_pair_count']:
        raise ValueError('constrained_pair_count exceeds incomparable pairs')
    if (
        record['incomparable_pair_inversion_count']
        > record['incomparable_pair_count']
    ):
        raise ValueError('inversion count exceeds incomparable pairs')
    if record['incomparable_pair_inversion_count'] != (
        record['constrained_inversion_count']
        + record['unconstrained_inversion_count']
    ):
        raise ValueError('inversion decomposition identity failed')
    record['unconstrained_pair_count'] = (
        record['incomparable_pair_count'] - record['constrained_pair_count']
    )
    if record['constrained_inversion_count'] > record['constrained_pair_count']:
        raise ValueError('constrained inversions exceed constrained pairs')
    if (
        record['unconstrained_inversion_count']
        > record['unconstrained_pair_count']
    ):
        raise ValueError('unconstrained inversions exceed unconstrained pairs')
    if record['moved_vertex_count'] > record['slice_size']:
        raise ValueError('moved_vertex_count exceeds slice_size')
    if record['seal_horizon'] != (
        record['slice_max_round'] + record['mrv_window']
    ):
        raise ValueError('seal_horizon identity failed')
    if record['seal_wait_rounds'] < record['mrv_window']:
        raise ValueError('seal_wait_rounds is shorter than mrv_window')

    median = record['position_displacement_median']
    p95 = record['position_displacement_p95']
    if not 0.0 <= median <= p95 <= 1.0:
        raise ValueError('normalized displacement must satisfy 0 <= median <= p95 <= 1')
    if record['unchanged_slice'] != (record['moved_vertex_count'] == 0):
        raise ValueError('unchanged_slice and moved_vertex_count disagree')
    if record['unchanged_slice'] and (median != 0.0 or p95 != 0.0):
        raise ValueError('unchanged slice has non-zero displacement')

    slice_size = record['slice_size']
    scc_count = record['scc_count']
    nontrivial_scc_count = record['nontrivial_scc_count']
    nontrivial_vertices = record['vertices_in_nontrivial_sccs']
    max_scc_size = record['max_scc_size']
    if slice_size < 1:
        raise ValueError('execution slice must be nonempty')
    if not 1 <= scc_count <= slice_size:
        raise ValueError('scc_count is outside slice bounds')
    if nontrivial_scc_count > scc_count:
        raise ValueError('nontrivial_scc_count exceeds scc_count')
    if nontrivial_vertices > slice_size:
        raise ValueError('vertices_in_nontrivial_sccs exceeds slice_size')
    if slice_size != nontrivial_vertices + scc_count - nontrivial_scc_count:
        raise ValueError('SCC vertex accounting identity failed')
    if not 1 <= max_scc_size <= slice_size:
        raise ValueError('max_scc_size is outside slice bounds')
    if nontrivial_scc_count == 0:
        if nontrivial_vertices != 0 or max_scc_size != 1:
            raise ValueError('trivial SCC statistics disagree')
    elif (
        nontrivial_vertices < 2 * nontrivial_scc_count
        or not 2 <= max_scc_size <= nontrivial_vertices
    ):
        raise ValueError('nontrivial SCC statistics disagree')

    release_delay_ms = record['release_delay_ms']
    if release_delay_ms is not None and release_delay_ms < record['seal_delay_ms']:
        raise ValueError('release_delay_ms precedes seal_delay_ms')

    record['data_status'] = 'valid'
    record['release_delay_status'] = (
        'valid' if record['release_delay_ms'] is not None else 'unavailable'
    )
    record['error'] = ''
    record['raw_record'] = ''
    record['unchanged_slice_rate'] = 1.0 if record['unchanged_slice'] else 0.0

    for name, numerator, denominator in MRV_RATE_SPECS:
        if record[denominator] == 0:
            record[name] = ''
            record[f'{name}_status'] = 'zero_denominator'
        else:
            record[name] = record[numerator] / record[denominator]
            record[f'{name}_status'] = 'valid'
    # A zero denominator applies only to that metric. The raw slice remains a
    # valid row and must not be filtered out wholesale.
    record['derived_status'] = 'valid'

    return record


def _parse_mrv_registered_line(line):
    """Parse the lifecycle record emitted when a nonempty slice is registered."""
    _, marker, body = line.partition(MRV_REGISTERED_MARKER)
    if not marker:
        raise ValueError(f'missing {MRV_REGISTERED_MARKER} marker')

    values = {}
    for token in body.lstrip(': ').split():
        if '=' not in token:
            raise ValueError(f'malformed token {token!r}')
        key, value = token.split('=', 1)
        if key in values:
            raise ValueError(f'duplicate field {key}')
        values[key] = value

    integer_fields = {
        'slice_id',
        'leader_round',
        'slice_size',
        'seal_horizon',
        'cumulative_member_count',
    }
    digest_fields = {
        'leader_digest',
        'member_set_digest',
        'member_order_digest',
        'export_prefix_digest',
    }
    expected = integer_fields | digest_fields
    missing = sorted(expected - set(values))
    unexpected = sorted(set(values) - expected)
    if missing:
        raise ValueError(f'missing field(s): {", ".join(missing)}')
    if unexpected:
        raise ValueError(f'unexpected field(s): {", ".join(unexpected)}')

    try:
        record = {key: int(values[key]) for key in integer_fields}
    except ValueError as e:
        raise ValueError(f'invalid numeric field: {e}') from e
    if any(record[key] < 0 for key in integer_fields):
        raise ValueError('negative registration field')
    if record['slice_size'] < 1:
        raise ValueError('registered slice must be nonempty')
    for key in digest_fields:
        record[key] = values[key]
    record['raw_record'] = line.strip()
    return record


def _parse_mrv_release_line(line):
    """Parse one release-delay update for a previously sealed slice."""
    _, marker, body = line.partition(MRV_RELEASE_MARKER)
    if not marker:
        raise ValueError(f'missing {MRV_RELEASE_MARKER} marker')

    values = {}
    for token in body.lstrip(': ').split():
        if '=' not in token:
            raise ValueError(f'malformed token {token!r}')
        key, value = token.split('=', 1)
        if key in values:
            raise ValueError(f'duplicate field {key}')
        values[key] = value

    expected = {'slice_id', 'release_delay_ms'}
    missing = sorted(expected - set(values))
    unexpected = sorted(set(values) - expected)
    if missing:
        raise ValueError(f'missing field(s): {", ".join(missing)}')
    if unexpected:
        raise ValueError(f'unexpected field(s): {", ".join(unexpected)}')

    try:
        slice_id = int(values['slice_id'])
        release_delay_ms = int(values['release_delay_ms'])
    except ValueError as e:
        raise ValueError(f'invalid numeric field: {e}') from e
    if slice_id < 0 or release_delay_ms < 0:
        raise ValueError('negative slice id or release delay')
    return slice_id, release_delay_ms


def _parse_mrv_diagnostic_line(line, marker):
    """Parse one invariant marker emitted immediately before a panic."""
    _, found, body = line.partition(marker)
    if not found:
        raise ValueError(f'missing {marker} marker')

    values = {}
    for token in body.lstrip(': ').split():
        if '=' not in token:
            raise ValueError(f'malformed token {token!r}')
        key, value = token.split('=', 1)
        if key in values:
            raise ValueError(f'duplicate field {key}')
        values[key] = value

    if marker == MRV_EXACT_ONCE_MARKER:
        expected = {
            'first_slice_id',
            'duplicate_slice_id',
            'member_digest',
            'member_round',
            'member_creator',
        }
    else:
        expected = {'slice_id', 'member_digest', 'reason'}
    missing = sorted(expected - set(values))
    unexpected = sorted(set(values) - expected)
    if missing:
        raise ValueError(f'missing field(s): {", ".join(missing)}')
    if unexpected:
        raise ValueError(f'unexpected field(s): {", ".join(unexpected)}')

    try:
        slice_id = int(
            values[
                'duplicate_slice_id'
                if marker == MRV_EXACT_ONCE_MARKER
                else 'slice_id'
            ]
        )
        first_slice_id = (
            int(values['first_slice_id'])
            if marker == MRV_EXACT_ONCE_MARKER
            else None
        )
        member_round = (
            int(values['member_round'])
            if marker == MRV_EXACT_ONCE_MARKER
            else None
        )
    except ValueError as e:
        raise ValueError(f'invalid slice_id: {e}') from e
    if slice_id < 0 or (first_slice_id is not None and first_slice_id < 0):
        raise ValueError('negative slice id')
    if member_round is not None and member_round < 0:
        raise ValueError('negative member round')

    record = _mrv_status_record(
        'diagnostic_failure',
        marker,
        line.strip(),
    )
    record.update(
        {
            'slice_id': slice_id,
            'member_digest': values['member_digest'],
            'first_slice_id': first_slice_id if first_slice_id is not None else '',
            'duplicate_slice_id': (
                slice_id if marker == MRV_EXACT_ONCE_MARKER else ''
            ),
            'member_round': member_round if member_round is not None else '',
            'member_creator': values.get('member_creator', ''),
            'mismatch_class': (
                'EXACT_ONCE_VIOLATION'
                if marker == MRV_EXACT_ONCE_MARKER
                else 'MEMBER_LIFECYCLE_VIOLATION'
            ),
            'lifecycle_reason': values.get('reason', ''),
        }
    )
    return record


def _reconcile_mrv_registrations(
    slice_records,
    registrations,
    stats_seen_ids,
    saw_registration_marker,
):
    """Expose unsealed registered slices and reject inconsistent lifecycle logs."""
    registrations_by_slice = {}
    for registration in registrations:
        registrations_by_slice.setdefault(registration['slice_id'], []).append(
            registration
        )

    valid_stats_by_slice = {}
    for record in slice_records:
        if record.get('data_status') == 'valid':
            valid_stats_by_slice.setdefault(record['slice_id'], []).append(record)

    for slice_id, matching_registrations in registrations_by_slice.items():
        matching_stats = valid_stats_by_slice.get(slice_id, [])
        if len(matching_registrations) != 1:
            for record in matching_stats:
                _mark_mrv_record_malformed(
                    record, f'duplicate registration for slice_id {slice_id}'
                )
            duplicate = _mrv_status_record(
                'malformed',
                f'duplicate registration for slice_id {slice_id}',
                matching_registrations[0]['raw_record'],
            )
            duplicate.update(
                {
                    key: matching_registrations[0][key]
                    for key in (
                        'slice_id',
                        'leader_round',
                        'leader_digest',
                        'slice_size',
                        'seal_horizon',
                        'member_set_digest',
                        'member_order_digest',
                        'cumulative_member_count',
                        'export_prefix_digest',
                    )
                }
            )
            slice_records.append(duplicate)
            continue

        registration = matching_registrations[0]
        if slice_id not in stats_seen_ids:
            censored = _mrv_status_record(
                'right_censored',
                f'registered slice_id {slice_id} has no {MRV_SLICE_MARKER} record',
                registration['raw_record'],
            )
            censored.update(
                {
                    key: registration[key]
                    for key in (
                        'slice_id',
                        'leader_round',
                        'leader_digest',
                        'slice_size',
                        'seal_horizon',
                        'member_set_digest',
                        'member_order_digest',
                        'cumulative_member_count',
                        'export_prefix_digest',
                    )
                }
            )
            slice_records.append(censored)
        elif len(matching_stats) == 1:
            stats = matching_stats[0]
            stats.update(
                {
                    key: registration[key]
                    for key in MRV_REGISTRATION_FIELDS
                }
            )
            if (
                stats['slice_size'] != registration['slice_size']
                or stats['seal_horizon'] != registration['seal_horizon']
            ):
                _mark_mrv_record_malformed(
                    stats,
                    f'registration fields disagree for slice_id {slice_id}',
                )

    if saw_registration_marker:
        registered_ids = set(registrations_by_slice)
        for record in slice_records:
            if (
                record.get('data_status') == 'valid'
                and record['slice_id'] not in registered_ids
            ):
                _mark_mrv_record_malformed(
                    record,
                    f'{MRV_SLICE_MARKER} has no registration for '
                    f'slice_id {record["slice_id"]}',
                )


def _merge_mrv_release_updates(slice_records, release_updates):
    """Merge valid per-primary release updates without guessing a slice."""
    records_by_slice = {}
    for record in slice_records:
        if record.get('data_status') == 'valid':
            records_by_slice.setdefault(record['slice_id'], []).append(record)

    updates_by_slice = {}
    for slice_id, release_delay_ms, raw_record in release_updates:
        updates_by_slice.setdefault(slice_id, []).append(
            (release_delay_ms, raw_record)
        )

    for slice_id, updates in updates_by_slice.items():
        matching_records = records_by_slice.get(slice_id, [])
        if not matching_records:
            slice_records.append(
                _mrv_status_record(
                    'malformed',
                    f'unknown release slice_id {slice_id}',
                    updates[0][1],
                )
            )
            continue
        if len(matching_records) != 1:
            for record in matching_records:
                _mark_mrv_record_malformed(
                    record,
                    f'release update is ambiguous for duplicate slice_id {slice_id}',
                )
            continue

        record = matching_records[0]
        if len(updates) != 1 or record['release_delay_ms'] is not None:
            _mark_mrv_record_malformed(
                record, f'duplicate release for slice_id {slice_id}'
            )
            continue

        release_delay_ms, _ = updates[0]
        if release_delay_ms < record['seal_delay_ms']:
            _mark_mrv_record_malformed(
                record,
                f'release_delay_ms precedes seal_delay_ms for slice_id {slice_id}',
            )
            continue
        record['release_delay_ms'] = release_delay_ms
        record['release_delay_status'] = 'valid'


def _mrv_status_record(status, error, raw_record=''):
    record = {
        'data_status': status,
        'derived_status': status,
        'release_delay_status': status,
        'error': error,
        'raw_record': raw_record,
    }
    for name, _, _ in MRV_RATE_SPECS:
        record[f'{name}_status'] = status
    return record


def _mark_mrv_record_malformed(record, error):
    record['data_status'] = 'malformed'
    record['derived_status'] = 'malformed'
    record['release_delay_status'] = 'malformed'
    record['error'] = error
    record['unchanged_slice_rate'] = ''
    for name, _, _ in MRV_RATE_SPECS:
        record[name] = ''
        record[f'{name}_status'] = 'malformed'


def _mark_duplicate_mrv_slices(records):
    counts = {}
    for record in records:
        if record.get('data_status') == 'valid':
            slice_id = record['slice_id']
            counts[slice_id] = counts.get(slice_id, 0) + 1
    for record in records:
        if (
            record.get('data_status') == 'valid'
            and counts[record['slice_id']] > 1
        ):
            _mark_mrv_record_malformed(
                record, f'duplicate slice_id {record["slice_id"]}'
            )


def _classify_mrv_cross_replica(records):
    """Classify a slice only after all primary logs have been parsed."""
    group_fields = (
        'experiment_id',
        'deployment',
        'node_count',
        'input_rate',
        'faults',
        'run_index',
        'mrv_window',
        'slice_id',
    )
    groups = {}
    for record in records:
        if record.get('data_status') == 'diagnostic_failure':
            record['cross_replica_status'] = 'diagnostic_failure'
            continue
        if 'slice_id' not in record:
            record['cross_replica_status'] = 'not_comparable'
            continue
        key = tuple(record.get(field) for field in group_fields)
        groups.setdefault(key, []).append(record)

    identity_fields = (
        'leader_round',
        'leader_digest',
        'member_set_digest',
        'member_order_digest',
        'trigger_slice_id',
        'trigger_export_prefix_digest',
        'execution_order_digest',
    )
    for group in groups.values():
        faults = group[0].get('faults')
        node_count = group[0].get('node_count')
        observed = {record.get('replica_index') for record in group}
        for record in group:
            record['observed_replica_count'] = len(observed)

        if (
            not isinstance(faults, int)
            or faults != 0
            or not isinstance(node_count, int)
        ):
            for record in group:
                record['cross_replica_status'] = 'not_comparable_faults'
                record['expected_replica_count'] = ''
            continue

        expected = set(range(node_count))
        for record in group:
            record['expected_replica_count'] = len(expected)
        if observed != expected or any(
            record.get('data_status') != 'valid' for record in group
        ):
            for record in group:
                record['cross_replica_status'] = 'incomplete'
            continue
        if any(
            any(field not in record for field in identity_fields)
            for record in group
        ):
            for record in group:
                record['cross_replica_status'] = 'identity_unavailable'
            continue

        classes = []
        leaders = {
            (record['leader_round'], record['leader_digest']) for record in group
        }
        if len(leaders) > 1:
            classes.append('LEADER_MAPPING_MISMATCH')
        elif len({record['member_set_digest'] for record in group}) > 1:
            classes.append('MEMBER_SET_MISMATCH')
        else:
            if len({record['member_order_digest'] for record in group}) > 1:
                classes.append('BASE_ORDER_MISMATCH')
            seal_prefixes = {
                (
                    record['trigger_slice_id'],
                    record['trigger_export_prefix_digest'],
                )
                for record in group
            }
            if len(seal_prefixes) > 1:
                classes.append('SEAL_PREFIX_MISMATCH')
            if len({record['execution_order_digest'] for record in group}) > 1:
                classes.append('EXECUTION_ORDER_MISMATCH')

        frontier_mismatch = len(
            {record['snapshot_frontier'] for record in group}
        ) > 1
        for record in group:
            record['cross_replica_status'] = (
                'mismatch' if classes else 'consistent'
            )
            record['mismatch_class'] = ','.join(classes)
            record['snapshot_frontier_mismatch'] = frontier_mismatch


class ParseError(Exception):
    pass


class LogParser:
    def __init__(
        self,
        clients,
        primaries,
        workers,
        faults=0,
        deployment=None,
        node_count=None,
        input_rate=None,
        mrv_window=None,
        run_index=1,
        experiment_id='unavailable',
        primary_indices=None,
    ):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(primaries) + int(faults)
            self.workers =  len(workers) // len(primaries)
        else:
            self.committee_size = '?'
            self.workers = '?'
        self.deployment = deployment or 'unavailable'
        self.node_count = node_count if node_count is not None else self.committee_size
        self.run_index = run_index
        self.experiment_id = experiment_id
        primary_indices = primary_indices or list(range(len(primaries)))

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse clients\' logs: {e}')
        self.size, self.rate, self.start, misses, self.sent_samples \
            = zip(*results)
        self.misses = sum(misses)

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse nodes\' logs: {e}')
        (
            proposals,
            consensus_commits,
            execution_commits,
            slice_records,
            self.configs,
            primary_ips,
        ) = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.consensus_commits = self._merge_results([x.items() for x in consensus_commits])
        self.execution_commits = self._merge_results([x.items() for x in execution_commits])

        configured_window = next(
            (x['mrv_window'] for x in self.configs if x['mrv_window'] is not None),
            None,
        )
        logged_window = next(
            (
                record['mrv_window']
                for records in slice_records
                for record in records
                if record.get('data_status') == 'valid'
            ),
            None,
        )
        self.mrv_window = (
            mrv_window
            if mrv_window is not None
            else configured_window if configured_window is not None
            else logged_window if logged_window is not None
            else DEFAULT_MRV_WINDOW
        )
        self.input_rate = input_rate if input_rate is not None else sum(self.rate)

        self.mrv_slice_records = []
        for replica_index, records, config in zip(
            primary_indices, slice_records, self.configs
        ):
            for source in records:
                record = dict(source)
                config_window = config['mrv_window']
                config_mismatch = (
                    config_window is not None
                    and config_window != self.mrv_window
                )
                if config_mismatch:
                    _mark_mrv_record_malformed(
                        record,
                        f'primary config mrv_window {config_window} does not '
                        f'match expected mrv_window {self.mrv_window}',
                    )
                elif record.get('data_status') == 'valid':
                    if record['mrv_window'] != self.mrv_window:
                        _mark_mrv_record_malformed(
                            record,
                            f'logged mrv_window {record["mrv_window"]} does not '
                            f'match configured mrv_window {self.mrv_window}',
                        )

                context = {
                    'experiment_id': self.experiment_id,
                    'deployment': self.deployment,
                    'node_count': self.node_count,
                    'input_rate': self.input_rate,
                    'client_target_rate': sum(self.rate),
                    'faults': self.faults,
                    'run_index': self.run_index,
                    'replica_index': replica_index,
                    'source_log': f'primary-{replica_index}.log',
                }
                if 'mrv_window' not in record:
                    record['mrv_window'] = self.mrv_window
                self.mrv_slice_records.append({**context, **record})

        _classify_mrv_cross_replica(self.mrv_slice_records)

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        sizes, self.received_samples, workers_ips = zip(*results)
        committed = set(self.consensus_commits) | set(self.execution_commits)
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in committed
        }

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}

        return size, rate, start, misses, samples

    def _parse_primaries(self, log):
        has_diagnostic_marker = (
            MRV_EXACT_ONCE_MARKER in log or MRV_LIFECYCLE_MARKER in log
        )
        if search(r'(?:panicked|Error)', log) is not None and not has_diagnostic_marker:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Tusk_Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        consensus_commits = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* MRV_Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        execution_commits = self._merge_results([tmp])

        slice_records = []
        registrations = []
        release_updates = []
        stats_seen_ids = set()
        saw_registration_marker = False
        for line in log.splitlines():
            if MRV_EXACT_ONCE_MARKER in line:
                try:
                    slice_records.append(
                        _parse_mrv_diagnostic_line(line, MRV_EXACT_ONCE_MARKER)
                    )
                except ValueError as e:
                    slice_records.append(
                        _mrv_status_record(
                            'malformed',
                            f'invalid {MRV_EXACT_ONCE_MARKER}: {e}',
                            line.strip(),
                        )
                    )
            elif MRV_LIFECYCLE_MARKER in line:
                try:
                    slice_records.append(
                        _parse_mrv_diagnostic_line(line, MRV_LIFECYCLE_MARKER)
                    )
                except ValueError as e:
                    slice_records.append(
                        _mrv_status_record(
                            'malformed',
                            f'invalid {MRV_LIFECYCLE_MARKER}: {e}',
                            line.strip(),
                        )
                    )
            elif MRV_REGISTERED_MARKER in line:
                saw_registration_marker = True
                try:
                    registrations.append(_parse_mrv_registered_line(line))
                except ValueError as e:
                    slice_records.append(
                        _mrv_status_record(
                            'malformed',
                            f'invalid {MRV_REGISTERED_MARKER}: {e}',
                            line.strip(),
                        )
                    )
            elif MRV_SLICE_MARKER in line:
                try:
                    record = _parse_mrv_slice_line(line)
                    slice_records.append(record)
                    stats_seen_ids.add(record['slice_id'])
                except ValueError as e:
                    match = search(
                        r'(?:^|\s)slice_id=(\d+)(?:\s|$)',
                        line.partition(MRV_SLICE_MARKER)[2],
                    )
                    if match:
                        stats_seen_ids.add(int(match.group(1)))
                    slice_records.append(
                        _mrv_status_record('malformed', str(e), line.strip())
                    )
            elif MRV_RELEASE_MARKER in line:
                try:
                    slice_id, release_delay_ms = _parse_mrv_release_line(line)
                    release_updates.append(
                        (slice_id, release_delay_ms, line.strip())
                    )
                except ValueError as e:
                    slice_records.append(
                        _mrv_status_record(
                            'malformed',
                            f'invalid {MRV_RELEASE_MARKER}: {e}',
                            line.strip(),
                        )
                    )

        _merge_mrv_release_updates(slice_records, release_updates)
        _mark_duplicate_mrv_slices(slice_records)
        _reconcile_mrv_registrations(
            slice_records,
            registrations,
            stats_seen_ids,
            saw_registration_marker,
        )
        if not slice_records:
            slice_records.append(
                _mrv_status_record(
                    'unavailable',
                    f'no {MRV_SLICE_MARKER} record in primary log',
                )
            )

        if not consensus_commits and not execution_commits:
            tmp = findall(r'\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
            tmp = [(d, self._to_posix(t)) for t, d in tmp]
            legacy_commits = self._merge_results([tmp])
            consensus_commits = legacy_commits
            execution_commits = legacy_commits

        configs = {
            'header_size': int(
                search(r'Header size .* (\d+)', log).group(1)
            ),
            'max_header_delay': int(
                search(r'Max header delay .* (\d+)', log).group(1)
            ),
            'gc_depth': int(
                search(r'Garbage collection depth .* (\d+)', log).group(1)
            ),
            'sync_retry_delay': int(
                search(r'Sync retry delay .* (\d+)', log).group(1)
            ),
            'sync_retry_nodes': int(
                search(r'Sync retry nodes .* (\d+)', log).group(1)
            ),
            'batch_size': int(
                search(r'Batch size .* (\d+)', log).group(1)
            ),
            'max_batch_delay': int(
                search(r'Max batch delay .* (\d+)', log).group(1)
            ),
            'mrv_window': (
                int(search(r'MRV window .* (\d+)', log).group(1))
                if search(r'MRV window .* (\d+)', log) is not None
                else None
            ),
        }

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)
        
        return proposals, consensus_commits, execution_commits, slice_records, configs, ip

    def _parse_workers(self, log):
        if search(r'(?:panic|Error)', log) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        return sizes, samples, ip

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _committed_bytes(self, commits):
        return sum(self.sizes[d] for d in commits if d in self.sizes)

    def _consensus_throughput(self):
        if not self.consensus_commits or not self.proposals:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.consensus_commits.values())
        duration = end - start
        bytes = self._committed_bytes(self.consensus_commits)
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [
            c - self.proposals[d]
            for d, c in self.consensus_commits.items()
            if d in self.proposals
        ]
        missing = len(self.consensus_commits) - len(latency)
        if missing:
            Print.warn(
                f'Skipping {missing:,} consensus latency sample(s) without a proposal timestamp'
            )
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        if not self.execution_commits:
            return 0, 0, 0
        start, end = min(self.start), max(self.execution_commits.values())
        duration = end - start
        bytes = self._committed_bytes(self.execution_commits)
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _end_to_end_latency(self):
        latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.execution_commits:
                    assert tx_id in sent  # We receive txs that we sent.
                    start = sent[tx_id]
                    end = self.execution_commits[batch_id]
                    latency += [end-start]
        return mean(latency) if latency else 0

    def result(self):
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']

        consensus_latency = self._consensus_latency() * 1_000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1_000
        valid_slices = sum(
            x.get('data_status') == 'valid' for x in self.mrv_slice_records
        )
        malformed_slices = sum(
            x.get('data_status') == 'malformed' for x in self.mrv_slice_records
        )
        unavailable_logs = sum(
            x.get('data_status') == 'unavailable' for x in self.mrv_slice_records
        )
        right_censored_slices = sum(
            x.get('data_status') == 'right_censored'
            for x in self.mrv_slice_records
        )
        exact_once_failures = sum(
            x.get('mismatch_class') == 'EXACT_ONCE_VIOLATION'
            for x in self.mrv_slice_records
        )
        lifecycle_failures = sum(
            x.get('mismatch_class') == 'MEMBER_LIFECYCLE_VIOLATION'
            for x in self.mrv_slice_records
        )

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} node(s)\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {self.input_rate:,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            f' Header size: {header_size:,} B\n'
            f' Max header delay: {max_header_delay:,} ms\n'
            f' GC depth: {gc_depth:,} round(s)\n'
            f' MRV window: {self.mrv_window:,} round(s)\n'
            f' Sync retry delay: {sync_retry_delay:,} ms\n'
            f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
            f' batch size: {batch_size:,} B\n'
            f' Max batch delay: {max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            f' MRV slice records: {valid_slices:,} valid, '
            f'{malformed_slices:,} malformed, '
            f'{unavailable_logs:,} unavailable, '
            f'{right_censored_slices:,} right-censored, '
            f'{exact_once_failures:,} exact-once violation(s), '
            f'{lifecycle_failures:,} lifecycle violation(s)\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    def print_mrv(self, filename):
        """Append one tidy row per replica/slice (or explicit error status)."""
        assert isinstance(filename, str)
        write_header = not exists(filename) or getsize(filename) == 0
        if not write_header:
            with open(filename, newline='') as f:
                existing_header = next(reader(f), [])
            if existing_header != list(MRV_CSV_FIELDS):
                raise ParseError(
                    'MRV CSV schema differs; archive or remove the old file '
                    'before starting this experiment'
                )
        with open(filename, 'a', newline='') as f:
            writer = DictWriter(
                f,
                fieldnames=MRV_CSV_FIELDS,
                extrasaction='ignore',
                restval='',
            )
            if write_header:
                writer.writeheader()
            writer.writerows(self.mrv_slice_records)

    @classmethod
    def process(
        cls,
        directory,
        faults=0,
        deployment=None,
        node_count=None,
        input_rate=None,
        mrv_window=None,
        run_index=1,
        experiment_id='unavailable',
    ):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        primaries = []
        primary_indices = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            with open(filename, 'r') as f:
                primaries += [f.read()]
            match = search(r'primary-(\d+)\.log$', basename(filename))
            primary_indices += [int(match.group(1)) if match else len(primary_indices)]
        workers = []
        for filename in sorted(glob(join(directory, 'worker-*.log'))):
            with open(filename, 'r') as f:
                workers += [f.read()]

        return cls(
            clients,
            primaries,
            workers,
            faults=faults,
            deployment=deployment,
            node_count=node_count,
            input_rate=input_rate,
            mrv_window=mrv_window,
            run_index=run_index,
            experiment_id=experiment_id,
            primary_indices=primary_indices,
        )

