"""Small fixed regularized logistic models; no runtime training or external ML dependency."""
import math
import statistics
from features import SCHEMA, NUMERIC, META
from logic import digest


def sigmoid(z):
    return 1/(1+math.exp(-max(-35., min(35., z))))


def fit_logistic(matrix, labels, steps=500, penalty=.02):
    weights = [0.]*(len(matrix[0])+1)
    for _ in range(steps):
        gradient = [0.]*len(weights)
        for row, label in zip(matrix, labels):
            error = sigmoid(weights[0]+sum(w*x for w, x in zip(weights[1:], row)))-label
            gradient[0] += error
            for j, value in enumerate(row, 1):
                gradient[j] += error*value
        weights = [w-.08*(g/len(labels)+(penalty*w if j else 0.)) for j, (w, g) in enumerate(zip(weights, gradient))]
    return weights


def vector(values, model):
    vals, missing = [], []
    for k, mean, scale in zip(model['names'], model['mean'], model['scale']):
        value = values.get(k)
        absent = value is None or not isinstance(value, (int, float)) or not math.isfinite(value)
        vals.append(0. if absent else max(-10., min(10., (value-mean)/scale)))
        missing.append(float(absent))
    return vals+missing


def score(values, model):
    x = vector(values, model)
    return model['weights'][0]+sum(w*v for w, v in zip(model['weights'][1:], x))


def fit(train, calibrate, names):
    model = {'names': list(names), 'mean': [], 'scale': []}
    for name in names:
        xs = [r['features'][name] for r in train if r['features'].get(name) is not None]
        model['mean'].append(statistics.mean(xs) if xs else 0.)
        model['scale'].append(max(statistics.pstdev(xs), 1e-6) if len(xs) > 1 else 1.)
    model['weights'] = fit_logistic([vector(r['features'], model) for r in train], [r['label'] for r in train])
    model['calibration'] = fit_logistic([[max(-10., min(10., score(r['features'], model))) ] for r in calibrate],
                                      [r['label'] for r in calibrate], penalty=.01)
    # Use the same clipping in training and inference calibration.
    model['calibration_score_clip'] = 10.
    return model


def calibrated(values, model):
    intercept, slope = model['calibration']
    raw = score(values, model)
    clip = model.get('calibration_score_clip', 10.)
    return sigmoid(intercept+slope*max(-clip, min(clip, raw)))


def probability_metrics(pairs):
    if not pairs:
        return {'n': 0, 'brier': None, 'log_loss': None, 'high_confidence_wrong_rate': None, 'bins': []}
    confident = [(p, y) for p, y in pairs if max(p, 1-p) >= .9]
    bins = []
    for lo, hi in ((0, .2), (.2, .4), (.4, .6), (.6, .8), (.8, 1.0000001)):
        items = [(p, y) for p, y in pairs if lo <= p < hi]
        bins.append({'lower': lo, 'upper': min(1, hi), 'n': len(items),
                     'mean_p': statistics.mean(p for p, _ in items) if items else None,
                     'observed_up': statistics.mean(y for _, y in items) if items else None})
    return {'n': len(pairs), 'brier': statistics.mean((p-y)**2 for p, y in pairs),
            'log_loss': statistics.mean(-y*math.log(max(1e-9, p))-(1-y)*math.log(max(1e-9, 1-p)) for p, y in pairs),
            'high_confidence_n': len(confident),
            'high_confidence_wrong_rate': statistics.mean((p >= .5) != bool(y) for p, y in confident) if confident else None,
            'bins': bins}


def temporal_split(rows, min_counts=(120, 40, 40)):
    rows = sorted(rows, key=lambda r: r['cutoff'])
    if len({r['start'] for r in rows}) != len(rows):
        raise ValueError('duplicate market within phase')
    if len(rows) < sum(min_counts):
        raise ValueError('need at least 200 complete markets per phase (120/40/40)')
    ntrain = max(min_counts[0], int(len(rows)*.6))
    ncal = max(min_counts[1], int(len(rows)*.2))
    train, cal, test = rows[:ntrain], rows[ntrain:ntrain+ncal], rows[ntrain+ncal:]
    # A five-minute outcome often arrives after the next market's E10 cutoff.
    # Purge unavailable labels instead of leaking them or asking for manual edits.
    train = [r for r in train if r['known_at'] < cal[0]['cutoff']]
    cal = [r for r in cal if r['known_at'] < test[0]['cutoff']]
    if any(len(part) < required for part, required in zip((train, cal, test), min_counts)):
        raise ValueError('insufficient rows after purging late labels; combine more frozen cohorts')
    for part in (train, cal, test):
        if {r['label'] for r in part} != {0, 1}:
            raise ValueError('each time segment needs both outcomes')
    return train, cal, test


class ProbabilityModel:
    def __init__(self, artifact=None):
        self.artifact = artifact
        self.hash = digest(artifact) if artifact else 'untrained'
        if artifact:
            if artifact.get('schema') != SCHEMA or artifact.get('version') != 1:
                raise ValueError('model schema')
            if any(type(artifact.get(k)) not in (int, float) or not math.isfinite(artifact[k]) or artifact[k] < 0
                   for k in ('created_at', 'trained_through')):
                raise ValueError('model provenance clocks')
            for pair in artifact['phases'].values():
                for lane, names in (('numeric', NUMERIC), ('jev', NUMERIC+META)):
                    m = pair[lane]
                    if m['names'] != list(names) or len(m['weights']) != len(names)*2+1 or len(m['mean']) != len(names) or len(m['scale']) != len(names) or len(m['calibration']) != 2:
                        raise ValueError('model dimensions')
                    if any(not math.isfinite(v) for k in ('weights', 'mean', 'scale', 'calibration') for v in m[k]) or any(v <= 0 for v in m['scale']):
                        raise ValueError('nonfinite model')

    def probabilities(self, phase, cutoff, numeric, meta=None):
        market = numeric['market_up']
        if market is None:
            return {'numeric': None, 'jev': None, 'status': 'missing_market_baseline'}
        if not self.artifact:
            return {'numeric': market, 'jev': market, 'status': 'collect_only_market_baseline'}
        if cutoff <= max(self.artifact['created_at'], self.artifact['trained_through']):
            raise ValueError('artifact not available at decision time')
        pair = self.artifact['phases'].get(phase)
        if pair is None:
            return {'numeric': market, 'jev': market, 'status': 'phase_untrained_market_baseline'}
        p0 = calibrated(numeric, pair['numeric'])
        p1 = calibrated({**numeric, **meta}, pair['jev']) if meta is not None else p0
        return {'numeric': p0, 'jev': p1, 'status': 'calibrated' if meta is not None else 'jev_missing_numeric_fallback'}
