"""T6.9a per-loop lane mask: the operator switches lanes off for one whole loop.

The mask is chosen before a loop starts and stored in the immutable loop market
binding. It is applied like DISABLED_BRANCHES (reserve the core slot, no entry)
but only for that loop, so it is kept out of POLICY: an empty mask leaves every
fingerprint unchanged and a non-empty mask only changes that loop's
execution_fingerprint.

Tokens are ``branch:SIDE`` because shallow_retracement trades both sides.
"""
import json

from .regime_t69a_policy import DISABLED_BRANCHES

LANE_TOKENS = (
    'core_first_down:DOWN',
    'core_first_up:UP',
    'core_stall_down:DOWN',
    'core_c_down:DOWN',
    'c_mirror_up_prior:UP',
    'shallow_retracement:UP',
    'shallow_retracement:DOWN',
)
DOWN_TOKENS = tuple(t for t in LANE_TOKENS if t.endswith(':DOWN'))
UP_TOKENS = tuple(t for t in LANE_TOKENS if t.endswith(':UP'))

# Preset order is the Telegram button order.
PRESETS = {
    'ALL': (),
    'C_DOWN_OFF': ('core_c_down:DOWN',),
    'DOWN_OFF': DOWN_TOKENS,
    'UP_OFF': UP_TOKENS,
}
PRESET_LABELS = {
    'ALL': '全開',
    'C_DOWN_OFF': '只關原 C DOWN',
    'DOWN_OFF': '關全部 DOWN',
    'UP_OFF': '關全部 UP',
}
TOKEN_LABELS = {
    'core_first_down:DOWN': 'first DOWN',
    'core_first_up:UP': 'first UP',
    'core_stall_down:DOWN': 'stall DOWN',
    'core_c_down:DOWN': '原 C DOWN',
    'c_mirror_up_prior:UP': 'C-UP 鏡像',
    'shallow_retracement:UP': '淺回撤 UP',
    'shallow_retracement:DOWN': '淺回撤 DOWN',
}

if any(t.split(':')[0] in DISABLED_BRANCHES for t in LANE_TOKENS):
    raise RuntimeError('a permanently disabled branch cannot be a mask token')


def normalize(value):
    """Canonical sorted token tuple; raises ValueError for anything not allowed.

    Accepts None/'' (no mask), a preset name, canonical JSON text, a
    comma-separated token string, or an iterable of tokens.
    """
    if value is None:
        return ()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ()
        if text.upper() in PRESETS:
            return tuple(sorted(PRESETS[text.upper()]))
        if text.startswith('['):
            try:
                value = json.loads(text)
            except ValueError:
                raise ValueError('lane mask invalid') from None
        else:
            # Telegram users type spaces and stray commas; empty parts are ignored.
            value = [part.strip() for part in text.replace(' ', ',').split(',') if part.strip()]
    if not isinstance(value, (list, tuple, set, frozenset)):
        raise ValueError('lane mask invalid')
    tokens = set()
    for token in value:
        if not isinstance(token, str) or token not in LANE_TOKENS:
            raise ValueError('lane mask token not allowed')
        tokens.add(token)
    if tokens >= set(LANE_TOKENS):
        raise ValueError('lane mask cannot switch every lane off')
    return tuple(sorted(tokens))


def to_text(mask):
    """Stored binding text: '' for no mask, else canonical JSON."""
    mask = normalize(mask)
    return json.dumps(list(mask), separators=(',', ':')) if mask else ''


def preset_of(mask):
    mask = normalize(mask)
    for name, tokens in PRESETS.items():
        if mask == tuple(sorted(tokens)):
            return name
    return 'CUSTOM'


def describe(mask):
    """Short Traditional Chinese label for Telegram and the report."""
    mask = normalize(mask)
    name = preset_of(mask)
    if name != 'CUSTOM':
        return PRESET_LABELS[name]
    return '自訂：關 ' + '、'.join(TOKEN_LABELS[t] for t in LANE_TOKENS if t in mask)


def is_masked(mask, branch, side):
    return f'{branch}:{side}' in normalize(mask)


def side_closed(mask, side):
    """True when the mask switches off every Live lane of ``side``."""
    tokens = UP_TOKENS if side == 'UP' else DOWN_TOKENS if side == 'DOWN' else None
    if tokens is None:
        raise ValueError('side invalid')
    return set(tokens) <= set(normalize(mask))
