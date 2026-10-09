"""Whole-loop asset selection. No orders, risk reset or implicit Live arming."""
import asyncio
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
import time

from .loop_market import (PROFILES, T69A_PROFILE, symbol, data_paths, execution_fingerprint,
                          binding_fingerprint, verify_data_db)


# The signal producer skips any market that starts less than 60s after it
# started (c180_signal_runtime PREOPEN_WARMUP_MS: the Tape needs the pre-open
# trades). A new coin's loop may start at the first market it can serve.
PRODUCER_PREOPEN_MS = 60_000
MARKET_MS = 300_000


def producer_ready_at_ms(started_ms):
    return -(-(started_ms + PRODUCER_PREOPEN_MS) // MARKET_MS) * MARKET_MS
PRODUCER_SWITCH_TIMEOUT_S = 90


class LoopMarketWorker:
    async def _switch_producers(self, asset):
        """Run only this coin's producers via scripts/t6_coin.sh (no observers).

        Returns (state, reason): state is 'done', 'unavailable' (script not
        installed; producers left as they are) or 'failed'.
        """
        script = Path(self.repository.db_path).resolve().parents[2] / 'scripts/t6_coin.sh'
        if not script.is_file():
            return 'unavailable', None
        proc = await asyncio.create_subprocess_exec(
            'sh', str(script), 'use', asset[:-len('USDT')],
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), PRODUCER_SWITCH_TIMEOUT_S)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return 'failed', '資料程式切換逾時'
        if proc.returncode:
            lines = [l for l in out.decode(errors='replace').splitlines() if l.strip()]
            return 'failed', f'資料程式切換失敗（exit {proc.returncode}）：' + (lines[-1] if lines else '')
        return 'done', None

    def _apply_market_context(self, asset):
        asset = symbol(asset)
        if asset == getattr(self.settings, "market_symbol", "BTCUSDT"):
            return
        from .spot import BinanceSpotProvider
        self.settings = replace(self.settings, market_symbol=asset)
        self._spot_source = BinanceSpotProvider(symbol=asset)
        self._regime_worker_bridge = None
        self._c180_worker_bridge = None
        # No source cache crosses an asset boundary.
        self._discovery_cache = None
        self._ignored_discovery_topic_ids = set()
        self._shadow_observer_discovery_cache = None
        self._shadow_observer_discovery_at_ms = 0

    async def restore_loop_market(self):
        if getattr(self, "_market_context_loaded", False):
            return
        getter = getattr(self.repository, "get_loop_market_binding", None)
        if not callable(getter):
            return  # old test/custom repositories cannot enable multi-market
        active = await self.repository.get_active_loop()
        binding = await getter(active['loop_id']) if active else None
        if binding:
            if (binding['profile'] != active['strategy_profile']
                    or binding['execution_fingerprint'] != binding_fingerprint(binding)
                    or binding['unit'] != str(self._selected_order_unit_usdt)
                    or binding['target'] != active['target']):
                raise ValueError("persisted loop market identity changed")
            asset = binding['symbol']
        else:
            selected = await self.repository.get_runtime_config("prediction_selected_market", {})
            asset = selected.get('symbol', getattr(self.settings, 'market_symbol', 'BTCUSDT'))
            if active and selected.get('symbol') and (asset != 'BTCUSDT' or getattr(self.settings, 'market_symbol', 'BTCUSDT') != 'BTCUSDT'):
                # Never reinterpret an unbound historical running loop.
                raise ValueError("running loop has no market binding")
        self._apply_market_context(asset)
        self._market_context_loaded = True

    async def _market_boundary_clear(self):
        if not await self.repository.loop_market_local_clear():
            return False
        # Query the entire official account, not only the selected symbol.
        return await self._c180_recovery_exposure_clear()

    async def select_market(self, value):
        try:
            asset = symbol(value)
        except ValueError:
            return {**self._status(), 'action_denied': True, 'reason': 'only BTCUSDT/ETHUSDT/BNBUSDT supported'}
        await self.restore_order_unit()
        await self.restore_selected_strategy()
        await self.restore_loop_market()
        async with self._lock:
            if self._selected_strategy_profile not in PROFILES:
                return {**self._status(), 'action_denied': True, 'reason': 'select T6.7c or T6.9 before selecting a market'}
            existing = await self.repository.get_active_loop()
            task = getattr(self, '_task', None)
            if existing or (task is not None and not task.done()):
                await self.repository.set_runtime_config('prediction_pending_market', {'symbol': asset})
                return {**self._status(), 'next_market_symbol': asset, 'market_queued': True,
                        'reason': 'current loop unchanged; apply the choice again after it drains'}
            if not await self._market_boundary_clear():
                return {**self._status(), 'action_denied': True, 'reason': 'local/official exposure or unknown state prevents market selection'}
            if asset != 'BTCUSDT':
                for path in data_paths(self.repository.db_path, asset):
                    verify_data_db(path, asset)
            switch, why = await self._switch_producers(asset)
            if switch == 'failed':
                return {**self._status(), 'action_denied': True, 'reason': why}
            changed = asset != self.settings.market_symbol
            warmup_until = None
            if switch == 'done' and changed and asset != 'BTCUSDT':
                warmup_until = producer_ready_at_ms(int(time.time() * 1000))
                await self.repository.set_runtime_config(
                    'prediction_producer_switch', {'symbol': asset, 'ready_at_ms': warmup_until})
            if changed:
                from .settings import RuntimeMode
                self._effective_mode = RuntimeMode.SHADOW
            self._apply_market_context(asset)
            await self.repository.set_runtime_config('prediction_selected_market', {'symbol': asset})
            await self.repository.set_runtime_config('prediction_pending_market', {})
            return {**self._status(), 'market_symbol': asset, 'next_market_symbol': asset,
                    'market_queued': False, 'live_rearm_required': changed,
                    'producer_switch': switch, 'producer_warmup_until_ms': warmup_until,
                    'reason': 'market selected; confirm Live and start a new loop explicitly'}

    async def _loop_market_start_guard(self, count):
        await self.restore_loop_market()
        asset = getattr(self.settings, "market_symbol", "BTCUSDT")
        if not await self.repository.get_active_loop():
            # A queued mask belongs to the next new loop and only T6.9b applies it.
            try:
                mask = await self._pending_lane_mask()
            except ValueError:
                return 'queued lane mask is invalid; choose again with /predict_lanemask'
            if mask and self._selected_strategy_profile != T69A_PROFILE:
                return 'lane mask is only for T6.9b; choose 全開 with /predict_lanemask first'
        if self._selected_strategy_profile not in PROFILES:
            if asset != 'BTCUSDT' and self._selected_strategy_profile.startswith('regime_target6'):
                return 'only T6.7c/T6.9 support non-BTC loop markets'
            return None
        active = await self.repository.get_active_loop()
        if active:
            binding = await self.repository.get_loop_market_binding(active['loop_id'])
            if binding:
                if (binding['symbol'] != asset or binding['unit'] != str(self._selected_order_unit_usdt)
                        or binding['target'] != count or binding['profile'] != self._selected_strategy_profile
                        or binding['execution_fingerprint'] != execution_fingerprint(
                            asset, self._selected_strategy_profile, binding.get('lane_mask') or '')):
                    return 'running loop market/profile/unit/target is immutable'
            elif asset != 'BTCUSDT':
                return 'non-BTC loop binding missing'
            return None
        pending = await self.repository.get_runtime_config('prediction_pending_market', {})
        if pending.get('symbol') and pending['symbol'] != asset:
            return 'apply next market with /predict_market and confirm Live before starting'
        warm = await self.repository.get_runtime_config('prediction_producer_switch', {})
        if warm.get('symbol') == asset:
            left_ms = int(warm.get('ready_at_ms', 0)) - int(time.time() * 1000)
            if left_ms > 0:
                return f'{asset} 資料程式暖機中（訊號需在市場開始前 60 秒就運行），約 {-(-left_ms // 60000)} 分鐘後再開輪'
        if self.settings.is_live_requested and not await self._market_boundary_clear():
            return 'local/official exposure or unknown state prevents a new loop'
        if asset != 'BTCUSDT':
            for path in data_paths(self.repository.db_path, asset):
                verify_data_db(path, asset)
        return None

    def _loop_market_start_kwargs(self, lane_mask=''):
        if self._selected_strategy_profile not in PROFILES:
            return {}
        kwargs = dict(market_symbol=self.settings.market_symbol,
                      market_unit=str(self._selected_order_unit_usdt))
        if lane_mask:
            kwargs['lane_mask'] = lane_mask
        return kwargs

    async def _pending_lane_mask(self):
        """Queued mask for the next new loop; () when none. Raises ValueError if corrupt."""
        from .regime_t69a_lane_mask import normalize
        getter = getattr(self.repository, 'get_runtime_config', None)
        if not callable(getter):
            return ()
        pending = await getter('prediction_pending_lane_mask', {}) or {}
        if not isinstance(pending, dict):
            raise ValueError('lane mask invalid')
        return normalize(pending.get('mask') or ())

    async def _active_lane_mask(self):
        """Mask of the running loop's binding; () when unbound or no loop."""
        from .loop_market import binding_lane_mask
        getter = getattr(self.repository, 'get_loop_market_binding', None)
        active = await self.repository.get_active_loop()
        if not active or not callable(getter):
            return ()
        return binding_lane_mask(await getter(active['loop_id']))

    async def _lane_mask_labels(self):
        """Current and queued mask labels for every reply, never a guessed 全開."""
        from .regime_t69a_lane_mask import describe
        labels = {}
        try:
            labels['next_lane_mask_label'] = describe(await self._pending_lane_mask())
        except ValueError:
            labels['next_lane_mask_label'] = '設定無效，請重選'
        try:
            labels['lane_mask_label'] = describe(await self._active_lane_mask())
        except ValueError:
            labels['lane_mask_label'] = '待核對'
        return labels

    async def _resume_lane_mask_error(self):
        """A stopped loop resumes with its own bound lanes, never a different queued mask."""
        active = await self.repository.get_active_loop()
        if not active or str(active.get('strategy_profile') or '').lower() != T69A_PROFILE:
            return None  # only T6.9b loops have lanes a queued mask could have meant
        try:
            pending = await self._pending_lane_mask()
            bound = await self._active_lane_mask()
        except ValueError:
            return '排隊中的 Lane 遮罩無效或本輪遮罩待核對；先用 /predict_lanemask 重選'
        if pending and pending != bound:
            return ('排隊中的 Lane 遮罩要開新 Loop 才會生效，暫停中的這一輪不能套用。'
                    '等這一輪出清後再選一次 /predict_lanemask（會結束這一輪），'
                    '或選回本輪的設定後再續跑')
        return None

    async def _close_drained_stopped_loop(self, existing):
        """Like a strategy/unit switch: end an operator-stopped loop that has fully drained."""
        from .models import CampaignState
        if (not existing or not bool(existing.get('new_entries_stopped'))
                or str(existing.get('terminal_reason') or '').upper() != 'OPERATOR_STOP'):
            return None
        task = getattr(self, '_task', None)
        if task is not None and not task.done():
            return None
        reconciliation = await self.reconcile()
        unresolved = await self.repository.load_unresolved_intents()
        active = [c for c in self._active_campaigns.values()
                  if c.state not in {CampaignState.DONE, CampaignState.CANCELLED}]
        if (not bool(reconciliation.get('known')) or int(reconciliation.get('orders') or 0)
                or unresolved or active):
            return None
        loop_id = str(existing.get('loop_id') or '')
        closer = getattr(self.repository, 'close_operator_stopped_loop_for_strategy_switch', None)
        closed = (await closer(loop_id) if callable(closer)
                  else {'closed': True, 'row': await self.repository.stop_loop(loop_id, state='STOPPED')})
        if not bool(closed.get('closed')):
            return None
        self._loop_id = None
        self._target_markets = 0
        return loop_id

    async def select_lane_mask(self, value):
        """Queue the lane mask for the next new T6.9b loop. Never changes a running loop.

        If the current loop was stopped by the operator and has fully drained, a
        different mask ends it (as a strategy or unit switch does), so the next
        start opens a new loop with the new mask instead of resuming the old one.
        """
        from .regime_t69a_lane_mask import normalize, describe, preset_of
        await self.restore_order_unit()
        await self.restore_selected_strategy()
        await self.restore_loop_market()
        try:
            mask = normalize(value)
        except ValueError as exc:
            return {**self._status(), **(await self._lane_mask_labels()), 'action_denied': True, 'reason': str(exc)}
        next_profile = str(await self._load_pending_strategy() or self._selected_strategy_profile or '').lower()
        if mask and next_profile != T69A_PROFILE:
            return {**self._status(), **(await self._lane_mask_labels()), 'action_denied': True,
                    'reason': 'lane mask is only for T6.9b; select T6.9b with /predict_lane first'}
        async with self._lock:
            existing = await self.repository.get_active_loop()
            closed = None
            if existing and mask != await self._active_lane_mask():
                closed = await self._close_drained_stopped_loop(existing)
                if closed:
                    existing = None
            await self.repository.set_runtime_config('prediction_pending_lane_mask', {
                'mask': list(mask), 'preset': preset_of(mask), 'at_ms': int(time.time() * 1000)})
            task = getattr(self, '_task', None)
            running = bool(existing) or (task is not None and not task.done())
        result = {**self._status(), **(await self._lane_mask_labels()), 'next_lane_mask': list(mask),
                  'next_lane_mask_label': describe(mask), 'lane_mask_queued': running}
        if closed:
            result.update(previous_loop_closed=True, previous_loop_id=closed)
        if running:
            result['lane_mask'] = list(await self._active_lane_mask())
        return result
