"""Whole-loop asset selection. No orders, risk reset or implicit Live arming."""
from dataclasses import replace
from decimal import Decimal

from .loop_market import PROFILE, symbol, data_paths, execution_fingerprint, verify_data_db


class LoopMarketWorker:
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
                    or binding['execution_fingerprint'] != execution_fingerprint(binding['symbol'])
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
            if self._selected_strategy_profile != PROFILE:
                return {**self._status(), 'action_denied': True, 'reason': 'select T6.7c before selecting a market'}
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
            changed = asset != self.settings.market_symbol
            if changed:
                from .settings import RuntimeMode
                self._effective_mode = RuntimeMode.SHADOW
            self._apply_market_context(asset)
            await self.repository.set_runtime_config('prediction_selected_market', {'symbol': asset})
            await self.repository.set_runtime_config('prediction_pending_market', {})
            return {**self._status(), 'market_symbol': asset, 'next_market_symbol': asset,
                    'market_queued': False, 'live_rearm_required': changed,
                    'reason': 'market selected; confirm Live and start a new loop explicitly'}

    async def _loop_market_start_guard(self, count):
        await self.restore_loop_market()
        asset = getattr(self.settings, "market_symbol", "BTCUSDT")
        if self._selected_strategy_profile != PROFILE:
            if asset != 'BTCUSDT' and self._selected_strategy_profile.startswith('regime_target6'):
                return 'only T6.7c supports non-BTC loop markets'
            return None
        active = await self.repository.get_active_loop()
        if active:
            binding = await self.repository.get_loop_market_binding(active['loop_id'])
            if binding:
                if (binding['symbol'] != asset or binding['unit'] != str(self._selected_order_unit_usdt)
                        or binding['target'] != count or binding['profile'] != self._selected_strategy_profile
                        or binding['execution_fingerprint'] != execution_fingerprint(asset)):
                    return 'running loop market/profile/unit/target is immutable'
            elif asset != 'BTCUSDT':
                return 'non-BTC loop binding missing'
            return None
        pending = await self.repository.get_runtime_config('prediction_pending_market', {})
        if pending.get('symbol') and pending['symbol'] != asset:
            return 'apply next market with /predict_market and confirm Live before starting'
        if self.settings.is_live_requested and not await self._market_boundary_clear():
            return 'local/official exposure or unknown state prevents a new loop'
        if asset != 'BTCUSDT':
            for path in data_paths(self.repository.db_path, asset):
                verify_data_db(path, asset)
        return None

    def _loop_market_start_kwargs(self):
        if self._selected_strategy_profile != PROFILE:
            return {}
        return dict(market_symbol=self.settings.market_symbol,
                    market_unit=str(self._selected_order_unit_usdt))
